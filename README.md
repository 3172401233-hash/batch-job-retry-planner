# batch-job-retry-planner

Python 批量任务重试规划库：输入步骤 DAG、checkpoint、错误类别、已提交副作用、
重试预算和截止时间，计算**恢复边界**（从哪个步骤恢复，而不是重跑整个作业）
以及**不可重试原因**。

## 快速开始

```python
from retry_planner import (
    Checkpoint, ErrorCategory, Failure, Job, SideEffect, Step, StepKind,
    plan_recovery,
)

job = Job(
    steps={
        "extract": Step("extract", estimated_seconds=10),
        "load": Step(
            "load",
            kind=StepKind.READ_WRITE,
            depends_on=("extract",),
            estimated_seconds=30,
            checkpoint=Checkpoint(position=1000, covers_committed=True),
            side_effects=[SideEffect("append-rows-0-999")],
        ),
    },
    retry_budget=3,
    deadline_seconds=300,
)

plan = plan_recovery(job, [Failure("load", ErrorCategory.RATE_LIMIT)])
# plan.retryable == True
# plan.recovery_boundary == ["load"]   —— 只恢复失败步骤，不重跑整个作业
# plan.actions[0].mode == RESUME_FROM_CHECKPOINT（部分提交，从 checkpoint 1000 续跑）
```

运行测试：

```bash
python -m unittest
```

## 错误分类

| 类别 | 含义 | 规划行为 |
| --- | --- | --- |
| `PERMANENT` | 永久性错误（如 schema 不兼容） | 不可重试，输出原因 |
| `RATE_LIMIT` | 暂时限流 | 可重试，按 `2 ** retries_used` 秒指数退避，并计入截止时间 |
| `DEPENDENCY_FAILED` | 上游依赖失败 | 恢复边界上移到失败的上游，本步骤跟随重跑 |
| `TIMEOUT` | 执行超时 | 可重试（受预算与截止时间约束） |
| `DUPLICATE_CALLBACK` | 重复回调 | 幂等跳过（`SKIP_IDEMPOTENT`），无需重跑 |
| `CANCELLED` | 作业被取消 | 整个作业禁止重试 |

## 恢复规则

- **只读步骤**（`READ_ONLY`）：无副作用，直接整体重跑（`RESTART`）。
- **部分提交步骤**（`READ_WRITE` 且已有提交的副作用）：禁止盲目重试，否则
  会重复追加结果。必须满足以下任一条件：
  1. 存在覆盖全部已提交副作用的 checkpoint（`covers_committed=True`），
     从该 checkpoint 续跑（`RESUME_FROM_CHECKPOINT`）；
  2. 所有已提交副作用均可回滚，先回滚再重跑（`ROLLBACK_THEN_RESTART`）。

  两者都不满足时该步骤不可重试，原因会写入 `RecoveryPlan.reasons`。
- **下游失效**：重跑某步骤时，其所有下游步骤一并失效重跑，
  恢复边界是重跑集合中无上游依赖的最早步骤。
- **重试预算**：每个重跑步骤消耗 1 点作业级预算，不足则整体不可重试。
- **截止时间**：重跑预计耗时（含限流退避）必须落在剩余时间内。

## 幂等要求

调度器与步骤实现需遵守以下约定，规划器的决策才是安全的：

- **副作用登记**：步骤每提交一批输出，必须同步登记 `SideEffect`，
  并推进 checkpoint；checkpoint 的 `covers_committed` 为真时，
  其位置必须覆盖所有已登记副作用。
- **重复回调去重**：同一完成回调可能送达多次，收到
  `DUPLICATE_CALLBACK` 时不得再次执行步骤，直接确认即可。
- **回滚即补偿**：`rollbackable=True` 的副作用必须提供补偿事务，
  回滚后重跑不得产生重复输出。
- **输出幂等键**：无法回滚的追加型输出（如对象存储分片）应使用
  确定性幂等键，使重试天然去重；否则该副作用必须标记为不可回滚，
  让规划器拒绝盲目重试。
