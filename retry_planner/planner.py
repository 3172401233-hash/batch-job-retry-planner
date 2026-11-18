"""重试规划器：计算恢复边界与不可重试原因。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

from .models import ErrorCategory, Failure, Job, Step, StepKind

#: 限流退避基数（秒），实际退避 = BASE * 2 ** retries_used
RATE_LIMIT_BACKOFF_BASE_SECONDS = 1.0


class ResumeMode(enum.Enum):
    RESTART = "restart"                            # 从头重跑（只读步骤）
    RESUME_FROM_CHECKPOINT = "resume_from_checkpoint"  # 从安全 checkpoint 续跑
    ROLLBACK_THEN_RESTART = "rollback_then_restart"    # 先回滚已提交副作用再重跑
    SKIP_IDEMPOTENT = "skip_idempotent"            # 重复回调：幂等跳过，无需重跑


@dataclass(frozen=True)
class ResumeAction:
    """单个步骤的恢复动作。"""

    step: str
    mode: ResumeMode
    checkpoint_position: int | None = None
    rollbacks: tuple[str, ...] = ()   # 需要回滚的副作用名
    backoff_seconds: float = 0.0      # 执行前需等待的退避时间


@dataclass
class RecoveryPlan:
    """恢复计划。"""

    retryable: bool
    reasons: list[str] = field(default_factory=list)      # 不可重试原因
    actions: list[ResumeAction] = field(default_factory=list)
    recovery_boundary: list[str] = field(default_factory=list)  # 恢复边界（最早重跑步骤）
    rerun_steps: list[str] = field(default_factory=list)        # 拓扑序的全部重跑步骤
    estimated_seconds: float = 0.0


def _rate_limit_backoff(step: Step) -> float:
    return RATE_LIMIT_BACKOFF_BASE_SECONDS * (2 ** step.retries_used)


def _step_action(step: Step, category: ErrorCategory) -> ResumeAction | str:
    """为可重试步骤生成恢复动作；无法安全恢复时返回原因字符串。"""
    backoff = _rate_limit_backoff(step) if category is ErrorCategory.RATE_LIMIT else 0.0

    if step.kind is StepKind.READ_ONLY or not step.committed_effects:
        # 只读步骤或尚未提交任何副作用：整体重跑是安全的
        return ResumeAction(step=step.name, mode=ResumeMode.RESTART, backoff_seconds=backoff)

    # 部分提交：优先从覆盖已提交副作用的 checkpoint 续跑
    if step.checkpoint is not None and step.checkpoint.covers_committed:
        return ResumeAction(
            step=step.name,
            mode=ResumeMode.RESUME_FROM_CHECKPOINT,
            checkpoint_position=step.checkpoint.position,
            backoff_seconds=backoff,
        )

    # 否则要求全部已提交副作用可回滚
    effects = step.committed_effects
    if effects and all(e.rollbackable for e in effects):
        return ResumeAction(
            step=step.name,
            mode=ResumeMode.ROLLBACK_THEN_RESTART,
            rollbacks=tuple(e.name for e in effects),
            backoff_seconds=backoff,
        )

    return (
        f"步骤 {step.name} 已部分提交且无法安全恢复："
        f"无覆盖已提交副作用的 checkpoint，且存在不可回滚的副作用，"
        f"直接重试会重复追加结果"
    )


def plan_recovery(job: Job, failures: list[Failure]) -> RecoveryPlan:
    """根据失败记录规划恢复。

    返回 RecoveryPlan：retryable 为 False 时 reasons 给出不可重试原因；
    为 True 时 recovery_boundary 给出恢复边界，actions 给出每步动作。
    """
    reasons: list[str] = []
    actions: list[ResumeAction] = []
    rerun: set[str] = set()

    failed_by_step = {f.step: f for f in failures}
    for f in failures:
        if f.step not in job.steps:
            reasons.append(f"失败记录引用了未知步骤 {f.step}")

    if reasons:
        return RecoveryPlan(retryable=False, reasons=reasons)

    for f in failures:
        step = job.steps[f.step]
        category = f.category

        if category is ErrorCategory.CANCELLED:
            reasons.append(f"作业已取消（步骤 {f.step}），禁止重试")
            continue
        if category is ErrorCategory.PERMANENT:
            reasons.append(f"步骤 {f.step} 发生永久性错误，重试无意义：{f.detail}")
            continue
        if category is ErrorCategory.DUPLICATE_CALLBACK:
            # 重复回调说明步骤实际已成功，幂等跳过即可
            actions.append(ResumeAction(step=f.step, mode=ResumeMode.SKIP_IDEMPOTENT))
            continue
        if category is ErrorCategory.DEPENDENCY_FAILED:
            failed_upstream = [
                u for u in job.upstream(f.step) if u in failed_by_step
            ]
            if not failed_upstream:
                reasons.append(
                    f"步骤 {f.step} 因依赖失败而无法运行，但未发现可恢复的失败上游"
                )
                continue
            # 上游会被加入重跑集合（由它们自身的失败记录驱动），本步骤跟随重跑
            rerun.add(f.step)
            continue
        # RATE_LIMIT / TIMEOUT：暂时性错误，可重试
        rerun.add(f.step)

    if reasons:
        return RecoveryPlan(retryable=False, reasons=reasons)

    # 下游失效：重跑步骤的下游若已消费其输出，也必须重跑
    for name in list(rerun):
        rerun |= job.downstream(name)

    # 为每个重跑步骤生成动作（部分提交步骤必须回滚或从 checkpoint 续跑）
    for name in rerun:
        step = job.steps[name]
        category = (
            failed_by_step[name].category if name in failed_by_step else ErrorCategory.TIMEOUT
        )
        action_or_reason = _step_action(step, category)
        if isinstance(action_or_reason, str):
            reasons.append(action_or_reason)
        else:
            actions.append(action_or_reason)

    if reasons:
        return RecoveryPlan(retryable=False, reasons=reasons)

    # 重试预算：每个重跑步骤消耗 1 次预算
    if len(rerun) > job.retry_budget:
        reasons.append(
            f"重试预算不足：需要重跑 {len(rerun)} 个步骤，剩余预算 {job.retry_budget}"
        )
        return RecoveryPlan(retryable=False, reasons=reasons)

    # 截止时间：重跑耗时 + 退避必须落在剩余时间内
    estimated = sum(job.steps[n].estimated_seconds for n in rerun)
    estimated += sum(a.backoff_seconds for a in actions)
    if job.deadline_seconds is not None and estimated > job.deadline_seconds:
        reasons.append(
            f"截止时间不足：预计重跑 {estimated:.1f}s，剩余 {job.deadline_seconds:.1f}s"
        )
        return RecoveryPlan(retryable=False, reasons=reasons)

    # 拓扑序输出重跑步骤与恢复边界
    ordered = [n for n in _topo_order(job) if n in rerun]
    boundary = [
        n for n in ordered
        if not any(dep in rerun for dep in _all_upstream(job, n))
    ]
    return RecoveryPlan(
        retryable=True,
        actions=actions,
        recovery_boundary=boundary,
        rerun_steps=ordered,
        estimated_seconds=estimated,
    )


def _topo_order(job: Job) -> list[str]:
    order: list[str] = []
    seen: set[str] = set()

    def visit(name: str) -> None:
        if name in seen:
            return
        seen.add(name)
        for dep in job.steps[name].depends_on:
            visit(dep)
        order.append(name)

    for name in job.steps:
        visit(name)
    return order


def _all_upstream(job: Job, name: str) -> set[str]:
    return job.upstream(name)
