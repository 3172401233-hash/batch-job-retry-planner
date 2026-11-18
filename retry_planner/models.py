"""数据模型：DAG、步骤、checkpoint、副作用与失败记录。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class ErrorCategory(enum.Enum):
    """错误类别，决定重试策略。"""

    PERMANENT = "permanent"                # 永久性错误，重试无意义
    RATE_LIMIT = "rate_limit"              # 暂时限流，退避后可重试
    DEPENDENCY_FAILED = "dependency_failed"  # 上游依赖失败
    TIMEOUT = "timeout"                    # 超时，可能可重试
    DUPLICATE_CALLBACK = "duplicate_callback"  # 重复回调（幂等，无需重跑）
    CANCELLED = "cancelled"                # 作业被取消，禁止重试


class StepKind(enum.Enum):
    """步骤类型。"""

    READ_ONLY = "read_only"    # 只读步骤：无副作用，可整体重试
    READ_WRITE = "read_write"  # 有副作用：部分提交时需回滚或从 checkpoint 续跑


@dataclass(frozen=True)
class Checkpoint:
    """步骤内的安全恢复点。

    position: 逻辑进度（如已处理行数），resume 时从此处继续。
    covers_committed: 该 checkpoint 是否已覆盖所有已提交副作用
        （即 checkpoint 之后的提交都可被安全丢弃/去重）。
    """

    position: int
    covers_committed: bool = True


@dataclass(frozen=True)
class SideEffect:
    """已提交的副作用记录。"""

    name: str
    committed: bool = True
    rollbackable: bool = True  # 是否支持回滚（补偿事务）


@dataclass
class Step:
    """DAG 中的一个步骤。"""

    name: str
    kind: StepKind = StepKind.READ_ONLY
    depends_on: tuple[str, ...] = ()
    estimated_seconds: float = 0.0       # 单次执行预计耗时
    checkpoint: Checkpoint | None = None
    side_effects: list[SideEffect] = field(default_factory=list)
    retries_used: int = 0                # 已消耗的重试次数

    @property
    def committed_effects(self) -> list[SideEffect]:
        return [e for e in self.side_effects if e.committed]

    @property
    def partially_committed(self) -> bool:
        """已提交部分输出但未完成（需要回滚或从 checkpoint 续跑）。"""
        return self.kind is StepKind.READ_WRITE and bool(self.committed_effects)


@dataclass(frozen=True)
class Failure:
    """一次步骤失败记录。"""

    step: str
    category: ErrorCategory
    detail: str = ""


@dataclass
class Job:
    """批量作业：步骤 DAG + 重试预算 + 截止时间。"""

    steps: dict[str, Step]
    retry_budget: int = 3          # 整个作业剩余的重试预算（按步骤计）
    deadline_seconds: float | None = None  # 距截止的剩余秒数，None 表示无限制

    def __post_init__(self) -> None:
        for step in self.steps.values():
            for dep in step.depends_on:
                if dep not in self.steps:
                    raise ValueError(f"步骤 {step.name} 依赖未知步骤 {dep}")

    def downstream(self, name: str) -> set[str]:
        """name 的所有下游（传递闭包）。"""
        result: set[str] = set()
        stack = [name]
        while stack:
            current = stack.pop()
            for step in self.steps.values():
                if current in step.depends_on and step.name not in result:
                    result.add(step.name)
                    stack.append(step.name)
        return result

    def upstream(self, name: str) -> set[str]:
        """name 的所有上游（传递闭包）。"""
        result: set[str] = set()
        stack = [name]
        while stack:
            current = stack.pop()
            for dep in self.steps[current].depends_on:
                if dep not in result:
                    result.add(dep)
                    stack.append(dep)
        return result
