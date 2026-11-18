import unittest

from retry_planner import (
    Checkpoint,
    ErrorCategory,
    Failure,
    Job,
    SideEffect,
    Step,
    StepKind,
    plan_recovery,
)
from retry_planner.planner import ResumeMode


def make_job(**overrides) -> Job:
    """三步骤线性 DAG：extract -> transform -> load。"""
    steps = {
        "extract": Step("extract", estimated_seconds=10.0),
        "transform": Step("transform", depends_on=("extract",), estimated_seconds=20.0),
        "load": Step(
            "load",
            kind=StepKind.READ_WRITE,
            depends_on=("transform",),
            estimated_seconds=30.0,
        ),
    }
    kwargs = {"steps": steps, "retry_budget": 5, "deadline_seconds": None}
    kwargs.update(overrides)
    return Job(**kwargs)


class TestErrorCategories(unittest.TestCase):
    def test_permanent_error_not_retryable(self):
        job = make_job()
        plan = plan_recovery(job, [
            Failure("transform", ErrorCategory.PERMANENT, "schema 不兼容"),
        ])
        self.assertFalse(plan.retryable)
        self.assertTrue(any("永久性错误" in r for r in plan.reasons))
        self.assertIn("transform", plan.reasons[0])

    def test_rate_limit_retryable_with_backoff(self):
        job = make_job()
        job.steps["transform"].retries_used = 2
        plan = plan_recovery(job, [
            Failure("transform", ErrorCategory.RATE_LIMIT),
        ])
        self.assertTrue(plan.retryable, plan.reasons)
        action = next(a for a in plan.actions if a.step == "transform")
        self.assertEqual(action.backoff_seconds, 4.0)  # 1 * 2**2
        # 只重跑失败步骤及其下游，而不是整个作业
        self.assertEqual(plan.recovery_boundary, ["transform"])
        self.assertEqual(plan.rerun_steps, ["transform", "load"])
        self.assertNotIn("extract", plan.rerun_steps)

    def test_rate_limit_respects_deadline(self):
        job = make_job(deadline_seconds=5.0)
        plan = plan_recovery(job, [Failure("transform", ErrorCategory.RATE_LIMIT)])
        self.assertFalse(plan.retryable)
        self.assertTrue(any("截止时间不足" in r for r in plan.reasons))

    def test_dependency_failed_recovers_from_upstream(self):
        job = make_job()
        plan = plan_recovery(job, [
            Failure("extract", ErrorCategory.TIMEOUT),
            Failure("transform", ErrorCategory.DEPENDENCY_FAILED),
        ])
        self.assertTrue(plan.retryable, plan.reasons)
        # 恢复边界是失败的上游 extract，而不是整个作业
        self.assertEqual(plan.recovery_boundary, ["extract"])
        self.assertEqual(plan.rerun_steps, ["extract", "transform", "load"])

    def test_dependency_failed_without_failed_upstream(self):
        job = make_job()
        plan = plan_recovery(job, [
            Failure("transform", ErrorCategory.DEPENDENCY_FAILED),
        ])
        self.assertFalse(plan.retryable)
        self.assertTrue(any("依赖失败" in r for r in plan.reasons))

    def test_timeout_retryable(self):
        job = make_job()
        plan = plan_recovery(job, [Failure("extract", ErrorCategory.TIMEOUT)])
        self.assertTrue(plan.retryable, plan.reasons)
        self.assertEqual(plan.recovery_boundary, ["extract"])

    def test_duplicate_callback_skipped_idempotently(self):
        job = make_job()
        plan = plan_recovery(job, [
            Failure("load", ErrorCategory.DUPLICATE_CALLBACK),
        ])
        self.assertTrue(plan.retryable, plan.reasons)
        action = next(a for a in plan.actions if a.step == "load")
        self.assertEqual(action.mode, ResumeMode.SKIP_IDEMPOTENT)
        self.assertEqual(plan.rerun_steps, [])

    def test_cancelled_not_retryable(self):
        job = make_job()
        plan = plan_recovery(job, [
            Failure("extract", ErrorCategory.TIMEOUT),
            Failure("load", ErrorCategory.CANCELLED),
        ])
        self.assertFalse(plan.retryable)
        self.assertTrue(any("取消" in r for r in plan.reasons))


class TestPartialCommit(unittest.TestCase):
    def make_partial_load(self, **step_kwargs) -> Job:
        job = make_job()
        load = job.steps["load"]
        load.side_effects = step_kwargs.pop("side_effects")
        load.checkpoint = step_kwargs.pop("checkpoint", None)
        return job

    def test_partial_commit_resume_from_checkpoint(self):
        job = self.make_partial_load(
            side_effects=[SideEffect("append-rows-0-999")],
            checkpoint=Checkpoint(position=1000, covers_committed=True),
        )
        plan = plan_recovery(job, [Failure("load", ErrorCategory.TIMEOUT)])
        self.assertTrue(plan.retryable, plan.reasons)
        action = next(a for a in plan.actions if a.step == "load")
        self.assertEqual(action.mode, ResumeMode.RESUME_FROM_CHECKPOINT)
        self.assertEqual(action.checkpoint_position, 1000)

    def test_partial_commit_rollback_then_restart(self):
        job = self.make_partial_load(
            side_effects=[SideEffect("append-rows-batch-1", rollbackable=True)],
        )
        plan = plan_recovery(job, [Failure("load", ErrorCategory.TIMEOUT)])
        self.assertTrue(plan.retryable, plan.reasons)
        action = next(a for a in plan.actions if a.step == "load")
        self.assertEqual(action.mode, ResumeMode.ROLLBACK_THEN_RESTART)
        self.assertEqual(action.rollbacks, ("append-rows-batch-1",))

    def test_partial_commit_not_retryable_without_checkpoint_or_rollback(self):
        job = self.make_partial_load(
            side_effects=[SideEffect("append-rows-batch-1", rollbackable=False)],
        )
        plan = plan_recovery(job, [Failure("load", ErrorCategory.TIMEOUT)])
        self.assertFalse(plan.retryable)
        self.assertTrue(any("重复追加" in r for r in plan.reasons))

    def test_read_only_step_restarts_directly(self):
        job = make_job()
        plan = plan_recovery(job, [Failure("extract", ErrorCategory.TIMEOUT)])
        action = next(a for a in plan.actions if a.step == "extract")
        self.assertEqual(action.mode, ResumeMode.RESTART)


class TestBudgetAndDeadline(unittest.TestCase):
    def test_retry_budget_exhausted(self):
        job = make_job(retry_budget=1)
        plan = plan_recovery(job, [Failure("extract", ErrorCategory.TIMEOUT)])
        # extract 失败需重跑 3 个步骤（含下游），预算只有 1
        self.assertFalse(plan.retryable)
        self.assertTrue(any("预算不足" in r for r in plan.reasons))

    def test_deadline_sufficient(self):
        job = make_job(deadline_seconds=120.0)
        plan = plan_recovery(job, [Failure("load", ErrorCategory.TIMEOUT)])
        self.assertTrue(plan.retryable, plan.reasons)
        self.assertEqual(plan.estimated_seconds, 30.0)

    def test_unknown_step_in_failure(self):
        job = make_job()
        plan = plan_recovery(job, [Failure("ghost", ErrorCategory.TIMEOUT)])
        self.assertFalse(plan.retryable)
        self.assertTrue(any("未知步骤" in r for r in plan.reasons))


if __name__ == "__main__":
    unittest.main()
