"""v2.5.1: the live clock must advance inside process_table when no clock is injected."""
from datetime import datetime, timedelta, timezone
from unittest import mock
import sys; sys.path.insert(0, "tests")
from test_process import FakeOps, ctx, m, NOW  # noqa: F401  (fixtures)
from bridge import process

def test_production_signature_clock_advances(ctx, m):
    """process_batch calls process_table(now=None) with no current_time."""
    ctx.cutoff_utc = datetime.now(timezone.utc) + timedelta(seconds=2)
    ops = FakeOps(m)
    real_export = ops.export
    def slow_export(*a, **k):
        # Simulate CETAS finishing after the cutoff by moving wall clock forward
        with_patch.start_offset = timedelta(seconds=10)
        return real_export(*a, **k)
    ops.export = slow_export
    class Clock:
        start_offset = timedelta(0)
    with_patch = Clock
    orig = process.datetime
    class FakeDT(orig):
        @classmethod
        def now(cls, tz=None):
            return orig.now(tz) + Clock.start_offset
    with mock.patch.object(process, "datetime", FakeDT):
        status = process.process_table(ctx, m, ops)   # exactly how process_batch calls it
    assert status == "SKIPPED_CUTOFF", f"published after cutoff: {status}; calls={ops.calls}"
