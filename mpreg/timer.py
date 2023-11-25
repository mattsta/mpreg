import time
import sys

from loguru import logger


class Timer:
    """Helper context manager to automatically print elapsed wall clock time.

    (from mutil, but easier to copy here than import mutil itself)

    Use as:
        with Timer("Completed!"):
            time.sleep(3)

    Added feature to allow also timing sub-steps for use like:
        with Timer("Event Processor", every=5) as timer:
            for data in dataset:
                timer.step()

                time.sleep(3)
    """

    def __init__(self, name: str = "", every: int = sys.maxsize):
        if name:
            # pre-format named output so we don't need two format strings
            self.name = f"[{name}] "
        else:
            # if no name, don't show any formatted name in the log output
            self.name = ""

        self.every = every
        self.count = 0

    def step(self):
        # Note: we currently only support SINGLE-ENTRY steps to avoid
        #       needing to check if we stepped "over" a print boundary
        #       (if we allowed something like `step(amount=N)`.
        self.count += 1
        if self.count % self.every == 0:
            self.log(self.count)

    def __enter__(self):
        # Note: don't use time.clock() or time.process_time()
        #       because those don't record time during sleep calls,
        #       but we need to record sleeps for when we're waiting
        #       for async operations (network replies, etc)
        self.start = time.perf_counter()
        self.start_global = self.start
        return self

    def log(self, extra=None) -> None:
        # calculate closing state
        self.end = time.perf_counter()
        self.interval = self.end - self.start

        # don't use .log() if we have no sub-step intervals
        if not self.count:
            return

        extrafmt = ""
        if extra:
            if isinstance(extra, int):
                extrafmt = f" ({extra:,})"
            elif isinstance(extra, float):
                extrafmt = f" ({extra:,.4f})"
            else:
                extrafmt = f" ({extra})"

        # re-set entry state in case we are looping again
        self.start = time.perf_counter()

        # Log current duration...
        # also: using "opt(depth=2)" so the CALLER's mod/func/line info is shown
        # instead of all logging show mutil.Timer.log as the log line.
        # (depth=1 is either timer.step or timer.__exit__, so depth=2 is the ORIGINAL callsite)
        logger.opt(depth=2).info(
            "{}Duration{}: {:,.4f}", self.name, extrafmt, self.interval
        )

    def __exit__(self, *args):
        # print final particle rollup ONLY IF we have sub-steps
        if self.count:
            self.log(self.count)

        self.end = time.perf_counter()
        self.interval = self.end - self.start

        # always print final global time for entire duration
        global_interval = self.end - self.start_global
        logger.opt(depth=1).info("{}Duration: {:,.4f}", self.name, global_interval)
