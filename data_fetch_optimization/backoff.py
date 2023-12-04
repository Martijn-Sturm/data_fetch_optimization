import time
import threading
import logging
import typing as t


class BackoffManager:
    def __init__(
        self,
        initial_backoff: int = 1,
        max_backoff: int = 5 * 60,
        backoff_factor: float = 2,
        max_seconds_in_subsequent_backoff: int = 30 * 60,
        logger: logging.Logger = logging.getLogger("backoff_manager"),
    ):
        self.lock = threading.Lock()
        self.backoff_release_time = time.time()
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_factor = backoff_factor
        self.current_backoff = initial_backoff
        self.max_seconds_in_subsequent_backoff = max_seconds_in_subsequent_backoff
        self.backoff_is_reset = True
        self.logger = logger
        self.backoff_kickin_time: t.Optional[float] = None

    def increase_backoff(self):
        with self.lock:
            if self.backoff_is_reset:
                self.logger.debug("Backoff is being set, as is backoff kickin time")
                self.backoff_is_reset = False
                self.backoff_kickin_time = time.time()
            self.current_backoff = min(
                self.current_backoff * self.backoff_factor, self.max_backoff
            )
            self.logger.debug("Backoff increased to %s", self.current_backoff)
            self.backoff_release_time = time.time() + self.current_backoff

    def reset_backoff(self):
        with self.lock:
            self.logger.debug("Backoff reset")
            self.current_backoff = self.initial_backoff
            self.backoff_is_reset = True
            self.backoff_kickin_time = None

    def get_wait_seconds(self):
        with self.lock:
            return max(self.backoff_release_time - time.time(), 0)

    def making_api_call(self):
        with self.lock:
            self.logger.debug("Making API call")
            self.backoff_release_time = time.time() + self.current_backoff

    def has_exceeded_max_time_in_subsequent_backoff(self):
        with self.lock:
            if self.backoff_kickin_time is None:
                return False
            return (
                time.time() - self.backoff_kickin_time
                > self.max_seconds_in_subsequent_backoff
            )
