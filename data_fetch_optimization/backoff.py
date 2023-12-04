import time
import threading
import logging
import typing as t


class BackoffManager:
    def __init__(
        self,
        initial_delay_seconds: int = 1,
        max_delay_seconds: int = 5 * 60,
        backoff_factor: float = 2,
        max_seconds_in_subsequent_backoff: int = 30 * 60,
        logger: logging.Logger = logging.getLogger("backoff_manager"),
    ):
        self.lock = threading.Lock()
        self.delay_release_time = time.time()
        self.initial_delay_seconds = initial_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.backoff_factor = backoff_factor
        self.current_delay = initial_delay_seconds
        self.max_seconds_in_subsequent_backoff = max_seconds_in_subsequent_backoff
        self.delay_is_reset = True
        self.logger = logger
        self.backoff_kickin_time: t.Optional[float] = None

    def increase_backoff(self):
        with self.lock:
            if self.delay_is_reset:
                self.logger.debug("Backoff is being set, as is backoff kickin time")
                self.delay_is_reset = False
                self.backoff_kickin_time = time.time()
            self.current_delay = min(
                self.current_delay * self.backoff_factor, self.max_delay_seconds
            )
            self.logger.debug("Delay seconds increased to %s", self.current_delay)

    def reset_backoff(self):
        with self.lock:
            self.logger.debug("Backoff reset")
            self.current_delay = self.initial_delay_seconds
            self.delay_is_reset = True
            self.backoff_kickin_time = None

    def get_wait_seconds(self):
        with self.lock:
            return max(self.delay_release_time - time.time(), 0)

    def acquire_api_call_permission(self) -> bool:
        with self.lock:
            if time.time() > self.delay_release_time:
                self.logger.debug(
                    "Acquired API call permission, delay release time: %s",
                    self.delay_release_time,
                )
                self.delay_release_time = time.time() + self.current_delay
                self.logger.debug(
                    "delay release time is set with %s seconds delay from now",
                    self.current_delay,
                )
                return True
            else:
                return False

    def has_exceeded_max_time_in_subsequent_backoff(self):
        with self.lock:
            if self.backoff_kickin_time is None:
                return False
            return (
                time.time() - self.backoff_kickin_time
                > self.max_seconds_in_subsequent_backoff
            )
