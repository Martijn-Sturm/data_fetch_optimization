import time
import threading
import logging


class BackoffManager:
    def __init__(
        self,
        initial_backoff: int = 1,
        max_backoff: int = 5 * 60,
        backoff_factor: float = 2,
        logger: logging.Logger = logging.getLogger("backoff_manager"),
    ):
        self.lock = threading.Lock()
        self.backoff_release_time = time.time()
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_factor = backoff_factor
        self.current_backoff = initial_backoff
        self.logger = logger

    def increase_backoff(self):
        with self.lock:
            self.current_backoff = min(
                self.current_backoff * self.backoff_factor, self.max_backoff
            )
            self.logger.debug("Backoff increased to %s", self.current_backoff)
            self.backoff_release_time = time.time() + self.current_backoff

    def reset_backoff(self):
        with self.lock:
            self.logger.debug("Backoff reset")
            self.current_backoff = self.initial_backoff

    def get_wait_seconds(self):
        with self.lock:
            return max(self.backoff_release_time - time.time(), 0)

    def making_api_call(self):
        with self.lock:
            self.logger.debug("Making API call")
            self.backoff_release_time = time.time() + self.current_backoff
