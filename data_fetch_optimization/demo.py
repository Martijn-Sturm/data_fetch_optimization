import logging
from data_fetch_optimization.operation import OperationModel
import time
import random
import typing as t


class DemoOperations(OperationModel[int, bool]):
    def __init__(
        self,
        n_fetches: int,
        api_fetch_delay_sequence: t.Sequence[float],
        response_successful: t.Sequence[bool],
        write_delay_sequence: t.Sequence[float],
        logger: logging.Logger,
    ):
        self.logger = logger
        self.n_fetches = n_fetches
        self.api_fetch_delay_sequence = api_fetch_delay_sequence
        self.response_successful = response_successful
        self.write_delay_sequence = write_delay_sequence

    def initial_operation(self) -> t.Sequence[int]:
        self.logger.info("Initial operation started")
        request_args = [i for i in range(self.n_fetches)]
        self.logger.info("Request arguments: %s", request_args)
        return request_args

    def fetch_from_api(self, request_argument: int) -> bool:
        """Simulate an API fetch operation."""
        self.logger.info("Fetching from API: %s", request_argument)
        time.sleep(random.choice(self.api_fetch_delay_sequence))
        return random.choice(self.response_successful)

    def response_succeeded(self, response: bool) -> bool:
        """Determine if the response is successful."""
        return response

    def write_upon_definitive_request_failure(self, request_argument: int) -> None:
        """Log instead of writing to disk upon failure."""
        self.logger.info(
            "Writing failed request to disk (simulated): %s", request_argument
        )
        time.sleep(random.choice(self.write_delay_sequence))

    def write_fetched_data(self, request_argument: int, response: bool) -> None:
        """Simulate data writing operation."""
        self.logger.info(
            "Writing fetched data to disk (simulated): %s: %s",
            request_argument,
            response,
        )
        time.sleep(random.choice(self.write_delay_sequence))
