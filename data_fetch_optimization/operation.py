import typing as t

Response = t.TypeVar("Response")
RequestArg = t.TypeVar("RequestArg")


class OperationModel(t.Generic[RequestArg, Response]):
    def initial_operation(self) -> t.Sequence[RequestArg]:
        """This method is invoked before multithreading starts.
        It must return a sequence
        It can be used to retrieve information and checking if the API is responsive."""
        ...

    def fetch_from_api(self, request_argument: RequestArg) -> Response:
        """This method is invoked by the worker threads.
        It must return a part of the response that can be processed
        by response_succeeded and write_fetched_data."""
        ...

    def response_succeeded(self, response: Response) -> bool:
        """This method processes the response from fetch_from_api.
        It must return a boolean indicating if the response was successful."""
        ...

    def upon_definitive_request_failure(
        self, request_argument: RequestArg, response: Response
    ) -> None:
        """[Optional] -> can be empty function
        This method is invoked when the API has failed to respond
        after max_attempts_per_request attempts.
        """
        ...

    def write_fetched_data(
        self, request_argument: RequestArg, response: Response
    ) -> None:
        """This method processes the response, and writes data to disk."""
        ...


class DummyOperations(OperationModel[str, int]):
    def initial_operation(self):
        return ["1", "2", "3"]

    def fetch_from_api(self, request_argument: str):
        return 12

    def response_succeeded(self, response: int):
        return response == 2

    def write_fetched_data(self, response: int) -> None:
        print(response)

    def upon_definitive_request_failure(self, request_argument: str, response: int):
        print(request_argument)
