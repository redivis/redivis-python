import builtins


class RedivisError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

    def __str__(self):
        return self.message


class APIError(RedivisError):
    def __init__(self, status_code, error, error_description=""):
        self.status_code = status_code
        self.message = error
        self.description = error_description
        super().__init__(error_description or error)

    def __str__(self):
        return f"[{self.status_code} {self.message}] {self.description}"


class NotFoundError(APIError):
    def __init__(self, status_code, error="", error_description=""):
        self.status_code = status_code
        self.message = error
        self.description = error_description
        super().__init__(status_code, error, error_description)

    def __str__(self):
        return self.description or self.message


class AuthorizationError(APIError):
    def __init__(self, status_code, error="", error_description=""):
        self.status_code = status_code
        self.message = error
        self.description = error_description
        super().__init__(status_code, error, error_description)

    def __str__(self):
        return self.description or self.message


class NetworkError(RedivisError):
    def __init__(
        self,
        message="A network error occurred",
        # either requests.RequestException or urllib3.exceptions.HTTPError
        original_exception=None,
    ):
        self.message = message
        self.original_exception = original_exception
        super().__init__(message)

    def __str__(self):
        if self.original_exception:
            return f"{self.message}: {self.original_exception}"
        return self.message


class ValueError(RedivisError, builtins.ValueError):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

    def __str__(self):
        return self.message


class JobError(RedivisError):
    def __init__(self, message=None, kind=None, status="status unknown"):
        self.message = message or f"Job finished with status: {status}"
        self.kind = kind
        self.status = status
        super().__init__(self.message)

    def __str__(self):
        return f"[{self.kind} {self.status}] {self.message}"


class DeprecationError(RedivisError):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

    def __str__(self):
        return self.message
