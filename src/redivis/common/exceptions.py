import builtins


class RedivisError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class APIError(RedivisError):
    def __init__(self, message, status_code, error_description=""):
        super().__init__(error_description or message)
        self.status_code = status_code
        self.message = message
        self.description = error_description

    def __str__(self):
        return f"[{self.status_code} {self.message}] {self.description}"


class NotFoundError(APIError):
    def __init__(self, message="", status_code=404, error_description=""):
        super().__init__(message, status_code, error_description)
        self.status_code = status_code
        self.message = message
        self.description = error_description

    def __str__(self):
        return self.description or self.message


class AuthorizationError(APIError):
    def __init__(self, message, status_code, error_description=""):
        super().__init__(message, status_code, error_description)
        self.status_code = status_code
        self.message = message
        self.description = error_description

    def __str__(self):
        return f"[{self.status_code} {self.message}] {self.description}"


class NetworkError(RedivisError):
    def __init__(
        self,
        message="A network error occurred",
        # either requests.RequestException or urllib3.exceptions.HTTPError
        original_exception=None,
    ):
        super().__init__(message)
        self.message = message
        self.original_exception = original_exception

    def __str__(self):
        if self.original_exception:
            return f"{self.message}: {self.original_exception}"
        return self.message


class ValueError(RedivisError, builtins.ValueError):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class JobError(RedivisError):
    def __init__(self, message=None, kind=None, status="status unknown"):
        final_message = message or f"Job finished with status: {status}"
        super().__init__(final_message)
        self.message = final_message
        self.kind = kind
        self.status = status

    def __str__(self):
        return f"[{self.kind} {self.status}] {self.message}"


class DeprecationError(RedivisError):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message
