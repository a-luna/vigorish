class Result:
    """Represents the outcome of an operation.

    Attributes
    ----------
    success : bool
        A flag that is set to True if the operation was successful, False if
        the operation failed.
    value :
        The result of the operation if successful, value is None if operation
        failed or if the operation has no return value.
    error : str
        Error message detailing why the operation failed, value is None if
        operation was successful.
    """

    def __init__(self, success, value, error):
        self.success = success
        self.error = error
        self.value = value

    @property
    def failure(self):
        """True if operation failed, False if successful (read-only)."""
        return not self.success

    def __str__(self):
        if self.success:
            return f"[Success]"
        else:
            return f"[Failure] {self.error}"

    def __repr__(self):
        if self.success:
            return f"Result<success={self.success}>"
        else:
            return f'Result<success={self.success}, message="{self.error}">'

    def on_success(self, func, *args, **kwargs):
        if self.failure:
            return self
        if self.value:
            return func(self.value, *args, **kwargs)
        return func(*args, **kwargs)

    def on_failure(self, func, *args, **kwargs):
        if self.success:
            return self.value if self.value else None
        if self.error:
            return func(self.error, *args, **kwargs)
        return func(*args, **kwargs)

    def on_both(self, func, *args, **kwargs):
        if self.value:
            return func(self.value, *args, **kwargs)
        return func(*args, **kwargs)

    @classmethod
    def Fail(cls, error):
        """Create a Result object for a failed operation."""
        return cls(False, value=None, error=error)

    @classmethod
    def Ok(cls, value=None):
        """Create a Result object for a successful operation."""
        return cls(True, value=value, error=None)
