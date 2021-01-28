from __future__ import annotations

from typing import Callable, Generic, Iterable, Optional, TypeVar

T = TypeVar("T")


class Result(Generic[T]):
    """Represent the outcome of an operation."""

    def __init__(self, success: bool, value: T, error: str) -> None:
        self.success = success
        self.error = error
        self.value = value

    def __str__(self) -> str:
        """Informal string representation of a result."""
        result = "Success" if self.success else "Fail"
        detail = f" {self.error}" if self.failure else f" value={self.value}" if self.value else ""
        return f"[{result}]{detail}"

    def __repr__(self) -> str:
        """Official string representation of a result."""
        detail = f', error="{self.error}"' if self.failure else f", value={self.value}" if self.value else ""
        return f"<Result success={self.success}{detail}>"

    @property
    def failure(self) -> bool:
        """Flag that indicates if the operation failed."""
        return not self.success

    def on_success(self, func: Callable, *args, **kwargs) -> Result:
        """Pass result of successful operation (if any) to subsequent function."""
        return self if self.failure else func(self.value, *args, **kwargs) if self.value else func(*args, **kwargs)

    def on_failure(self, func: Callable, *args, **kwargs) -> Result:
        """Pass error message from failed operation to subsequent function."""
        return self if self.success else func(self.error, *args, **kwargs)

    def on_both(self, func: Callable, *args, **kwargs) -> Result:
        """Pass result (either succeeded/failed) to subsequent function."""
        return func(self, *args, **kwargs)

    @staticmethod
    def Fail(error_message: str) -> Result:
        """Create a Result object for a failed operation."""
        return Result(False, value=None, error=error_message)

    @staticmethod
    def Ok(value: Optional[T] = None) -> Result[T]:
        """Create a Result object for a successful operation."""
        return Result(True, value=value, error=None)

    @staticmethod
    def Combine(results: Iterable[Result]) -> Result:
        """Return a Result object based on the outcome of a list of Results."""
        if all(result.success for result in results):
            return Result.Ok()
        errors = [result.error for result in results if result.failure]
        return Result.Fail("\n".join(errors))
