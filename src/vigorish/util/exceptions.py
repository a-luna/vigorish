"""Custom exceptions for issues specific to vigorish actions/processes."""


class ScrapedDataException(Exception):
    """Exception raised when data identified by file_type, data_set and url_id cannot be found."""

    def __init__(self, file_type, data_set, url_id):
        message = (
            f"Failed to locate scraped data: URL ID: {url_id} "
            f"(File Type, {file_type}, Data Set: {data_set})"
        )
        super().__init__(message)
