"""Custom exceptions for issues specific to vigorish actions/processes."""


class S3BucketException(Exception):
    """Exception raised when an operation requiring S3 bucket access cannot be performed."""

    def __init__(self, message=None):
        if not message:
            message = "Unable to access S3 bucket, please verify AWS credentials are propertly configured."
        super().__init__(message)


class ScrapedDataException(Exception):
    """Exception raised when data identified by file_type, data_set and url_id cannot be found."""

    def __init__(self, file_type, data_set, url_id):
        message = f"Failed to locate scraped data: URL ID: {url_id} (File Type: {file_type}, Data Set: {data_set})"
        super().__init__(message)


class InvalidSeasonException(Exception):
    """Exception raised when the database does not contain data for the requested season."""

    def __init__(self, year):
        message = f"The database does not contain any data for the MLB {year} Season"
        super().__init__(message)


class UnknownPlayerException(Exception):
    """Exception raised when the database does not contain data for the requested player."""

    def __init__(self, mlb_id):
        message = f"The database does not contain any data for a player with MLB ID: {mlb_id}"
        super().__init__(message)
