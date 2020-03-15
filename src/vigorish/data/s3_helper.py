"""Functions for downloading from and uploading files to an Amazon S3 bucket."""
import boto3
from botocore.exceptions import ClientError

from vigorish.enums import DataSet, DocFormat
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_pitch_app_id


class S3Helper:
    """Perform CRUD operations on objects stored in an S3 bucket."""

    def __init__(self, config):
        self.config = config
        self.bucket_name = self.config.all_settings.get("S3_BUCKET").current_setting(DataSet.ALL)
        self.client = boto3.client("s3")
        self.resource = boto3.resource("s3")
        self.bucket = self.resource.Bucket(self.bucket_name)

    def rename_brooks_pitchfx_log(self, old_pitch_app_id, new_pitch_app_id, year):
        result = validate_pitch_app_id(old_pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        old_key = self.get_object_key(
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=old_pitch_app_id,
        )
        result = validate_pitch_app_id(new_pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        new_key = self.get_object_key(
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=new_pitch_app_id,
        )
        return rename_s3_object(old_key, new_key)

    def rename_s3_object(self, old_key, new_key):
        try:
            self.resource.Object(self.bucket_name, new_key).copy_from(
                CopySource=f"{self.bucket_name}/{old_key}"
            )
            self.resource.Object(self.bucket_name, old_key).delete()
            return Result.Ok()
        except ClientError as e:
            return Result.Fail(repr(e))
