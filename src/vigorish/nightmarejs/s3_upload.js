const { createReadStream } = require("fs");
const path = require("path");
const AWS = require("aws-sdk");
const S3 = new AWS.S3();
const bucket = "vig-data";

function uploadFilesToS3(fileList, s3KeyPrefix) {
  for (let i = 0; i < fileList.length; i++) {
    uploadFileToS3(fileList[i], s3KeyPrefix);
  }
}

function uploadFileToS3(filePath, s3KeyPrefix) {
  var uploadParams = { Bucket: bucket, Key: "", Body: "" };
  var fileStream = createReadStream(filePath);
  fileStream.on("error", function(err) {
    console.log("File Error", err);
  });
  uploadParams.Body = fileStream;
  uploadParams.Key = `${s3KeyPrefix}${path.basename(filePath)}`;
  S3.upload(uploadParams, function(err, data) {
    if (err) {
      console.log("Error", err);
    }
    if (data) {
    }
  });
}

module.exports = {
  uploadFilesToS3: uploadFilesToS3,
  uploadFileToS3: uploadFileToS3
};
