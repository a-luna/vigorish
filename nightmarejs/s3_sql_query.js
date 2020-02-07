const AWS = require("aws-sdk");
const S3 = new AWS.S3();

async function executeS3SelectQuery(params, retryCounter = 3) {
  try {
    const result = await S3.selectObjectContent(params).promise();
    const events = result.Payload;
    let records = [];
    for await (const event of events) {
      if (event.Records) {
        records.push(event.Records.Payload);
      }
    }
    let resultsString = Buffer.concat(records).toString("utf8");
    resultsString = resultsString.replace(/\,$/, "");
    resultsString = `[${resultsString}]`;

    try {
      const jsonResult = JSON.parse(resultsString);
      return jsonResult;
    } catch (e) {
      console.error(
        `Unable to convert S3 data to JSON object. S3 Select Query: ${params.Expression}`
      );
    }
  } catch (e) {
    if (e.code === "NoSuchKey") {
      console.info(`S3 key does not exist: ${params.Key}`);
      return [];
    }
    if (e.code === "InternalError") {
      if (retryCount > 0) {
        console.log(`Retrying... ${retryCount - 1} remaining`);
        return await executeS3SelectQuery(params, retryCounter - 1);
      }
    }
    console.error(
      `Unknown error occurred attempting to query S3 object: ${params.Key}: ${e}`
    );
    throw e;
  }
}

module.exports = {
  executeS3SelectQuery: executeS3SelectQuery
};
