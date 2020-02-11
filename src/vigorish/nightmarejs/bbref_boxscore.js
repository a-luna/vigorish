const { readFileSync } = require("fs")
const { executeS3SelectQuery } = require("./s3_sql_query")
const { executeBatchJob } = require("./scrape_urls")

const gameDatesCsvFilePath = "./csv/game_dates.csv"

const timeoutParams = {
  urlTimeoutRequired: true,
  urlTimeoutMinMs: 3000,
  urlTimeoutMaxMs: 6000,
  batchTimeoutRequired: true,
  batchTimeoutMinMs: 360000,
  batchTimeoutMaxMs: 600000,
}

const batchJobParams = {
  minBatchSize: 50,
  maxBatchSize: 80,
}

const s3KeyList = constructS3KeyList(gameDatesCsvFilePath)
scrapeBoxscoreHtmlFiles(s3KeyList)

function constructS3KeyList(gameDatesCsvFilePath) {
  let s3KeyList = []
  readFileSync(gameDatesCsvFilePath, { encoding: "utf8" })
    .trim()
    .split("\n")
    .forEach(s =>
      s3KeyList.push({
        year: s.trim().substring(0, 4),
        key: getS3Key(s.trim()),
      })
    )
  return s3KeyList

  function getS3Key(dateString) {
    const year = dateString.substring(0, 4)
    return `${year}/bbref_games_for_date/${getFileName(dateString)}`
  }

  function getFileName(dateString) {
    const year = dateString.substring(0, 4)
    const month = dateString.substring(4, 6)
    const day = dateString.substring(6, 8)
    return `bbref_games_for_date_${year}-${month}-${day}.json`
  }
}

async function scrapeBoxscoreHtmlFiles(s3KeyList) {
  let allUrls = await constructUrlSet(s3KeyList)
  await executeBatchJob(allUrls, batchJobParams, timeoutParams)
  return

  async function constructUrlSet(s3KeyList) {
    let allUrls = []
    await s3KeyList.reduce(async (promise, s3Details) => {
      await promise
      let urlSet = await constructBoxscoreUrlSet(s3Details.key, s3Details.year)
      allUrls = allUrls.concat(urlSet)
    }, Promise.resolve())
    return allUrls

    async function constructBoxscoreUrlSet(s3FileKey, year) {
      const bucket = "vig-data"
      const query = "select s.boxscore_urls from s3object s"
      const params = getS3SelectQueryParams(bucket, s3FileKey, query)
      const results = await executeS3SelectQuery(params)
      let urlSet = []
      if (results.length > 0) {
        results[0].boxscore_urls.forEach(boxscoreUrl =>
          urlSet.push({
            displayId: `${getBBRefGameIdFromUrl(boxscoreUrl)}           `,
            fileName: `${getBBRefGameIdFromUrl(boxscoreUrl)}.html`,
            htmlFolderPath: `../html_storage/${year}/bbref_boxscores/`,
            s3KeyPrefix: `${year}/bbref_boxscore/html/`,
            url: boxscoreUrl,
          })
        )
      }
      return urlSet

      function getS3SelectQueryParams(bucket, key, query) {
        return {
          Bucket: bucket,
          Key: key,
          ExpressionType: "SQL",
          Expression: query,
          InputSerialization: {
            JSON: {
              Type: "DOCUMENT",
            },
          },
          OutputSerialization: {
            JSON: {
              RecordDelimiter: ",",
            },
          },
        }
      }

      function getBBRefGameIdFromUrl(url) {
        gameIdRegex = /^https:\/\/www.baseball-reference.com\/boxes\/[A-Z]{3,3}\/([0-9A-Z]{12,12}).shtml$/gm
        let match = url.matchAll(gameIdRegex)
        let [m1, _] = match
        return m1[1]
      }
    }
  }
}
