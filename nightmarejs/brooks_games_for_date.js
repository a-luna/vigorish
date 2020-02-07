const { readFileSync } = require("fs")
const { executeBatchJob } = require("./scrape_urls")

const gameDatesCsvFilePath = "./csv/game_dates.csv"

const timeoutParams = {
  urlTimeoutRequired: true,
  urlTimeoutMinMs: 3000,
  urlTimeoutMaxMs: 6000,
  batchTimeoutRequired: true,
  batchTimeoutMinMs: 1800000,
  batchTimeoutMaxMs: 2700000,
}

const batchJobParams = {
  minBatchSize: 50,
  maxBatchSize: 80,
}

let urlSet = constructUrlSet(gameDatesCsvFilePath)
executeBatchJob(urlSet, batchJobParams, timeoutParams)

function constructUrlSet(gameDatesCsvFilePath) {
  let urlSet = []
  readFileSync(gameDatesCsvFilePath, { encoding: "utf8" })
    .trim()
    .split("\n")
    .forEach(s => {
      const year = s.trim().substring(0, 4)
      const month = s.trim().substring(4, 6)
      const day = s.trim().substring(6, 8)
      urlSet.push({
        displayId: `${month}/${day}/${year}             `,
        fileName: `${s.trim()}.html`,
        htmlFolderPath: `../html_storage/${year}/brooks_games_for_date/`,
        s3KeyPrefix: `${year}/brooks_games_for_date/html/`,
        url: `http://www.brooksbaseball.net/dashboard.php?dts=${month}/${day}/${year}`,
      })
    })
  return urlSet
}
