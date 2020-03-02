const { readFileSync } = require("fs")
const { getS3SelectQueryParams, executeS3SelectQuery } = require("./s3_sql_query")
const { executeBatchJob } = require("./scrape_urls")

const gameDatesCsvFilePath = "./csv/game_dates.csv"
const bucket = "vig-data"
const query = "select s.games from s3object s"

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

const s3KeyList = constructS3KeyList(gameDatesCsvFilePath)
scrapePitchLogHtmlFiles(s3KeyList)

function constructS3KeyList(gameDatesCsvFilePath) {
  return readFileSync(gameDatesCsvFilePath, { encoding: "utf8" })
    .trim()
    .split("\n")
    .map(s => ({
      year: s.trim().substring(0, 4),
      key: getS3Key(s.trim()),
    }))

  function getS3Key(dateString) {
    const year = dateString.substring(0, 4)
    return `${year}/brooks_games_for_date/${getFileName(dateString)}`
  }

  function getFileName(dateString) {
    const year = dateString.substring(0, 4)
    const month = dateString.substring(4, 6)
    const day = dateString.substring(6, 8)
    return `brooks_games_for_date_${year}-${month}-${day}.json`
  }
}

async function scrapePitchLogHtmlFiles(s3KeyList) {
  let allUrls = await constructUrlSet(s3KeyList)
  await executeBatchJob(allUrls, batchJobParams, timeoutParams)
  return

  async function constructUrlSet(s3KeyList) {
    let allUrls = []
    await s3KeyList.reduce(async (promise, s3Details) => {
      await promise
      let urlSet = await constructPitchLogUrlSet(s3Details.key, s3Details.year)
      allUrls = allUrls.concat(urlSet)
    }, Promise.resolve())
    return allUrls

    async function constructPitchLogUrlSet(s3FileKey, year) {
      const params = getS3SelectQueryParams(bucket, s3FileKey, query)
      const results = await executeS3SelectQuery(params)
      let urlSet = []
      if (results.length == 0) return urlSet

      results[0].games.forEach(game => {
        if (!game.might_be_postponed) {
          urls = Object.entries(game.pitcher_appearance_dict).map(
            ([pitcherIdMlb, pitchLogUrl]) => ({
              displayId: `${game.bbref_game_id}_${pitcherIdMlb}    `,
              fileName: `${game.bbref_game_id}_${pitcherIdMlb}.html`,
              htmlFolderPath: `../html_storage/${year}/brooks_pitch_logs/`,
              s3KeyPrefix: `${year}/brooks_pitch_logs/html/`,
              url: pitchLogUrl,
            })
          )
          urlSet = urlSet.concat(urls)
        }
      })
      return urlSet
    }
  }
}
