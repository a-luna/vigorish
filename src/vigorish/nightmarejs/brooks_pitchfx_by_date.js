const { readFileSync } = require("fs")
const { getS3SelectQueryParams, executeS3SelectQuery } = require("./s3_sql_query")
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

const dailyGamesS3KeyList = constructDailyGamesS3KeyList(gameDatesCsvFilePath)
scrapePitchFxHtmlFiles(dailyGamesS3KeyList)

function constructDailyGamesS3KeyList(gameDatesCsvFilePath) {
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
    return `${year}/brooks_games_for_date/${getFileName(dateString)}`
  }

  function getFileName(dateString) {
    const year = dateString.substring(0, 4)
    const month = dateString.substring(4, 6)
    const day = dateString.substring(6, 8)
    return `brooks_games_for_date_${year}-${month}-${day}.json`
  }
}

async function scrapePitchFxHtmlFiles(dailyGamesS3KeyList) {
  const allS3Keys = await constructPitchLogS3KeyList(dailyGamesS3KeyList)
  let allUrls = await constructPitchFxUrlSet(allS3Keys)
  await executeBatchJob(allUrls, batchJobParams, timeoutParams)
  return

  async function constructPitchLogS3KeyList(pitchLogsS3KeyList) {
    let allS3Keys = []
    await pitchLogsS3KeyList.reduce(async (promise, s3Details) => {
      await promise
      let s3Keys = await constructPitchLogS3KeyListForDate(s3Details.key, s3Details.year)
      allS3Keys = allS3Keys.concat(s3Keys)
    }, Promise.resolve())
    return allS3Keys

    async function constructPitchLogS3KeyListForDate(s3FileKey, year) {
      const bucket = "vig-data"
      const query = "select s.games from s3object s"
      const params = getS3SelectQueryParams(bucket, s3FileKey, query)
      const results = await executeS3SelectQuery(params)
      let s3Keys = []
      if (results.length > 0) {
        results[0].games.forEach(game => {
          if (!game.might_be_postponed) {
            s3Keys.push({
              year: year,
              key: `${year}/brooks_pitch_logs/${game.brooks_game_id}.json`,
            })
          }
        })
      }
      return s3Keys
    }
  }

  async function constructPitchFxUrlSet(pitchLogsS3KeyList) {
    let allUrls = []
    await pitchLogsS3KeyList.reduce(async (promise, s3Details) => {
      await promise
      let urlSet = await constructPitchFxUrlSetForGame(s3Details.key, s3Details.year)
      allUrls = allUrls.concat(urlSet)
    }, Promise.resolve())
    return allUrls

    async function constructPitchFxUrlSetForGame(s3FileKey, year) {
      const bucket = "vig-data"
      const query = "select s.pitch_logs from s3object s"
      const params = getS3SelectQueryParams(bucket, s3FileKey, query)
      const results = await executeS3SelectQuery(params)
      let urlSet = []
      if (results.length > 0) {
        results[0].pitch_logs.forEach(pitch_log => {
          if (pitch_log.parsed_all_info) {
            const pitch_app_id = `${pitch_log.bbref_game_id}_${pitch_log.pitcher_id_mlb}`
            urlSet.push({
              displayId: `${pitch_app_id}    `,
              fileName: `${pitch_app_id}.html`,
              htmlFolderPath: `../html_storage/${year}/brooks_pitchfx/`,
              s3KeyPrefix: `${year}/brooks_pitchfx/html/`,
              url: pitch_log.pitchfx_url,
            })
          }
        })
      }
      return urlSet
    }
  }
}
