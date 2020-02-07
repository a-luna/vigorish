const { readFileSync } = require("fs")
const { executeS3SelectQuery } = require("./s3_sql_query")
const { executeBatchJob } = require("./scrape_urls")

const brooksGameIdsFilePath = "./csv/bb_game_ids_2019.txt"

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

const pitchLogsS3KeyList = constructS3KeyList(brooksGameIdsFilePath)
scrapePitchFxHtmlFiles(pitchLogsS3KeyList)

function constructS3KeyList(brooksGameIdsFilePath) {
  let s3KeyList = []
  readFileSync(brooksGameIdsFilePath, { encoding: "utf8" })
    .trim()
    .split("\n")
    .forEach(gameId =>
      s3KeyList.push({
        year: gameId.trim().substring(4, 8),
        key: getS3Key(gameId.trim()),
      })
    )
  return s3KeyList

  function getS3Key(gameId) {
    const year = gameId.trim().substring(4, 8)
    return `${year}/brooks_pitch_logs/${game_id}.json`
  }
}

async function scrapePitchFxHtmlFiles(pitchLogsS3KeyList) {
  let allUrls = await constructPitchFxUrlSet(pitchLogsS3KeyList)
  await executeBatchJob(allUrls, batchJobParams, timeoutParams)
  return

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
