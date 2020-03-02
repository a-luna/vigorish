const Nightmare = require("./new_nightmare")
const Xvfb = require("xvfb")
const parseArgs = require("minimist")

const args = parseArgs(process.argv.slice(2))
main(args).catch(console.error)

async function main(args) {
  const timeoutParams = getTimeoutParams(args)
  const batchJobParams = getBatchJobParams(args)
  const nightmare = Nightmare({
    show: false,
    pollInterval: 50,
    waitTimeout: 150000,
    gotoTimeout: 150000,
  })
  const xvfb = new Xvfb()
  xvfb.startSync()

  const [err, title] = await poss(run(nightmare))
  if (err) {
    await nightmare.end()
    xvfb.stopSync()
    throw err
  }

  console.log(title)
  await nightmare.end()
  xvfb.stopSync()
}

function getTimeoutParams(args) {
  let timeoutParams = { urlTimeoutRequired: false, batchTimeoutRequired: false }
  if ("urlTimeoutRequired" in args) {
    timeoutParams.urlTimeoutRequired = true
    if ("urlTimeoutMinMs" in args && "urlTimeoutMaxMs" in args) {
      timeoutParams.urlTimeoutIsRandom = true
      timeoutParams.urlTimeoutMinMs = args.urlTimeoutMinMs
      timeoutParams.urlTimeoutMaxMs = args.urlTimeoutMaxMs
    } else if ("urlTimeoutUniformMs" in args) {
      timeoutParams.urlTimeoutIsRandom = false
      timeoutParams.urlTimeoutUniformMs = args.urlTimeoutUniformMs
    }
  }
  if ("batchTimeoutRequired" in args) {
    timeoutParams.batchTimeoutRequired = true
    if ("batchTimeoutMinMs" in args && "batchTimeoutMaxMs" in args) {
      timeoutParams.batchTimeoutIsRandom = true
      timeoutParams.batchTimeoutMinMs = args.batchTimeoutMinMs
      timeoutParams.batchTimeoutMaxMs = args.batchTimeoutMaxMs
    } else if ("batchTimeoutUniformMs" in args) {
      timeoutParams.batchTimeoutIsRandom = false
      timeoutParams.batchTimeoutUniformMs = args.batchTimeoutUniformMs
    }
  }
  return timeoutParams
}

function getBatchJobParams(args) {
  let batchJobParams = { batchScrapingEnabled: false }
  if ("batchScrapingEnabled" in args) {
    batchJobParams.batchScrapingEnabled = true
    if ("minBatchSize" in args && "maxBatchSize" in args) {
      batchJobParams.batchSizeIsRandom = true
      batchJobParams.minBatchSize = args.minBatchSize
      batchJobParams.maxBatchSize = args.maxBatchSize
    } else if ("uniformBatchSize" in args) {
      batchJobParams.batchSizeIsRandom = false
      batchJobParams.uniformBatchSize = args.uniformBatchSize
    }
  }
  return batchJobParams
}

// run nightmare
//
// put all your nightmare commands in here
async function run(nightmare) {
  await nightmare.goto("https://google.com")
  const title = await nightmare.title()
  return title
}

// try/catch helper
async function poss(promise) {
  try {
    const result = await promise
    return [null, result]
  } catch (err) {
    return [err, null]
  }
}
