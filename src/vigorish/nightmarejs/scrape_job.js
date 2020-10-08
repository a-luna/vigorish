const Xvfb = require("xvfb")
const parseArgs = require("minimist")
const Nightmare = require("./new_nightmare")
const { executeBatchJob, scrapeUrls } = require("./scrape_urls")
require("events").EventEmitter.defaultMaxListeners = 0

const args = parseArgs(process.argv.slice(2))
main(args).catch(console.error)

async function main(args) {
    const xvfb = new Xvfb()
    const nightmare = Nightmare({
        show: false,
        pollInterval: 50,
        waitTimeout: 150000,
        gotoTimeout: 150000,
    })
    jobParams = {
        urlSetFilepath: args.urlSetFilepath,
        batchParams: getBatchParams(args),
        timeoutParams: getTimeoutParams(args),
    }
    const [success, err] = await run(xvfb, nightmare, jobParams)
    if (!success) {
        throw err
    }
    process.exit(0)
}

function getBatchParams(args) {
    let batchParams = { batchScrapingEnabled: false }
    if ("batchScrapingEnabled" in args) {
        batchParams.batchScrapingEnabled = true
        if ("minBatchSize" in args && "maxBatchSize" in args) {
            batchParams.batchSizeIsRandom = true
            batchParams.minBatchSize = args.minBatchSize
            batchParams.maxBatchSize = args.maxBatchSize
        } else if ("uniformBatchSize" in args) {
            batchParams.batchSizeIsRandom = false
            batchParams.uniformBatchSize = args.uniformBatchSize
        }
    }
    return batchParams
}

function getTimeoutParams(args) {
    let timeoutParams = { urlTimeoutRequired: false, batchTimeoutRequired: false }
    if ("urlTimeoutRequired" in args) {
        timeoutParams.urlTimeoutRequired = true
        if ("urlTimeoutMinMs" in args && "urlTimeoutMaxMs" in args) {
            timeoutParams.urlTimeoutMinMs = args.urlTimeoutMinMs
            timeoutParams.urlTimeoutMaxMs = args.urlTimeoutMaxMs
        } else if ("urlTimeoutUniformMs" in args) {
            timeoutParams.urlTimeoutMinMs = args.urlTimeoutUniformMs
            timeoutParams.urlTimeoutMaxMs = args.urlTimeoutUniformMs
        }
    }
    if ("batchTimeoutRequired" in args) {
        timeoutParams.batchTimeoutRequired = true
        if ("batchTimeoutMinMs" in args && "batchTimeoutMaxMs" in args) {
            timeoutParams.batchTimeoutMinMs = args.batchTimeoutMinMs
            timeoutParams.batchTimeoutMaxMs = args.batchTimeoutMaxMs
        } else if ("batchTimeoutUniformMs" in args) {
            timeoutParams.batchTimeoutMinMs = args.batchTimeoutUniformMs
            timeoutParams.batchTimeoutMaxMs = args.batchTimeoutUniformMs
        }
    }
    return timeoutParams
}

async function run(xvfb, nightmare, jobParams) {
    try {
        xvfb.startSync()
        await execute_job(nightmare, jobParams)
        return [true, null]
    } catch (err) {
        return [false, err]
    } finally {
        xvfb.stopSync()
        await nightmare.end()
    }
}

async function execute_job(nightmare, { urlSetFilepath, timeoutParams, batchParams }) {
    if (batchParams.batchScrapingEnabled) {
        await executeBatchJob(nightmare, urlSetFilepath, batchParams, timeoutParams)
    } else {
        await scrapeUrls(nightmare, urlSetFilepath, timeoutParams)
    }
}
