const { readFileSync, writeFileSync } = require("fs")
const cliProgress = require("cli-progress")
const colors = require("colors")
const UserAgent = require("user-agents")
const { makeIrregularChunkedList } = require("./list_functions")
const { uploadFileToS3 } = require("./s3_upload")

const multibar = new cliProgress.MultiBar({
  format:
    "|" +
    colors.cyan("{bar}") +
    "| {displayId} | {percentage}% | {value}/{total} {unit}",
  clearOnComplete: false,
  barCompleteChar: "\u2588",
  barIncompleteChar: "\u2591",
  hideCursor: true,
})

async function executeBatchJob(
  nightmare,
  urlSetFilepath,
  batchJobParams,
  timeoutParams,
  s3Bucket
) {
  const allUrls = readUrlSetFromFile(urlSetFilepath)
  let batchCounter = 0,
    urlCounter = 0
  let { batchTimeoutRequired } = timeoutParams
  let [batchList, totalBatches, totalUrls] = createBatchJob(allUrls, batchJobParams)
  let [batchProgress, urlProgress, timeoutProgress] = initializeProgressBars(
    batchList,
    totalUrls
  )
  await batchList.reduce(async (promise, urlBatch) => {
    await promise
    const userAgent = new UserAgent()
    let progressDescription = `${batchList[batchCounter].length} URLs this batch     `
    batchProgress.update(batchCounter, {
      displayId: progressDescription,
      unit: "Batches",
    })
    nightmare.useragent(userAgent.toString())
    urlCounter = await scrapeUrlBatch(
      nightmare,
      urlBatch,
      timeoutParams,
      urlCounter,
      urlProgress,
      s3Bucket
    )
    batchCounter += 1
    progressDescription = ""
    if (batchCounter < totalBatches) {
      progressDescription = `${batchList[batchCounter].length} URLs next batch     `
    } else {
      progressDescription = "Scraped all batches    "
    }
    batchProgress.update(batchCounter, {
      displayId: progressDescription,
      unit: "Batches",
    })
    if (batchTimeoutRequired && batchCounter < totalBatches) {
      await mandatoryTimeout(timeoutProgress, timeoutParams)
    }
  }, Promise.resolve())
  timeoutProgress.stop()
  urlProgress.stop()
  batchProgress.stop()
  return

  function createBatchJob(allUrls, batchJobParams) {
    let { minBatchSize, maxBatchSize } = batchJobParams
    let batchList = makeIrregularChunkedList(allUrls, minBatchSize, maxBatchSize)
    return [batchList, batchList.length, allUrls.length]
  }

  function readUrlSetFromFile(urlSetFilepath) {
    const urlSetText = readFileSync(urlSetFilepath, { encoding: "utf8" })
    return JSON.parse(urlSetText)
  }

  function initializeProgressBars(batchList, totalUrls) {
    const batchProgress = multibar.create(batchList.length, 0, {
      displayId: `${batchList[0].length} URLs this batch     `,
      unit: "Batches",
    })
    const urlProgress = multibar.create(totalUrls, 0, {
      displayId: "In Progress...         ",
      unit: "URLs",
    })
    const timeoutProgress = multibar.create(1, 0, {
      displayId: "Executing Job Batch... ",
      unit: "Seconds",
    })
    return [batchProgress, urlProgress, timeoutProgress]
  }

  async function scrapeUrlBatch(
    nightmare,
    urlSet,
    timeoutParams,
    urlCounter,
    progressBar,
    s3Bucket
  ) {
    await urlSet.reduce(async (promise, urlDetails) => {
      await promise
      let { url, fileName, displayId, htmlFolderPath, s3KeyPrefix } = urlDetails
      progressBar.update(urlCounter, { displayId: displayId, unit: "URLs" })
      let filePath = await scrapeUrl(
        nightmare,
        url,
        fileName,
        htmlFolderPath,
        timeoutParams
      )
      uploadFileToS3(filePath, s3Bucket, s3KeyPrefix, fileName)
      urlCounter += 1
      progressBar.update(urlCounter, { displayId: displayId, unit: "URLs" })
    }, Promise.resolve())
    return urlCounter
  }

  async function mandatoryTimeout(timeoutProgress, timeoutParams) {
    let { batchTimeoutMinMs, batchTimeoutMaxMs } = timeoutParams
    let counter = 1
    const timeoutMs = getRandomInt(batchTimeoutMinMs, batchTimeoutMaxMs)
    let minRemaining = Math.floor(timeoutMs / 60000)
    let secRemaining = Math.floor((timeoutMs - minRemaining * 60000) / 1000)
    var totalSeconds = Math.floor(timeoutMs / 1000)
    let timeoutList = Array.from({ length: totalSeconds }).map(x => 1000)
    timeoutProgress.setTotal(totalSeconds)
    timeoutProgress.update(0, {
      displayId: getTimeoutDisplayString(minRemaining, secRemaining),
      unit: "Seconds",
    })
    await timeoutList.reduce(async (promise, thisTimeout) => {
      await promise
      if (secRemaining == 0) {
        minRemaining -= 1
        secRemaining = 59
      } else {
        secRemaining -= 1
      }
      timeoutProgress.update(counter, {
        displayId: getTimeoutDisplayString(minRemaining, secRemaining),
        unit: "Seconds",
      })
      await sleep(thisTimeout)
      counter += 1
    }, Promise.resolve())
    timeoutProgress.update(0, { displayId: "Executing Job Batch... ", unit: "Seconds" })

    function getTimeoutDisplayString(minRemaining, secRemaining) {
      const minPadded = minRemaining.toString().padStart(2, "0")
      const secPadded = secRemaining.toString().padStart(2, "0")
      return `Until Next Batch: ${minPadded}:${secPadded}`
    }
  }
}

function sleep(timeoutMs) {
  return new Promise(resolve => setTimeout(resolve, timeoutMs))
}

async function scrapeUrls(nightmare, urlSetFilepath, timeoutParams, s3Bucket) {
  const urlSet = readUrlSetFromFile(urlSetFilepath)
  let counter = 1
  const scrapeUrlSetProgress = multibar.create(urlSet.length, 0, {
    displayId: "N/A",
    unit: "URLs",
  })
  await urlSet.reduce(async (promise, urlDetails) => {
    await promise
    let { url, fileName, displayId, htmlFolderPath, s3KeyPrefix } = urlDetails
    let filePath = await scrapeUrl(
      nightmare,
      url,
      fileName,
      htmlFolderPath,
      timeoutParams
    )
    uploadFileToS3(filePath, s3Bucket, s3KeyPrefix, fileName)
    scrapeUrlSetProgress.update(counter, { displayId: displayId, unit: "URLs" })
    counter += 1
  }, Promise.resolve())
  scrapeUrlSetProgress.stop()
}

async function scrapeUrl(
  nightmare,
  url,
  outputFileName,
  outputFolderPath,
  timeoutParams
) {
  let html = await fetchUrl(url, timeoutParams)
  let filePath = `${outputFolderPath}${outputFileName}`
  writeFileSync(filePath, html)
  return filePath

  async function fetchUrl(url, timeoutParams) {
    let { urlTimeoutRequired, urlTimeoutMinMs, urlTimeoutMaxMs } = timeoutParams
    try {
      await nightmare.on("did-get-response-details", () => {
        startTime = Date.now()
      })
      await nightmare.goto(url)
      await nightmare.waitUntilNetworkIdle(500)
      let html = await nightmare.evaluate(() => document.body.innerHTML)
      if (urlTimeoutRequired) {
        const timeout = getRandomInt(urlTimeoutMinMs, urlTimeoutMaxMs)
        await nightmare.wait(timeout)
      }
      return html
    } catch (e) {
      console.log(e)
    }
  }
}

function getRandomInt(min, max) {
  min = Math.ceil(min)
  max = Math.floor(max)
  return Math.floor(Math.random() * (max - min)) + min
}

module.exports = {
  executeBatchJob: executeBatchJob,
  scrapeUrls: scrapeUrls,
  scrapeUrl: scrapeUrl,
}
