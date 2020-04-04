const { readFileSync, writeFileSync } = require("fs")
const cliProgress = require("cli-progress")
const colors = require("colors")
const UserAgent = require("user-agents")
var joinPath = require("path.join")
const { makeChunkedList, makeIrregularChunkedList } = require("./list_functions")

const multibar = new cliProgress.MultiBar({
  format:
    "|" +
    colors.cyan("{bar}") +
    "| {displayId} | {percentage}% | {value}/{total} {unit}",
  clearOnComplete: false,
  barCompleteChar: "\u2588",
  barIncompleteChar: "\u2591",
  hideCursor: true,
  emptyOnZero: true,
})

const PBAR_MAX_LENGTH = 23

async function executeBatchJob(
  nightmare,
  urlSetFilepath,
  batchJobParams,
  timeoutParams
) {
  const allUrls = readUrlSetFromFile(urlSetFilepath)
  let batchCounter = 0
  let urlCounter = 0
  let { batchTimeoutRequired } = timeoutParams
  let [batchList, totalBatches, totalUrls] = createBatchJob(allUrls, batchJobParams)
  let [batchBar, urlBar, delayBar] = initializeProgressBars(batchList, totalUrls)
  const userAgent = new UserAgent()
  nightmare.useragent(userAgent.toString())
  await batchList.reduce(async (promise, urlBatch) => {
    await promise
    batchBar.update(batchCounter, {
      displayId: getPbarDisplayStr(`${batchList[batchCounter].length} URLs this batch`),
      unit: "Batches",
    })
    urlCounter = await scrapeUrlBatch(
      nightmare,
      urlBatch,
      timeoutParams,
      urlCounter,
      urlBar
    )
    batchCounter += 1
    if (batchTimeoutRequired && batchCounter < totalBatches) {
      const nextBatchSize = batchList[batchCounter].length
      const nextBatchStr = `${nextBatchSize} URLs next batch`
      batchBar.update(batchCounter, {
        displayId: getPbarDisplayStr(nextBatchStr, (padEllipses = false)),
        unit: "Batches",
      })
      await batchTimeout(delayBar, timeoutParams, urlCounter, nextBatchSize)
    }
  }, Promise.resolve())
  batchBar.update(batchCounter, {
    displayId: getPbarDisplayStr("Scraped all batches"),
    unit: "Batches",
  })
  multibar.stop()
  return

  function readUrlSetFromFile(urlSetFilepath) {
    const urlSetText = readFileSync(urlSetFilepath, { encoding: "utf8" })
    return JSON.parse(urlSetText)
  }

  function createBatchJob(allUrls, batchJobParams) {
    let batchList
    if (batchJobParams.batchSizeIsRandom) {
      let { minBatchSize, maxBatchSize } = batchJobParams
      batchList = makeIrregularChunkedList(allUrls, minBatchSize, maxBatchSize)
    } else {
      batchList = makeChunkedList(allUrls, batchJobParams.uniformBatchSize)
    }
    return [batchList, batchList.length, allUrls.length]
  }

  function initializeProgressBars(batchList, totalUrls) {
    const batchBar = multibar.create(batchList.length, 0, {
      displayId: getPbarDisplayStr(`${batchList[0].length} URLs this batch`),
      unit: "Batches",
    })
    const urlBar = multibar.create(totalUrls, 0, {
      displayId: getPbarDisplayStr("In Progress"),
      unit: "URLs",
    })
    const delayBar = multibar.create(0, 0, {
      displayId: getNextUrlRangeStr(0, batchList[0].length),
      unit: "",
    })
    return [batchBar, urlBar, delayBar]
  }

  function getNextUrlRangeStr(urlCounter, nextBatchSize) {
    let nextUrlRange = `Scraping URLs ${urlCounter}-${urlCounter + nextBatchSize}`
    return getPbarDisplayStr(nextUrlRange)
  }

  function getPbarDisplayStr(displayStr, padEllipses = true) {
    let padLength = PBAR_MAX_LENGTH - displayStr.length
    if (padLength <= 0) {
      return displayStr
    }
    if (padLength >= 3 && padEllipses) {
      displayStr += "..."
      padLength -= 3
    }
    displayStr += " ".repeat(padLength)
    return displayStr
  }

  async function scrapeUrlBatch(
    nightmare,
    urlSet,
    timeoutParams,
    urlCounter,
    progressBar
  ) {
    await urlSet.reduce(async (promise, urlDetails) => {
      await promise
      let { url, fileName, displayId, htmlFolderPath } = urlDetails
      progressBar.update(urlCounter, {
        displayId: getPbarDisplayStr(displayId, (padEllipses = false)),
        unit: "URLs",
      })
      await scrapeUrl(nightmare, url, fileName, htmlFolderPath, timeoutParams)
      urlCounter += 1
    }, Promise.resolve())
    progressBar.update(urlCounter)
    return urlCounter
  }

  async function batchTimeout(delayBar, timeoutParams, urlCounter, nextBatchSize) {
    let { batchTimeoutMinMs, batchTimeoutMaxMs } = timeoutParams
    let counter = 1
    const timeoutMs = getRandomInt(batchTimeoutMinMs, batchTimeoutMaxMs)
    let minRemaining = Math.floor(timeoutMs / 60000)
    let secRemaining = Math.floor((timeoutMs - minRemaining * 60000) / 1000)
    var totalSeconds = Math.floor(timeoutMs / 1000)
    let timeoutList = Array.from({ length: totalSeconds }).map(x => 1000)
    delayBar.setTotal(totalSeconds)
    delayBar.update(0, {
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
      delayBar.update(counter, {
        displayId: getTimeoutDisplayString(minRemaining, secRemaining),
        unit: "Seconds",
      })
      await sleep(thisTimeout)
      counter += 1
    }, Promise.resolve())
    delayBar.setTotal(0)
    delayBar.update(0, {
      displayId: getNextUrlRangeStr(urlCounter, nextBatchSize),
      unit: "",
    })
    return

    function getTimeoutDisplayString(minRemaining, secRemaining) {
      const minPadded = minRemaining.toString().padStart(2, "0")
      const secPadded = secRemaining.toString().padStart(2, "0")
      return getPbarDisplayStr(`${minPadded}:${secPadded} Until Next Batch`)
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
    await scrapeUrl(nightmare, url, fileName, htmlFolderPath, timeoutParams)
    scrapeUrlSetProgress.update(counter, { displayId: displayId, unit: "URLs" })
    counter += 1
  }, Promise.resolve())
  scrapeUrlSetProgress.stop()
  return
}

async function scrapeUrl(
  nightmare,
  url,
  outputFileName,
  outputFolderPath,
  timeoutParams
) {
  let html = await fetchUrl(url, timeoutParams)
  let filePath = joinPath(outputFolderPath, outputFileName)
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
