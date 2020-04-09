const { readFileSync, writeFileSync } = require("fs")
const cliProgress = require("cli-progress")
const colors = require("colors")
const UserAgent = require("user-agents")
var joinPath = require("path.join")
const { makeChunkedList, makeIrregularChunkedList } = require("./list_functions")

const PBAR_MAX_LENGTH = 23

const multibar = new cliProgress.MultiBar({
    format: "|" + colors.cyan("{bar}") + "| {message} | {percentage}% | {value}/{total} {unit}",
    clearOnComplete: false,
    barCompleteChar: "\u2588",
    barIncompleteChar: "\u2591",
    hideCursor: true,
    emptyOnZero: true,
})

async function executeBatchJob(nightmare, urlSetFilepath, batchJobParams, timeoutParams) {
    let batchCounter = 0
    let urlCounter = 0
    let pBarDisplay = ""
    const allUrls = readUrlSetFromFile(urlSetFilepath)
    let [batchList, totalBatches, totalUrls] = createBatchJob(allUrls, batchJobParams)
    let [batchBar, urlBar, timeoutBar] = initializeProgressBars(batchList, totalUrls)
    nightmare.useragent(new UserAgent().toString())
    await batchList.reduce(async (promise, urlBatch) => {
        await promise
        const batchSize = batchList[batchCounter].length
        batchBar.update(batchCounter, { message: pBarMessage(`${batchSize} URLs this batch`) })
        urlCounter = await scrapeUrlBatch(nightmare, urlBatch, timeoutParams, urlCounter, urlBar)
        batchCounter += 1
        if (batchCounter < totalBatches) {
            const nextBatchSize = batchList[batchCounter].length
            const nextBatchStr = pBarMessage(`${nextBatchSize} URLs next batch`, false)
            batchBar.update(batchCounter, { message: nextBatchStr })
            if (timeoutParams.batchTimeoutRequired) {
                await batchTimeout(timeoutParams, timeoutBar, urlCounter, nextBatchSize)
            }
        }
    }, Promise.resolve())
    batchBar.update(batchCounter, { message: pBarMessage("Scraped all batches") })
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
            message: pBarMessage(`${batchList[0].length} URLs this batch`),
            unit: "Batches",
        })
        const urlBar = multibar.create(totalUrls, 0, {
            message: pBarMessage("In Progress"),
            unit: "URLs",
        })
        const timeoutBar = multibar.create(0, 0, {
            message: getNextUrlRangeStr(0, batchList[0].length),
            unit: "",
        })
        return [batchBar, urlBar, timeoutBar]
    }

    function getNextUrlRangeStr(urlCounter, nextBatchSize) {
        let nextUrlRange = `Scraping URLs ${urlCounter + 1}-${urlCounter + nextBatchSize}`
        return pBarMessage(nextUrlRange)
    }

    function pBarMessage(displayStr, padEllipses = true) {
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

    async function scrapeUrlBatch(nightmare, urlSet, timeoutParams, urlCounter, progressBar) {
        await urlSet.reduce(async (promise, urlDetails) => {
            await promise
            let { url, fileName, identifier, scrapedHtmlFolderpath } = urlDetails
            const pbarDisplay = pBarMessage(identifier, (padEllipses = false))
            progressBar.update(urlCounter, { message: pbarDisplay })
            await scrapeUrl(nightmare, url, fileName, scrapedHtmlFolderpath, timeoutParams)
            urlCounter += 1
        }, Promise.resolve())
        progressBar.update(urlCounter)
        return urlCounter
    }

    async function batchTimeout(timeoutParams, progressBar, urlCounter, nextBatchSize) {
        let [timeoutList, minutes, seconds] = getRandomTimeout(timeoutParams)
        progressBar.setTotal(timeoutList.length)
        progressBar.update(0, { message: getTimeoutDisplay(minutes, seconds), unit: "Seconds" })
        let counter = 0
        await timeoutList.reduce(async (promise, timeout) => {
            await promise
            await sleep(timeout)
            if (seconds == 0) {
                minutes -= 1
                seconds = 59
            } else {
                seconds -= 1
            }
            counter += 1
            progressBar.update(counter, { message: getTimeoutDisplay(minutes, seconds) })
        }, Promise.resolve())
        progressBar.setTotal(0)
        progressBar.update(0, { message: getNextUrlRangeStr(urlCounter, nextBatchSize), unit: "" })
        return

        function getRandomTimeout({ batchTimeoutMinMs, batchTimeoutMaxMs }) {
            const timeoutMs = getRandomInt(batchTimeoutMinMs, batchTimeoutMaxMs)
            const minutes = Math.floor(timeoutMs / 60000)
            const seconds = Math.floor((timeoutMs - minutes * 60000) / 1000)
            const totalSeconds = Math.floor(timeoutMs / 1000)
            const timeoutList = Array.from({ length: totalSeconds }).map(x => 1000)
            return [timeoutList, minutes, seconds]
        }

        function getTimeoutDisplay(minutes, seconds) {
            const minPadded = minutes.toString().padStart(2, "0")
            const secPadded = seconds.toString().padStart(2, "0")
            return pBarMessage(`${minPadded}:${secPadded} until next batch`)
        }
    }
}

function sleep(timeoutMs) {
    return new Promise(resolve => setTimeout(resolve, timeoutMs))
}

async function scrapeUrls(nightmare, urlSetFilepath, timeoutParams) {
    const urlSet = readUrlSetFromFile(urlSetFilepath)
    const urlBar = multibar.create(urlSet.length, 0, { message: "N/A", unit: "URLs" })
    let counter = 1
    await urlSet.reduce(async (promise, urlDetails) => {
        await promise
        let { url, fileName, identifier, scrapedHtmlFolderpath } = urlDetails
        await scrapeUrl(nightmare, url, fileName, scrapedHtmlFolderpath, timeoutParams)
        urlBar.update(counter, { message: identifier })
        counter += 1
    }, Promise.resolve())
    urlBar.stop()
    return
}

async function scrapeUrl(nightmare, url, outputFileName, outputFolderPath, timeoutParams) {
    let html = await fetchUrl(url, timeoutParams)
    let filePath = joinPath(outputFolderPath, outputFileName)
    writeFileSync(filePath, html)
    return filePath

    async function fetchUrl(url, timeoutParams) {
        let { urlTimeoutMinMs, urlTimeoutMaxMs } = timeoutParams
        try {
            await nightmare.on("did-get-response-details", () => {
                startTime = Date.now()
            })
            await nightmare.goto(url)
            await nightmare.waitUntilNetworkIdle(500)
            let html = await nightmare.evaluate(() => document.body.innerHTML)
            await nightmare.wait(getRandomInt(urlTimeoutMinMs, urlTimeoutMaxMs))
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
