const { readFileSync, writeFileSync } = require("fs")
const cliProgress = require("cli-progress")
const colors = require("colors")
var joinPath = require("path.join")
const { makeChunkedList, makeIrregularChunkedList } = require("./list_functions")

const PBAR_MAX_LENGTH = 23

const multibar = new cliProgress.MultiBar({
    format: "{message} " + colors.cyan("|{bar}| ") + " {percentage}% | {value}/{total} {unit}",
    barCompleteChar: "\u2588",
    barIncompleteChar: "\u2591",
    stopOnComplete: true,
    clearOnComplete: true,
    hideCursor: true,
    emptyOnZero: true,
    autopadding: true,
})

async function executeBatchJob(nightmare, urlSetFilepath, batchJobParams, timeoutParams) {
    let batchCounter = 0
    let urlCounter = 0
    const allUrls = readUrlSetFromFile(urlSetFilepath)
    let [batchList, totalBatches, totalUrls] = createBatchJob(allUrls, batchJobParams)
    let [batchBar, urlBar, timeoutBar] = initializeProgressBars(batchList, totalUrls)
    await batchList.reduce(async (promise, urlBatch) => {
        await promise
        batchBar.update(batchCounter, { message: urlBatchMessage(batchList[batchCounter].length) })
        urlCounter = await scrapeUrlBatch(nightmare, urlBatch, timeoutParams, urlCounter, urlBar)
        batchCounter += 1
        if (batchCounter < totalBatches) {
            const nextBatchSize = batchList[batchCounter].length
            batchBar.update(batchCounter, { message: urlBatchMessage(nextBatchSize, true) })
            if (timeoutParams.batchTimeoutRequired) {
                await batchTimeout(timeoutParams, timeoutBar, urlCounter, nextBatchSize)
            }
        }
    }, Promise.resolve())
    batchBar.update(batchCounter, { message: pBarMessage("Scraped all batches") })
    multibar.stop()
    return

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
        const timeoutBar = multibar.create(0, 0, {
            message: urlRangeMessage(0, batchList[0].length),
            unit: "",
        })
        const batchBar = multibar.create(batchList.length, 0, {
            message: urlBatchMessage(batchList[0].length),
            unit: "Batches",
        })
        const urlBar = multibar.create(totalUrls, 0, {
            message: pBarMessage("In Progress"),
            unit: "URLs",
        })
        return [batchBar, urlBar, timeoutBar]
    }

    function urlRangeMessage(urlCounter, nextBatchSize) {
        let nextUrlRange = `Scraping URLs ${urlCounter + 1}-${urlCounter + nextBatchSize}`
        return pBarMessage(nextUrlRange)
    }

    function urlBatchMessage(batchSize, isTimeout = false) {
        const thisNext = isTimeout ? "next" : "this"
        const batchMessage = `${batchSize} URLs ${thisNext} batch`
        return pBarMessage(batchMessage)
    }

    function pBarMessage(input) {
        let padLength = PBAR_MAX_LENGTH - input.length
        if (padLength <= 0) {
            return input
        }
        padLeft = Math.floor(padLength / 2)
        padRight = padLength - padLeft
        return `${" ".repeat(padLeft)}${input}${" ".repeat(padRight)}`
    }

    async function scrapeUrlBatch(nightmare, urlSet, timeoutParams, urlCounter, progressBar) {
        await urlSet.reduce(async (promise, urlDetails) => {
            await promise
            let { url, fileName, identifier, scrapedHtmlFolderpath } = urlDetails
            progressBar.update(urlCounter, { message: pBarMessage(identifier) })
            await scrapeUrl(nightmare, url, fileName, scrapedHtmlFolderpath, timeoutParams)
            urlCounter += 1
        }, Promise.resolve())
        progressBar.update(urlCounter)
        return urlCounter
    }

    async function batchTimeout(timeoutParams, progressBar, urlCounter, nextBatchSize) {
        let [timeoutList, minutes, seconds] = getRandomTimeout(timeoutParams)
        progressBar.setTotal(timeoutList.length)
        progressBar.update(0, { message: timeoutMessage(minutes, seconds), unit: "Seconds" })
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
            progressBar.update(counter, { message: timeoutMessage(minutes, seconds) })
        }, Promise.resolve())
        progressBar.setTotal(0)
        progressBar.update(0, { message: urlRangeMessage(urlCounter, nextBatchSize), unit: "" })
        return

        function getRandomTimeout({ batchTimeoutMinMs, batchTimeoutMaxMs }) {
            const timeoutMs = getRandomInt(batchTimeoutMinMs, batchTimeoutMaxMs)
            const minutes = Math.floor(timeoutMs / 60000)
            const seconds = Math.floor((timeoutMs - minutes * 60000) / 1000)
            const totalSeconds = Math.floor(timeoutMs / 1000)
            const timeoutList = Array.from({ length: totalSeconds }).map((x) => 1000)
            return [timeoutList, minutes, seconds]
        }

        function timeoutMessage(minutes, seconds) {
            const minPadded = minutes.toString().padStart(2, "0")
            const secPadded = seconds.toString().padStart(2, "0")
            return pBarMessage(`${minPadded}:${secPadded} until next batch`)
        }
    }
}

async function scrapeUrls(nightmare, urlSetFilepath, timeoutParams) {
    const urlSet = readUrlSetFromFile(urlSetFilepath)
    const urlBar = multibar.create(urlSet.length, 0, { message: "N/A", unit: "URLs" })
    let counter = 0
    await urlSet.reduce(async (promise, urlDetails) => {
        await promise
        let { url, fileName, identifier, scrapedHtmlFolderpath } = urlDetails
        urlBar.update(counter, { message: identifier })
        await scrapeUrl(nightmare, url, fileName, scrapedHtmlFolderpath, timeoutParams)
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

function readUrlSetFromFile(urlSetFilepath) {
    const urlSetText = readFileSync(urlSetFilepath, { encoding: "utf8" })
    return JSON.parse(urlSetText)
}

function sleep(timeoutMs) {
    return new Promise((resolve) => setTimeout(resolve, timeoutMs))
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
