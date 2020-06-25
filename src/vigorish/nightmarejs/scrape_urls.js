const { readFileSync, writeFileSync } = require("fs")
const cliProgress = require("cli-progress")
const colors = require("colors")
var joinPath = require("path.join")
const { makeChunkedList, makeIrregularChunkedList } = require("./list_functions")

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

let PBAR_MAX_LENGTH
const PBAR_MAX_LEN_SINGLE_BATCH = 20
const PBAR_MAX_LEN_LT_100_URLS = 20
const PBAR_MAX_LEN_LT_1K_URLS = 22
const PBAR_MAX_LEN_LT_10K_URLS = 24
const PBAR_MAX_LEN_GT_10K_URLS = 26

async function executeBatchJob(nightmare, urlSetFilepath, batchJobParams, timeoutParams) {
    let batchCounter = 0
    let urlCounter = 0
    const allUrls = readUrlSetFromFile(urlSetFilepath)
    let batchList = createBatchJob(allUrls, batchJobParams)
    let [batchBar, urlBar, timeoutBar] = initializeProgressBars(batchList, allUrls.length)
    for await (const urlBatch of batchList) {
        batchBar.update(batchCounter, { message: urlBatchMessage(batchList[batchCounter].length) })
        urlCounter = await scrapeUrlBatch(nightmare, urlBatch, timeoutParams, urlCounter, urlBar)
        batchCounter += 1
        if (batchCounter < batchList.length) {
            const nextBatchSize = batchList[batchCounter].length
            batchBar.update(batchCounter, { message: urlBatchMessage(nextBatchSize, true) })
            if (timeoutParams.batchTimeoutRequired) {
                await batchTimeout(timeoutParams, timeoutBar, urlCounter, nextBatchSize)
            }
        }
    }
    batchBar.update(batchCounter, { message: pBarMessage("Scraped all batches") })
    multibar.stop()
    return

    function createBatchJob(
        allUrls,
        { batchSizeIsRandom, minBatchSize, maxBatchSize, uniformBatchSize }
    ) {
        return batchSizeIsRandom
            ? makeIrregularChunkedList(allUrls, minBatchSize, maxBatchSize)
            : makeChunkedList(allUrls, uniformBatchSize)
    }

    function initializeProgressBars(batchList, totalUrls) {
        set_pbar_max_length(batchList.length, totalUrls)
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

    function set_pbar_max_length(totalBatches, totalUrls) {
        PBAR_MAX_LENGTH =
            totalBatches == 1
                ? PBAR_MAX_LEN_SINGLE_BATCH
                : totalUrls < 100
                ? PBAR_MAX_LEN_LT_100_URLS
                : totalUrls < 1000
                ? PBAR_MAX_LEN_LT_1K_URLS
                : totalUrls < 10000
                ? PBAR_MAX_LEN_LT_10K_URLS
                : PBAR_MAX_LEN_GT_10K_URLS
    }

    function urlRangeMessage(urlCounter, nextBatchSize) {
        return pBarMessage(`Scraping URLs ${urlCounter + 1}-${urlCounter + nextBatchSize}`)
    }

    function urlBatchMessage(batchSize, isTimeout = false) {
        return pBarMessage(`${batchSize} URLs ${isTimeout ? "next" : "this"} batch`)
    }

    function pBarMessage(input) {
        return input.length >= PBAR_MAX_LENGTH
            ? input
            : `${" ".repeat(PBAR_MAX_LENGTH - input.length)}${input}`
    }

    async function scrapeUrlBatch(nightmare, urlSet, timeoutParams, urlCounter, progressBar) {
        for await (const { url, fileName, identifier, scrapedHtmlFolderpath } of urlSet) {
            progressBar.update(urlCounter, { message: pBarMessage(identifier) })
            await scrapeUrl(nightmare, url, fileName, scrapedHtmlFolderpath, timeoutParams)
            urlCounter += 1
        }
        progressBar.update(urlCounter)
        return urlCounter
    }

    async function batchTimeout(timeoutParams, progressBar, urlCounter, nextBatchSize) {
        let [timeoutList, minutes, seconds] = getRandomTimeout(timeoutParams)
        let secondsRemaining = timeoutList.length
        progressBar.setTotal(secondsRemaining)
        progressBar.update(secondsRemaining, { message: timeoutMessage(minutes, seconds), unit: "Seconds" })
        for await (const timeout of timeoutList) {
            await sleep(timeout)
            if (seconds == 0) {
                minutes -= 1
                seconds = 59
            } else {
                seconds -= 1
            }
            secondsRemaining -= 1
            progressBar.update(secondsRemaining, { message: timeoutMessage(minutes, seconds) })
        }
        progressBar.setTotal(0)
        progressBar.update(0, { message: urlRangeMessage(urlCounter, nextBatchSize), unit: "" })
        return

        function getRandomTimeout({ batchTimeoutMinMs, batchTimeoutMaxMs }) {
            const timeoutMs = getRandomInt(batchTimeoutMinMs, batchTimeoutMaxMs)
            const minutes = Math.floor(timeoutMs / 60000)
            const seconds = Math.floor((timeoutMs - minutes * 60000) / 1000)
            const totalSeconds = Math.floor(timeoutMs / 1000)
            const timeoutList = Array(totalSeconds).fill(1000)
            return [timeoutList, minutes, seconds]
        }

        function timeoutMessage(minutes, seconds) {
            const minPad = minutes.toString().padStart(2, "0")
            const secPad = seconds.toString().padStart(2, "0")
            return pBarMessage(`${minPad}:${secPad} until next batch`)
        }
    }
}

async function scrapeUrls(nightmare, urlSetFilepath, timeoutParams) {
    const urlSet = readUrlSetFromFile(urlSetFilepath)
    const urlBar = multibar.create(urlSet.length, 0, { message: "N/A", unit: "URLs" })
    let counter = 0
    for await (const { url, fileName, identifier, scrapedHtmlFolderpath } of urlSet) {
        urlBar.update(counter, { message: identifier })
        await scrapeUrl(nightmare, url, fileName, scrapedHtmlFolderpath, timeoutParams)
        counter += 1
    }
    urlBar.stop()
    return
}

async function scrapeUrl(nightmare, url, outputFileName, outputFolderPath, timeoutParams) {
    let html = await fetchUrl(url, timeoutParams)
    let filePath = joinPath(outputFolderPath, outputFileName)
    writeFileSync(filePath, html)
    return filePath

    async function fetchUrl(url, { urlTimeoutMinMs, urlTimeoutMaxMs }) {
        try {
            await nightmare.on("did-get-response-details", () => (startTime = Date.now()))
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
    return JSON.parse(readFileSync(urlSetFilepath, { encoding: "utf8" }))
}

function sleep(timeoutMs) {
    return new Promise((resolve) => setTimeout(resolve, timeoutMs))
}

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (Math.floor(max) - Math.ceil(min))) + Math.ceil(min)
}

module.exports = {
    executeBatchJob: executeBatchJob,
    scrapeUrls: scrapeUrls,
    scrapeUrl: scrapeUrl,
}
