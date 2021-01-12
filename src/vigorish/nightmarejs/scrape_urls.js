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
const PBAR_MAX_LEN_LT_1K_URLS = 22
const PBAR_MAX_LEN_LT_10K_URLS = 24
const PBAR_MAX_LEN_GT_10K_URLS = 26

const readUrlSetFromFile = (urlSetFilepath) =>
    JSON.parse(readFileSync(urlSetFilepath, { encoding: "utf8" }))

const createBatchJob = (
    allUrls,
    { batchSizeIsRandom, minBatchSize, maxBatchSize, uniformBatchSize }
) =>
    batchSizeIsRandom
        ? makeIrregularChunkedList(allUrls, minBatchSize, maxBatchSize)
        : makeChunkedList(allUrls, uniformBatchSize)

const set_pbar_max_length = (totalBatches, totalUrls) =>
(PBAR_MAX_LENGTH =
    totalBatches == 1
        ? PBAR_MAX_LEN_SINGLE_BATCH
        : totalUrls < 1000
            ? PBAR_MAX_LEN_LT_1K_URLS
            : totalUrls < 10000
                ? PBAR_MAX_LEN_LT_10K_URLS
                : PBAR_MAX_LEN_GT_10K_URLS)

const pBarMessage = (input) =>
    input.length >= PBAR_MAX_LENGTH
        ? input
        : `${" ".repeat(PBAR_MAX_LENGTH - input.length)}${input}`

const urlRangeMessage = (urlCounter, nextBatchSize) =>
    pBarMessage(`Scraping URLs ${urlCounter + 1}-${urlCounter + nextBatchSize}`)

const urlBatchMessage = (batchSize, isTimeout = false) =>
    pBarMessage(`${batchSize} URLs ${isTimeout ? "next" : "this"} batch`)

const sleep = (timeoutMs) => new Promise((resolve) => setTimeout(resolve, timeoutMs))

const getRandomInt = (min, max) =>
    Math.floor(Math.random() * (Math.floor(max) - Math.ceil(min))) + Math.ceil(min)

async function executeBatchJob(nightmare, urlSetFilepath, batchParams, timeoutParams) {
    let batchCounter = 0
    let urlCounter = 0
    const allUrls = readUrlSetFromFile(urlSetFilepath)
    const batchList = createBatchJob(allUrls, batchParams)
    const [batchBar, urlBar, comboBar] = initializeProgressBars(batchList, allUrls.length)
    for await (const urlBatch of batchList) {
        batchBar.update(batchCounter, { message: urlBatchMessage(batchList[batchCounter].length) })
        urlCounter = await scrapeUrlBatch(
            nightmare,
            urlBatch,
            timeoutParams,
            urlCounter,
            urlBar,
            comboBar
        )
        batchCounter += 1
        if (batchCounter < batchList.length) {
            const nextBatchSize = batchList[batchCounter].length
            batchBar.update(batchCounter, { message: urlBatchMessage(nextBatchSize, true) })
            if (timeoutParams.batchTimeoutRequired) {
                await batchTimeout(timeoutParams, comboBar, urlCounter, nextBatchSize)
            }
        }
    }
    batchBar.update(batchCounter, { message: pBarMessage("Scraped all batches") })
    multibar.stop()
}

function initializeProgressBars(batchList, totalUrls) {
    set_pbar_max_length(batchList.length, totalUrls)
    const comboBar = multibar.create(0, 0, {
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
    return [batchBar, urlBar, comboBar]
}

async function scrapeUrlBatch(nightmare, urlSet, timeoutParams, urlCounter, urlBar, comboBar) {
    comboBar.setTotal(urlSet.length)
    const urlCounterStart = urlCounter
    for await (const { url, url_id, htmlFileName, htmlFolderpath } of urlSet) {
        comboBar.update(urlCounter - urlCounterStart, {
            message: urlRangeMessage(urlCounterStart, urlSet.length),
            unit: "URLs",
        })
        urlBar.update(urlCounter, { message: pBarMessage(url_id) })
        await scrapeUrl(nightmare, url, htmlFileName, htmlFolderpath, timeoutParams)
        urlCounter += 1
    }
    urlBar.update(urlCounter)
    comboBar.update(urlCounter - urlCounterStart)
    return urlCounter
}

async function batchTimeout(timeoutParams, comboBar, urlCounter, nextBatchSize) {
    let [timeoutList, minutes, seconds] = getRandomTimeout(timeoutParams)
    let secondsRemaining = timeoutList.length
    comboBar.setTotal(secondsRemaining)
    comboBar.update(secondsRemaining, {
        message: timeoutMessage(minutes, seconds),
        unit: "Seconds",
    })
    for await (const timeout of timeoutList) {
        await sleep(timeout)
        if (seconds == 0) {
            minutes -= 1
            seconds = 59
        } else {
            seconds -= 1
        }
        secondsRemaining -= 1
        comboBar.update(secondsRemaining, { message: timeoutMessage(minutes, seconds) })
    }
    comboBar.setTotal(0)
    comboBar.update(0, { message: urlRangeMessage(urlCounter, nextBatchSize), unit: "" })
}

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

async function scrapeUrls(nightmare, urlSetFilepath, timeoutParams) {
    const urlSet = readUrlSetFromFile(urlSetFilepath)
    const urlBar = multibar.create(urlSet.length, 0, { message: "N/A", unit: "URLs" })
    let counter = 0
    for await (const { url, url_id, htmlFileName, htmlFolderpath } of urlSet) {
        urlBar.update(counter, { message: url_id })
        await scrapeUrl(nightmare, url, htmlFileName, htmlFolderpath, timeoutParams)
        counter += 1
    }
    urlBar.stop()
}

async function scrapeUrl(nightmare, url, outputFileName, outputFolderPath, timeoutParams) {
    let remaining = 10
    let html = ""
    while (remaining > 0 && html == "") {
        try {
            html = await fetchUrl(nightmare, url, timeoutParams)
        } catch (error) {
            remaining -= 1
            if (remaining > 0) {
                await sleep(getRandomInt(urlTimeoutMinMs, urlTimeoutMaxMs))
            }
        }
    }
    if (html == "") {
        throw `Failed to retrieve HTML after 10 attempts. URL: ${url}`
    }
    const filePath = joinPath(outputFolderPath, outputFileName)
    writeFileSync(filePath, html)
    return filePath
}

async function fetchUrl(nightmare, url, { urlTimeoutMinMs, urlTimeoutMaxMs }) {
    try {
        await nightmare.on("did-get-response-details", () => (startTime = Date.now()))
        await nightmare.goto(url)
        await nightmare.waitUntilNetworkIdle(500)
        const html = await nightmare.evaluate(() => document.body.innerHTML)
        await nightmare.wait(getRandomInt(urlTimeoutMinMs, urlTimeoutMaxMs))
        return html
    } catch (e) {
        console.log(e)
    }
}

module.exports = {
    executeBatchJob: executeBatchJob,
    scrapeUrls: scrapeUrls,
    scrapeUrl: scrapeUrl,
}
