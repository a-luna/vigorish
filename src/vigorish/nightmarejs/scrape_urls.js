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
        const currentBatchSize = batchList[batchCounter].length
        pBarDisplay = getPbarDisplayStr(`${currentBatchSize} URLs this batch`)
        batchBar.update(batchCounter, { message: pBarDisplay })
        urlCounter = await scrapeUrlBatch(nightmare, urlBatch, timeoutParams, urlCounter, urlBar)
        batchCounter += 1
        if (batchCounter < totalBatches) {
            const nextBatchSize = batchList[batchCounter].length
            const nextBatchStr = `${nextBatchSize} URLs next batch`
            pBarDisplay = getPbarDisplayStr(nextBatchStr, (padEllipses = false))
            batchBar.update(batchCounter, { message: pBarDisplay })
            if (timeoutParams.batchTimeoutRequired) {
                await batchTimeout(timeoutParams, timeoutBar, urlCounter, nextBatchSize)
            }
        }
    }, Promise.resolve())
    pBarDisplay = getPbarDisplayStr("Scraped all batches")
    batchBar.update(batchCounter, { message: pBarDisplay })
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
            message: getPbarDisplayStr(`${batchList[0].length} URLs this batch`),
            unit: "Batches",
        })
        const urlBar = multibar.create(totalUrls, 0, {
            message: getPbarDisplayStr("In Progress"),
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

    async function scrapeUrlBatch(nightmare, urlSet, timeoutParams, urlCounter, progressBar) {
        await urlSet.reduce(async (promise, urlDetails) => {
            await promise
            let { url, fileName, identifier, scrapedHtmlFolderpath } = urlDetails
            const pbarDisplay = getPbarDisplayStr(identifier, (padEllipses = false))
            progressBar.update(urlCounter, { message: pbarDisplay })
            await scrapeUrl(nightmare, url, fileName, scrapedHtmlFolderpath, timeoutParams)
            urlCounter += 1
        }, Promise.resolve())
        progressBar.update(urlCounter)
        return urlCounter
    }

    async function batchTimeout(timeoutParams, progressBar, urlCounter, nextBatchSize) {
        let { batchTimeoutMinMs, batchTimeoutMaxMs } = timeoutParams
        let [timeoutList, totalSeconds, minRemaining, secRemaining] = getRandomTimeout(
            batchTimeoutMinMs,
            batchTimeoutMaxMs
        )
        let pBarDisplay = getTimeoutDisplayString(minRemaining, secRemaining)
        progressBar.setTotal(totalSeconds)
        progressBar.update(0, { message: pBarDisplay, unit: "Seconds" })
        let counter = 1
        await timeoutList.reduce(async (promise, timeout) => {
            await promise
            if (secRemaining == 0) {
                minRemaining -= 1
                secRemaining = 59
            } else {
                secRemaining -= 1
            }
            pBarDisplay = getTimeoutDisplayString(minRemaining, secRemaining)
            progressBar.update(counter, { message: pBarDisplay })
            await sleep(timeout)
            counter += 1
        }, Promise.resolve())
        pBarDisplay = getNextUrlRangeStr(urlCounter, nextBatchSize)
        progressBar.setTotal(0)
        progressBar.update(0, { message: pbarDisplay, unit: "" })
        return

        function getRandomTimeout(timeoutMinMs, timeoutMaxMs) {
            const timeoutMs = getRandomInt(timeoutMinMs, timeoutMaxMs)
            const minRemaining = Math.floor(timeoutMs / 60000)
            const secRemaining = Math.floor((timeoutMs - minRemaining * 60000) / 1000)
            const totalSeconds = Math.floor(timeoutMs / 1000)
            const timeoutList = Array.from({ length: totalSeconds }).map(x => 1000)
            return [timeoutList, totalSeconds, minRemaining, secRemaining]
        }

        function getTimeoutDisplayString(minRemaining, secRemaining) {
            const minPadded = minRemaining.toString().padStart(2, "0")
            const secPadded = secRemaining.toString().padStart(2, "0")
            return getPbarDisplayStr(`${minPadded}:${secPadded} until next batch`)
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
