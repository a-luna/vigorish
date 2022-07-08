const puppeteer = require("puppeteer");
const parseArgs = require("minimist");
const { executeBatchJob, scrapeUrls } = require("./scrape_urls");
require("events").EventEmitter.defaultMaxListeners = 0;

const args = parseArgs(process.argv.slice(2));
main(args).catch(console.error);

async function main(args) {
    const browser = await puppeteer.launch();
    jobParams = {
        urlSetFilepath: args.urlSetFilepath,
        batchParams: getBatchParams(args),
        timeoutParams: getTimeoutParams(args),
    };

    const [success, err] = await run(browser, jobParams);
    if (!success) {
        throw err;
    }
    process.exit(0);
}

function getBatchParams(args) {
    let batchParams = { batchScrapingEnabled: false };
    if ("batchScrapingEnabled" in args) {
        batchParams.batchScrapingEnabled = true;
        if ("minBatchSize" in args && "maxBatchSize" in args) {
            batchParams.batchSizeIsRandom = true;
            batchParams.minBatchSize = args.minBatchSize;
            batchParams.maxBatchSize = args.maxBatchSize;
        } else if ("uniformBatchSize" in args) {
            batchParams.batchSizeIsRandom = false;
            batchParams.uniformBatchSize = args.uniformBatchSize;
        }
    }
    return batchParams;
}

function getTimeoutParams(args) {
    let timeoutParams = {
        urlTimeoutRequired: false,
        batchTimeoutRequired: false,
    };
    if ("urlTimeoutRequired" in args) {
        timeoutParams.urlTimeoutRequired = true;
        if ("urlTimeoutMinMs" in args && "urlTimeoutMaxMs" in args) {
            timeoutParams.urlTimeoutMinMs = args.urlTimeoutMinMs;
            timeoutParams.urlTimeoutMaxMs = args.urlTimeoutMaxMs;
        } else if ("urlTimeoutUniformMs" in args) {
            timeoutParams.urlTimeoutMinMs = args.urlTimeoutUniformMs;
            timeoutParams.urlTimeoutMaxMs = args.urlTimeoutUniformMs;
        }
    }
    if ("batchTimeoutRequired" in args) {
        timeoutParams.batchTimeoutRequired = true;
        if ("batchTimeoutMinMs" in args && "batchTimeoutMaxMs" in args) {
            timeoutParams.batchTimeoutMinMs = args.batchTimeoutMinMs;
            timeoutParams.batchTimeoutMaxMs = args.batchTimeoutMaxMs;
        } else if ("batchTimeoutUniformMs" in args) {
            timeoutParams.batchTimeoutMinMs = args.batchTimeoutUniformMs;
            timeoutParams.batchTimeoutMaxMs = args.batchTimeoutUniformMs;
        }
    }
    return timeoutParams;
}

async function run(browser, jobParams) {
    try {
        await execute_job(browser, jobParams);
        return [true, null];
    } catch (err) {
        return [false, err];
    } finally {
        await browser.close();
    }
}

async function execute_job(
    browser,
    { urlSetFilepath, timeoutParams, batchParams }
) {
    const page = await newPageWithRequestInterceptor(browser);
    if (batchParams.batchScrapingEnabled) {
        await executeBatchJob(page, urlSetFilepath, batchParams, timeoutParams);
    } else {
        await scrapeUrls(page, urlSetFilepath, timeoutParams);
    }
}

async function newPageWithRequestInterceptor(browser) {
    let page = await browser.newPage();
    page.setDefaultNavigationTimeout(0);
    await page.setRequestInterception(true); 
    page.on("request", (request) => {
        const blockedDomains = [
            "ad-delivery.net",
            "/*.doubleclick.net",
            "/*.googleadservices.com",
            "/*.amazon-adsystem.com",
            "/*.adsrvr.org",
            "/*.googlesyndication.com",
            "/*.pubgw.yahoo.com",
            "/*.adnxs.com",
            "/*.deployads.com",
            "/*.ssp.yahoo.com",
            "/*.mantisadnetwork.com",
            "/*.teads.tv",
            "/*.amazon-adsystem.com",
            "/*.ads.linkedin.com",
            "/*.adsymptotic.com",
        ];
        if (blockedDomains.find((pattern) => request.url().match(pattern))) {
            request.abort();
        } else if (
            request.resourceType() === "image" ||
            request.resourceType() === "media"
        ) {
            request.abort();
        } else request.continue();
    });
    return page;
}
