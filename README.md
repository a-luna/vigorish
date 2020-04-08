## vigorish

Vigorish is a hybrid Python/Nodejs application that scrapes MLB data from mlb.com, brooksbaseball.com and baseball-reference.com. My primary goal is to capture as much pitching data as possible including the in-game context when the pitch was thrown. Scraped play-by-play data from boxscores is combined and reconciled against raw PitchFX measurements to provide this context.

Use of vigorish must abide by the terms stated in the license. Also, in order to abide by the guidelines quoted below (from [baseball-reference.com](https://www.sports-reference.com/data_use.html)), **a delay of at least two seconds MUST always occur after a URL is scraped:**

> Please do not attempt to aggressively spider data from our web sites, as spidering violates the terms and conditions that govern your use of our web sites: [Site Terms of Use](https://www.sports-reference.com/termsofuse.html)

> If we notice excessive activity from a particular IP address we will be forced to take appropriate measures, which will include, but not be limited to, blocking that IP address. We thank you in advance for respecting our terms of use.

You may notice that the URL delay time is a configurable setting, allowing you to specify a min and max delay time that generates a random delay between the two values. This setting must be enabled and the min delay time must be greater than two seconds. _**If the setting is disabled or if you enter a min value of two seconds or shorter, you will be unable to start any scrape job.**_
