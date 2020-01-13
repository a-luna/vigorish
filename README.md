# vigorish

Vigorish scrapes and stores MLB data sets, including detailed boxscores, pitchfx measurements and player biographical info. Data is scraped from brooksbaseball.com and baseball-reference.com.

Use of vigorish must abide by the terms stated in the license. Also, in order to abide by the guidelines quoted below (from [baseball-reference.com](https://www.sports-reference.com/data_use.html)), __a delay of 2.5-3.0 seconds (delay time is determined randomly) always occurs after a URL is scraped:__

> Please do not attempt to aggressively spider data from our web sites, as spidering violates the terms and conditions that govern your use of our web sites: [Site Terms of Use](https://www.sports-reference.com/termsofuse.html)

> If we notice excessive activity from a particular IP address we will be forced to take appropriate measures, which will include, but not be limited to, blocking that IP address. We thank you in advance for respecting our terms of use.

This package is not ready for public consumption at this point, several features are incomplete and all mothods/modules are subject to change. However, the CLI help documentation gives a general idea of the desired v1.0 functionality and can be accessed as shown below:

```shell
$ vig --help
Usage: vig [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  scrape  Scrape MLB data from websites.
  setup   Populate database with initial Player, Team and Season data.
  status  Report progress of scraped mlb data sets.
```

```shell
$ vig setup --help
Usage: vig setup [OPTIONS]

  Populate database with initial Player, Team and Season data.

  WARNING! Before the setup process begins, all existing data will be
  deleted. This cannot be undone.

Options:
  --yes   Confirm the action without prompting.
  --help  Show this message and exit.
```

```shell
$ vig scrape --help
Usage: vig scrape [OPTIONS]

  Scrape MLB data from websites.

Options:
  --data-set DATA-SET  Data set to scrape, must be a value from the following
                       list:
                       bbref_games_for_date, bbref_boxscore,
                       bbref_player, brooks_games_for_date, brooks_pitch_logs,
                       brooks_pitchfx
  --start DATE-STRING  Date to start scraping data, string can be in any
                       format that is recognized by dateutil.parser.
  --end DATE-STRING    Date to stop scraping data, string can be in any format
                       that is recognized by dateutil.parser.
  --help               Show this message and exit.
```

```shell
$ vig status --help
Usage: vig status [OPTIONS]

  Report progress of scraped mlb data sets.

Options:
  --year YEAR-NUMBER  Year of the MLB Season to report progress of scraped
                      data sets.
  --help              Show this message and exit.
```
