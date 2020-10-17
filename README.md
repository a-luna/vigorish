[![PyPI version](https://badge.fury.io/py/vigorish.svg)](https://badge.fury.io/py/vigorish) ![PyPI - Downloads](https://img.shields.io/pypi/dm/vigorish?color=%234DC71F) ![PyPI - License](https://img.shields.io/pypi/l/vigorish?color=%25234DC71F) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/vigorish) [![Maintainability](https://api.codeclimate.com/v1/badges/4a1753c77add039c3850/maintainability)](https://codeclimate.com/github/a-luna/vigorish/maintainability) [![codecov](https://codecov.io/gh/a-luna/vigorish/branch/master/graph/badge.svg)](https://codecov.io/gh/a-luna/vigorish)

## vigorish

`vigorish` is a hybrid Python/Node.js application that scrapes MLB data from mlb.com, brooksbaseball.net and baseball-reference.com.

My goal is to capture as much data as possible &mdash; ranging from PitchFX measurements at the most granular level to play-by-play data (play descriptions, substitutions, manager challenges, etc) and individual player pitch/bat stats at the highest level.

### Requirements

-   Python 3.6+
-   Node.js 10+ (Tested with Node.js 11-13)
-   Xvfb
-   AWS account (optional but recommended, used to store scraped data in S3)

### Project Documentation

For a step-by-step install guide and instructions for configuring/using `vigorish`, please visit the link below:

[Vigorish: Hybrid Python/Node.Js Web Scraper](https://aaronluna.dev/projects/vigorish/)
