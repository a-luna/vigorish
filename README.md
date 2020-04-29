## vigorish

`vigorish` is a hybrid Python/Node.js application that scrapes MLB data from mlb.com, brooksbaseball.net and baseball-reference.com.

My goal is to capture as much data as possible &mdash; ranging from PitchFX measurements at the most granular level to play-by-play data (play descriptions, substitutions, manager challenges, etc) and individual player pitch/bat stats at the highest level.

### Requirements

-   Python 3.6+
-   Node.js 4+ (Tested with Node.js 11-13)
-   Xvfb
-   AWS account (optional, used to store scraped data in S3)

Since `vigorish` uses the `dataclass` decorator, Python 3.6+ is required (`dataclass` was introduced in Python 3.7, however a [backport](https://pypi.org/p/dataclasses/) exists for 3.6).

All web scraping is performed by [Nightmare](https://github.com/segmentio/nightmare), which is a browser automation tool similar to Selenium/PhantomJS. Nightmare is a nodejs module based on [Electron](http://electron.atom.io/)/[Chromium](https://www.chromium.org/Home), requiring Node.js 4+ (I have only tested with versions 11-13).

`vigorish` is designed for a headless environment. In order to run Nightmare/Electron in this environment, you must install xvfb and several dependencies along with nodejs/npm.

### _**WARNING**_

Use of vigorish must abide by the terms stated in the license. Also, in order to abide by the guidelines quoted below (from [baseball-reference.com](https://www.sports-reference.com/data_use.html)), **a delay of at least two seconds MUST always occur after a URL is scraped:**

> Please do not attempt to aggressively spider data from our web sites, as spidering violates the terms and conditions that govern your use of our web sites: [Site Terms of Use](https://www.sports-reference.com/termsofuse.html)

> If we notice excessive activity from a particular IP address we will be forced to take appropriate measures, which will include, but not be limited to, blocking that IP address. We thank you in advance for respecting our terms of use.

You may notice that the URL delay time is a configurable setting. This setting must be enabled and the min delay time must be greater than two seconds. _**If the setting is disabled or if you enter a min value of two seconds or shorter, you will be unable to start any scrape job.**_

### Install Prerequisites

First, install a recent, stable version of Node.js (i.e., v10-13), along with npm. I'll provide instructions for Ubuntu, but they should be easily adaptable to any Linux-distro.

#### Node.js

While you can install Node.js using the default package repository on your system, the versions that are available tend to be outdated. On Ubuntu, you can add a PPA (personal package archive) maintained by NodeSource which always contains the latest versions of Node.js.

The command below will download the installation script for the NodeSource PPA containing the latest v12.x version and run the script after it has been downloaded (if you would like to download a different version, simply replace `12.x` with `8.x`, `10.x`, etc.). You must have sudo privileges to execute the installation script:

```console
$ curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
```

The script will add the NodesSource PPA and update your local package cache. After the package listing is refreshed, install the `nodejs` package using the command below:

```console
$ sudo apt install nodejs
```

The version of node you specified and npm should both be installed. Verify that the versions installed are correct:

```console
$ node --version
v12.16.2
```

```console
$ npm --version
6.14.2
```

Finally, install the `build-essential` package since some of the packages needed for `vigorish` require compiling from source:

```console
$ sudo apt install build-essential
```

#### Xvfb

An X server must be installed in order to use Electron in headless mode. [The most popular X server for UNIX-like systems is Xvfb](https://www.x.org/releases/X11R7.6/doc/man/man1/Xvfb.1.xhtml). `Xvfb` enables a system without display hardware to run graphical applications.

Install `Xvfb` and its dependencies using the command below:

```console
sudo apt install -y xvfb x11-xkb-utils xfonts-100dpi xfonts-75dpi xfonts-scalable xfonts-cyrillic x11-apps clang libdbus-1-dev libgtk2.0-dev libnotify-dev libgnome-keyring-dev libgconf2-dev libasound2-dev libcap-dev libcups2-dev libxtst-dev libxss1 libnss3-dev gcc-multilib g++-multilib
```

#### pipx

The recommended way to install `vigorish` is [with pipx](https://github.com/pipxproject/pipx). On Ubuntu with Python 3.7 aliased to `python3.7`, you can install `pipx` with the commands below:

```console
$ python3.7 -m pip install --user pipx
$ python3.7 -m pipx ensurepath
```

After executing the two commands above, logout and log back in. To setup tab completions, run this command and follow the instructions for your shell:

```console
$ pipx completions
```

Logout and login again in order for the changes to take effect.

Why should you use `pipx` to install `vigorish`? If you are brand new to Python, you may not be aware of all the little annoyances that come along with installing packages from pip. If you use your system Python installation, very quickly you will encounter unresolvable dependency issues. Eventually, you will fall into the habit of creating a new virtual environment whenever you install an application to isolate all of the required packages and their specific versions from your system install.

While creating unique virtual environments works, it can be annoying and confusing to manage applications that are intended to be installed globally in this manner. `pipx` is the solution to this problem. [From the README](https://github.com/pipxproject/pipx/blob/master/README.md):

> You can globally install an application by running
>
> ```console
> pipx install PACKAGE
> ```
>
> This automatically creates a virtual environment, installs the package, and adds the package's associated applications (entry points) to a location on your `PATH`. For example, `pipx install pycowsay` makes the `pycowsay` command available globally, but sandboxes the pycowsay package in its own virtual environment.

It is similarly easy to update and remove Python applications with `pipx`. Check out the README for install instructions for Mac and usage examples.

### Install

If you followed the instructions to install `pipx`, you can install `vigorish` with the command below:

```console
$ pipx install vigorish
```

If you did not install `pipx`, follow the steps below to install `vigorish`:

1. Create a new directory for `vigorish` and navigate to it:

    ```console
    $ mkdir vigorish && cd vigorish
    ```

2. Create a new Python 3.6+ virtual environment:

    ```console
    $ python3.7 -m venv venv --prompt vig
    ```

3. Activate the virtual environment:

    ```console
    $ source venv/bin/activate
    ```

4. Update `pip`, `setuptools` and `wheel` packages to the latest versions:

    ```console
    (vig) $ pip install --upgrade pip setuptools wheel
    ```

5. Finally, install `vigorish` using `pip`:

    ```console
    (vig) $ pip install vigorish
    ```

#### Verify Install

If you installed `vigorish` within a virtual environment, activate the environment and run the `vig` command. If you installed `vigorish` with `pipx`, simply run `vig`. If the install succeeded, the help documentation for the CLI should be displayed:

```console
(vig) $ vig
Usage: vig [OPTIONS] COMMAND [ARGS]...

  Entry point for the CLI application.

Options:
  --help  Show this message and exit.

Commands:
  scrape  Scrape MLB data from websites.
  setup   Populate database with initial player, team and season data.
  status  Report progress of scraped data, by date or MLB season.
  ui      Menu-driven TUI powered by Bullet.
```

Next, run `vig ui` and verify that the UI displays:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/main_menu_setup_db.jpg" alt="Main Menu" width="500px">

If both the CLI help screen and the UI are displayed, the installation was successful! However, you can not begin scraping data just yet. You need to perform a few additional configuration steps first.

### Config/Setup

There are quite a few settings that determine how `vigorish` scrapes data, where scraped HTML/JSON is stored, etc. The majority of these settings are stored in a JSON file, and the rest are stored in a file named `.env`

When you launch the UI or run any CLI command, `vigorish` looks for a folder named `~/.vig` and creates the directory if it does not exist. Next, it looks for `.env` in the `~/.vig` folder. If this file does not exist, a default `.env` is created with the following values:

```ini
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=
CONFIG_FILE={YOUR_HOME_FOLDER}/.vig/vig.config.json
DATABASE_URL=sqlite:///{YOUR_HOME_FOLDER}/.vig/vig.db
```

> ⚠️ `{YOUR_HOME_FOLDER}` _is not a real value that you should use or will see in your_ `.env` _file. It is a placeholder for the path of the current user's home folder._

These values are **Environment Variables**. They are loaded into the environment using the `python-dotenv` package, and their values can be accessed from code with the `os.getenv` function.

Next, `vigorish` uses the value of `CONFIG_FILE` to read the contents of `vig.config.json`. Just like `.env`, if this file does not exist a default config file is created. Let's go back and discuss how to manage the values in `.env` before we discuss the settings available in `vig.config.json`.

#### Environment Variables

Run `vig ui` and use the arrow keys to select **Environment Variables** from the menu options:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/main_menu_env_vars.jpg" alt="Main Menu" width="500px">

Press **Enter** and you should see the menu below:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/env_var_menu.jpg" alt="Environment Variables Menu" width="500px">

The first three settings in this list must be set if you wish to store scraped HTML/JSON documents in an S3 bucket. If you do not want to use this feature, you have the option to store scraped HTML in a local folder or to not store HTML files at all. JSON files must be stored in a local folder if you do not wish to store them in a S3 bucket.

If you do wish to store files in S3, you can find more details in the **HTML/JSON Storage** section.

The remaining environment variables are rather self-explanatory. If you wish to change the location/filename of `vig.config.json`, select **CONFIG_FILE**. If you wish to change the location/filename of the SQLite database, select **DATABASE_URL** .

Selecting any item in this list will show the current value of the environment variable. For example, if you select **DATABASE_URL** you will see the menu below:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/current_env_var_setting.jpg" alt="Current Env Var Setting" width="500px">

You can return to the list of environment variables by selecting **NO**. If you select **YES**, you will be prompted to enter a new value.

Next, you will be asked to confirm the new value is correct before it is applied. If it is not correct, selecting **NO** will prompt you to re-enter the new value. Selecting **CANCEL** will keep the current value and return to the previous menu:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/change_env_var_setting.jpg" alt="Env Var Change Value" width="500px">

> ⚠️ _If you change the value of either_ **CONFIG_FILE** _or_ **DATABASE_URL**, `vigorish` _will automatically exit to ensure that the new settings/SQLite file are used, rather than the previous, discarded settings/database._

Please enter your AWS credentials if you plan on using the S3 storage feature. If you would like to change the location/name of the `vig.config.json` file and/or SQLite database file, please do so now. When complete, return to the Main Menu.

#### Config File Settings

Next, select **Config File Settings** from the Main Menu:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/main_menu_config_settings.jpg" alt="Main Menu" width="500px">

Press **Enter** and you should see the menu below:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/config_file_settings_1.jpg" alt="Config File Settings (Scroll Menu)" width="500px">

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/config_file_settings_2.jpg" alt="Config File Settings (Scroll Menu)" width="500px">

The **Config File Settings** menu allows the user to view or modify the current value of any setting defined in `vig.config.json`. Please note the up/down arrows highlighted in the two screenshots above. Whenever a menu contains more than eight items, it will be rendered as a scrolling menu. The arrows indicate that there are additional menu items that can be accessed by navigating through the list.

To make navigating the UI easier, options to return to the previous menu are added to the beginning **AND** end of the scroll menu (as opposed to normal menus which only have the return option at the end of the list).

We will cover the purpose/effect of each config setting shortly. Selecting a config setting (press `ENTER`) displays the setting name, data type, description and current setting as shown below:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/change_config_setting.jpg" alt="Change Config Setting" width="500px">

Within this menu, you are asked if you would like to change the current setting. In the screenshot above, the `Json Storage` setting has a current value of `ALL_DATA_SETS..: BOTH`. Most settings can be configured per data set **OR** or the same value can be used for all data sets.

If you choose to change the current setting, you are asked if you would like to use the same setting for all data sets:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/same_setting_all_data_sets.jpg" alt="Same Setting For All Data Sets" width="500px">

Selecting **YES** will prompt you to select a new value:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/change_setting_all_data_sets.jpg" alt="Select Value All Data Sets" width="500px">

In this case, the data type of the setting is an `Enum`. For this data type, you can select a new value from a list of all possible values (all other data types (`str`, `numeric`, `Path`) will be typed in directly).

Going back one step, if you selected **NO** when asked if you would like to use the same setting for all data sets, you will be asked to enter a new value for the setting five times, once for each data set that can be scraped using vigorish.

The prompt for a single data type is shown below:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/change_setting_bbref_games_for_date.jpg" alt="Select Value Single Data Set" width="500px">

After entering values for all data types, you can verify that the new value was applied by selecting the setting you modified from the **Config File Settings** menu. If you chose to set separate values for each data set, you will see something similar to the screenshot below:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/updated_config_setting.jpg" alt="Updated Config Setting" width="500px">

With a fresh install, the following default settings are used:

##### **Same setting for all data sets**

-   **`STATUS_REPORT`**: `SEASON_SUMMARY`
-   **`S3_BUCKET`**: `your-bucket`
    -   This is a placeholder value, you must set this to the name of a S3 bucket that your AWS account has permission to create/read/delete objects.
-   **`SCRAPE_CONDITION`**: `ONLY_MISSING_DATA`
-   **`URL_SCRAPE_DELAY`**: `Delay is random (3-6 seconds)`
-   **`BATCH_JOB_SETTINGS`**: `Batch size is random (50-80 URLs)`
-   **`HTML_STORAGE`**: `NONE`
    -   By default, scraped HTML is **NOT STORED** since this will consume storage space very quickly. However, I frequently make changes to the parser functions and it is extremely useful and 100x quicker to store the HTML and run the updated parsers against local copies of scraped HTML. The owners of the scraped sites surely appreciate it as well.
-   **`HTML_LOCAL_FOLDER_PATH`**: `html_storage/{year}/{data_set}/`
    -   All settings that are `data_type` = `Path` recognize the two placeholder strings `{year}` and `{data_set}`. The path that is generated substitutes values in place of the placeholders using the date that the game occurred on and the data set being scraped.
-   **`HTML_S3_FOLDER_PATH`**: `{year}/{data_set}/html/`
-   **`JSON_STORAGE`**: `LOCAL_FOLDER`
    -   Be default, JSON files containing parsed data are only stored in the local file system. If you would like to store JSON files in S3 or both S3 and the local file system, you must enter your AWS credentials in the **Environment Variables** menu and then update this setting.
-   **`JSON_LOCAL_FOLDER_PATH`:** `json_storage/{year}/{data_set}/`
-   **`JSON_S3_FOLDER_PATH`**: `{year}/{data_set}`
    <br>

##### **Different setting for each data set**

-   **`BATCH_SCRAPE_DELAY`**
    -   `BBREF_GAMES_FOR_DATE`: `Delay is random (5-10 minutes)`
    -   `BROOKS_GAMES_FOR_DATE`: `Delay is random (30-45 minutes)`
    -   `BBREF_BOXSCORES`: `Delay is random (5-10 minutes)`
    -   `BROOKS_PITCH_LOGS`: `Delay is random (30-45 minutes)`
    -   `BROOKS_PITCHFX`: `Delay is random (30-45 minutes)`

I arrived at these settings through much trial and error. I know that it takes much longer to scrape data from brooksbaseball.net with a 30-45 minute delay every 50-80 URLs, but I have never been banned with this configuration. Without the batch delay, I was always blocked within an hour or two.

Please make any changes you wish at this point. When you have everything setup to your liking, return to the Main Menu.

#### Install Node.js Packages (Nightmare)

Next, select **NPM Packages** from the Main Menu:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/main_menu_npm_packages.jpg" alt="Main Menu" width="500px">

Press **Enter** and you will be prompted to install all node dependencies:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/npm_install_prompt.jpg" alt="NPM Install Prompt" width="500px">

Select **YES** to begin the installation. You will see the output from npm, and when complete simply press any key to continue:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/npm_install_complete.jpg" alt="NPM Install Complete" width="500px">

After successful installation, the **NPM Packages** menu will instead ask if you would like to update the installed npm packages:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/npm_update_prompt.jpg" alt="NPM Update Prompt" width="500px">

Select **YES** to check for updates. If any are available, they will be updated automatically and the output from npm will be displayed as shown below:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/npm_update_complete.jpg" alt="NPM Update Complete" width="500px">

When the update process is complete, return to the main menu by pressing any key.

#### Initialize SQLite Database

Finally, select **Setup Database** from the Main Menu:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/main_menu_setup_db.jpg" alt="Main Menu" width="500px">

Press **Enter** and you will be prompted to initialize the SQLite database:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/setup_db_prompt.jpg" alt="Setup DB Prompt" width="500px">

Select **YES** to begin the initialization process. After all tables have been populated with initial data, press any key to return to the Main Menu.

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/setup_db_complete.jpg" alt="Setup DB Complete" width="500px">

### Usage

Congratulations! If you made it this far, you have successfully installed and configured `vigorish`! After the database has been initialized, the Main Menu will contain a slightly different set of options:

<img src="https://alunapublic.s3-us-west-1.amazonaws.com/vigorish/main_menu_db_setup.jpg" alt="Main Menu After DB Initialized" width="500px">

#### Create New Job

#### View All Jobs

#### Job Execution

#### Reset Database
