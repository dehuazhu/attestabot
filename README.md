# attestabot

The attesta-bot is made of 3 components.
1) zefix-scraper: downloads all companies registered on zefix to local files.
2) Moneyhouse-scraper: searches for each company's CH-ID number from zefix on moneyhouse.ch and saves the company's contact information (homepage, address, phone number and email) to local files.
3) iso-finder: crawls through the web pages found by the Moneyhouse-scraper and finds text, images or pdf files that contain information on ISO certificates. Saves the sub-urls pointing to the information in local files information in local files.

An alternative version of the Moneyhouse-scraper, called website-finder, was developed but not used in the final version. This bot constructs possible web pages starting from the name of a company. It was discarded because it leads to many false positives, and Moneyhouse offers a better solution.

## Getting started

~~~
git clone git@github.com:zhu-partner/attestabot.git
cd attestabot
~~~
The bot works entirely in python. Required packages are listed in `requirements.txt`. A working conda environment with python 3.10 can be created through the `environment.yml` file through:
~~~
conda env create -f environment.yml
~~~
Older python versions might work as well but have not been tested.

## zefix-scraper

Run the following command to fetch all data from `zefix.ch`:
~~~
scrapy crawl zefix
~~~
This should take about a minute, depending on your connection. It will create the `firms_zefix` folder, containing a parquet (`.parquet`) file for each canton. Each parquet file contains a pandas DataFrame with 17 columns and one row per firm. The columns are: name, ehraid, uid, uidFormatted, chid, chidFormatted, legalSeatId, legalSeat, cantonId, canton, registerOfficeId, legalFormId, status, rabId, shabDate, deleteDate, cantonalExcerptWeb.

## Moneyhouse-scraper

This step requires valid login information for moneyhouse.ch, as otherwise the contact information of companies is not visible. Put the login information in a text file called 'moneyhouse.login': write the user name on the first line and the password on the second line.

Once the parquet files have been downloaded successfully from zefix, run:
~~~
python moneyhouse_scraper.py
~~~
The bot will search for each CH-ID number and add columns for homepage, address, phone number and email to the parquet files. An additional columns indicates the date on which the bot is run. The files with the new columns are saved in a new folder called `firms_moneyhouse`.

## iso-finder

When the output parquet-files produced by the Moneyhouse-scraper are available, run:
~~~
python iso_finder.py
~~~
This will take each valid url and follow all links on that page (always staying on the same domain) in search for information about ISO certificates.
The iso-finder trigger is based on the following list of keywords: "iso", "zertifikat", "zertifizierung", "certifica". It follows the following criteria.
 - The webpage contains a link to a file of type `pdf`, `png`, `jpg` or `jpeg` which has a keyword (case insensitive) in its name.
 - The text (or source code) of the webpage contains one or more keywords and not words from a blacklist, to avoid false positives. The blacklist consists of words which contain "iso" but have nothing to do with certificates,  such as isolation. Additionally, the letters "ISO" must be capitalized and form an isolated word, as in " ISO", "ISO " or "ISO.".
 - The webpage displays an image which has a keyword (case insensitive) in its name.
The raw results are saved to the file `iso_finder_raw.jl` and then filled into the dataframes. The dataframes are extended by four new columns: `has_iso_info` contains TRUE or FALSE and indicates if at least one suburl of the homepage triggered the search. The other three columns contain the first three suburls, if they are found, that triggered the search. The resulting DataFrames are saved in `parquet` and `xlsx` format in the new folder `firms_iso_finder`.

## website-finder

This module was deprecated in favour of the Moneyhouse-scraper. It was supposed to be run after the zefix-scraper. Once the parquet files have been downloaded successfully from zefix, run:
~~~
python website_finder.py
~~~
This will load all parquet files downloaded by the zefix-scraper, and attempt to find web pages for each company based on a set of rules described below. This step can take several hours, as there are hundreds of thousands of firms and up to 18 urls are checked for each one. To check if a url exists, a simple request is sent with a timeout of 15 seconds. A url is considered valid if the request is successful (`status_code==200`). All valid urls are first saved to the file `website_finder_raw.jl` and then filled into the dataframes. Each checked url is placed on a separate row in the table, and an additional column indicates if the url is valid or not. The resulting DataFrames are saved in `parquet` and `xlsx` format in the new folder `firms_website_finder`.

### Url construction

At the moment, the construction of possible urls is based solely on the firm's name and comprises following steps:
 - remove the trailing 'in Liquidation' (in any language), if present;
 - remove the company form (ag, gmbh, ...), if present;
 - if the name consists of more than one word, consider also partial names with only the first one, two, three or four words;
 - for each list of words (full company name, and first few words) join the words with hypens or simply chain them together;
 - complete each possibility with a `.ch` or `.com` top level domain.
These rules create up to 18 possible urls per firm.

## Possible improvements

### website-finder
 - Some firms might be excluded a priori by filtering by available info (e.g. ehraid, uid, ...), as some categories might not require a ISO certificate at all, for example.
 - Expanding the list of possible top level domains.
 - It should be possible to obtain a list of all `*.ch` domain names and then search within it to find firm names. See https://rimann.org/blog/listing-all-ch-domains.
 - Use a search engine like Google to search for web pages.
