# attestabot

Scraper for `zefix.ch`.

## Getting started

~~~
git clone git@github.com:zhu-partner/attestabot.git
cd attestabot
~~~
The scraper works entirely in python. Required packages are listed in `requirements.txt`. A working conda environment with python 3.10 can be created through the `environment.yml` file through:
~~~
conda env create -f environment.yml
~~~
Older python versions might work as well but have not been tested.

## Usage

Run the following command to fetch all data from `zefix.ch`:
~~~
scrapy crawl zefix
~~~
This should take less than a minute, depending on your connection. It will create the `firms` folder, containing a pickle (`.pkl`) file for each canton. Each pickle file contains a pandas DataFrame with 17 columns and one row per firm. The columns are: name, ehraid, uid, uidFormatted, chid, chidFormatted, legalSeatId, legalSeat, cantonId, canton, registerOfficeId, legalFormId, status, rabId, shabDate, deleteDate, cantonalExcerptWeb.

Once the pickle files have been downloaded successfully, run:
~~~
python urlFinder.py
~~~
This will load all pickle files from the previous step, merge them into a single DataFrame and attempt to find web pages for each firm based on a set of rules described below. This step can take several hours, as there are hundreds of thousands of firms and up to four urls are checked for each one. To check if a urls exist, a simple request is sent with a timeout of 3 seconds. A url is considered valid if the request is successful (`status_code==200`). The existing urls found for each firm are saved in a new column of the DataFrame. The resulting DataFrame is saved in a pickle file (`all.pkl`) and in an Excel file (`all.xlsx`) in the `firms` folder.

## Url construction

At the moment, the construction of possible urls is based solely on the firm's name and comprises following steps:
 - remove the trailing 'in Liquidation' (in any language), if present;
 - remove the company form (ag, gmbh, ...), if present;
 - if the name contains spaces, either remove them or replace them with hyphens --> two possibilities;
 - complete each possibility with a `.ch` or `.com` top level domain.
These rules create up to four possible urls per firm.

## Possible improvements

 - Some firms might be excluded a priori by filtering by available info (e.g. ehraid, uid, ...), as some categories might not require a ISO certificate at all, for example.
 - Expanding the list of possible top level domains.
 - It should be possible to obtain a list of all `*.ch` domain names and then search within it to find firm names. See https://rimann.org/blog/listing-all-ch-domains.
 - Use a search engine like Google to search for web pages.
