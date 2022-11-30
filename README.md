# attestabot

The attesta-bot is made of 4 components
1) zefix-scraper: scraper finding all companies registered on zefix.
2) website-finder: for the zefix companies, find a website belonging to the company.
3) iso-finder: for a given company website, find a sub-url where iso-info is contained.
4) iso-analyzer: for a given company iso-suburl, find the meta data regarding the iso-info.

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
This should take about a minute, depending on your connection. It will create the `firms_zefix` folder, containing a pickle (`.pkl`) file for each canton. Each pickle file contains a pandas DataFrame with 17 columns and one row per firm. The columns are: name, ehraid, uid, uidFormatted, chid, chidFormatted, legalSeatId, legalSeat, cantonId, canton, registerOfficeId, legalFormId, status, rabId, shabDate, deleteDate, cantonalExcerptWeb.

## website-finder

Once the pickle files have been downloaded successfully from zefix, run:
~~~
python website_finder.py
~~~
This will load all pickle files downloaded by the zefix-scraper, and attempt to find web pages for each company based on a set of rules described below. This step can take several hours, as there are hundreds of thousands of firms and up to 18 urls are checked for each one. To check if a url exists, a simple request is sent with a timeout of 15 seconds. A url is considered valid if the request is successful (`status_code==200`). All valid urls are first saved to the file `website_finder_raw.jl` and then filled into the dataframes. Each checked url is placed on a separate row in the table, and an additional column indicates if the url is valid or not. The resulting DataFrames are saved in `pkl` and `xlsx` format in the new folder `firms_website_finder`.

### Url construction

At the moment, the construction of possible urls is based solely on the firm's name and comprises following steps:
 - remove the trailing 'in Liquidation' (in any language), if present;
 - remove the company form (ag, gmbh, ...), if present;
 - if the name consists of more than one word, consider also partial names with only the first one, two, three or four words;
 - for each list of words (full company name, and first few words) join the words with hypens or simply chain them together;
 - complete each possibility with a `.ch` or `.com` top level domain.
These rules create up to 18 possible urls per firm.

## iso-finder

When the output `pkl`-files produced by the website-finder are available, run:
~~~
python iso_finder.py
~~~
This will take each valid url and follow all links on that page (always staying on the same domain) in search for information about ISO certificates.
The iso-finder trigger is based on the following list of keywords (case sensitive): "ISO", "zertifikat", "zertifizierung", "certifica". It follows these criteria:
 - the webpage contains a link to a file of type `pdf, png, jpg or jpeg` which has a keyword (case insensitive) in its name;
 - the text (or source code) of the webpage contain one or more keywords;
 - the webpage displays an image which has a keyword (case insensitive) in its name.
The raw results are saved to the file `iso_finder_raw.jl` and then filled into the dataframes. Each suburl found is placed on a separate line, and new columns are added indicating which of the trigger criteria are fulfilled. The resulting DataFrames are saved in `pkl` and `xlsx` format in the new folder `firms_iso_finder`.

## Possible improvements

### website-finder
 - Some firms might be excluded a priori by filtering by available info (e.g. ehraid, uid, ...), as some categories might not require a ISO certificate at all, for example.
 - Expanding the list of possible top level domains.
 - It should be possible to obtain a list of all `*.ch` domain names and then search within it to find firm names. See https://rimann.org/blog/listing-all-ch-domains.
 - Use a search engine like Google to search for web pages.
