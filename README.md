## Introduction

BGPblend combines BGP data from RIPE RIS and Routeviews to address the challenge of mapping IP Prefixes to ASes with high accuracy and coverage.
Due to BGP prefix hijack attacks, route leaks or any other misconfiguration, a daily BGP snapshot may contain tainted entries misinfering AS-to-prefix mappings.

**Why BGPblend is important?**
Can be used for:
- More accurate monitoring when translating IP paths to AS paths
- Preliminary indications of BGP misconfigurations/attacks
- More accurate IP geolocation based on AS country feeds

------------

## How it works
- For a given time window it collects all the announced ASes as seen from both RIPE RIS and Routeviews monitors and for each AS it fetches the corresponding prefixes.
- Selecting a consistency threshold (%), we preserve only the AS-to-prefix mappings that consistently appeared for more than the applied threshold (number of days) accross the selected period.
- Then, it merges the RIPE RIS and Routeviews datasets respectively accross the time window for which it has already downloaded data according to the initial step, extracting two representative merged files for every database.
- Finally, it merges the two merged files from the previous step. 
- We filter out the final list with the reserved IP prefixees.
- For those conflicting cases where for the same prefix we have multiple AS mappings, we consider all associated ASes as valid mappings and treat the case as BGP MOAS.

## How to run
For example, to download datasets for the time period between 2022-02-01 and 2022-03-01 (Y-M-D), run:

`$ python3 bgpblend.py -id ./ -s 2022-02-01 -e 2022-03-01 download -m 2`

It will create two directories (ris and routeviews) containing all the ASes with their announced prefixes for each date

Then, to merge the retrieved datasets for the time period between 2022-02-01 and 2022-03-01 (Y-M-D) (for which you have previously downloaded the relative datasets), run:

`$ python3 bgpblend.py  -id ./ -s 2022-02-01 -e 2022-03-01 merge -o test -ex private_reserved_v4.txt -t 25`

It will create two directories, the merged dir containing the two merged .json files merging the datasets from RIS and Routeviews respectively based on the selected consistency threshold (25% in our example, which is barely 1 week)
and the final dir, containing the merged file as derived merging the two files from the merged folder.

## Requirements
- Python 3.6 or greater

## Limitation
It only works with the IPv4 address space.

## License

This repository is licensed under the [GNU AGPLv3](LICENSE). All code in this repository belongs to Georgios Nomikos unless otherwise stated.
