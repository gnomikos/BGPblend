## Introduction

BGPblend combines BGP data from RIPE RIS and Routeviews to address the challenge of mapping IP Prefixes to ASes with high accuracy and coverage utilizing a configurable consistency threshold.
Due to BGP prefix hijack attacks, route leaks or any other misconfiguration, a daily BGP snapshot may contain invalid entries misinfering AS-to-IP prefix mappings.

**Why BGPblend is important?**

Can be used for:
- More accurate monitoring when translating IP paths to AS paths
- Preliminary indications of BGP misconfigurations/attacks
- More accurate IP geolocation based on AS country feeds

------------

## How it works
- For a given time window it collects all the announced ASes as seen from both [RIPE RIS](https://stat.ripe.net/docs/data_api "RIPES RIS") and [Routeviews](https://www.routeviews.org/routeviews/ "Routeviews") BGP monitors (via [CAIDA](https://www.caida.org/catalog/datasets/routeviews-prefix2as/ "CAIDA")) and for each AS it fetches the corresponding prefixes.
- Selecting a consistency threshold (0-100%), we preserve only the AS-to-prefix mappings that consistently appeared for more than the applied threshold (number of days) across the selected period.
- Then, it merges the RIPE RIS and Routeviews datasets respectively across the time window for which it has already downloaded data according to the initial step and extracts two representative merged files for each database.
- Finally, it merges the two merged files from the previous step extracting the final database. 
- We filter out the final list with the reserved IP prefixes.
- For those conflicting cases where for the same prefix we have multiple AS mappings, we consider all the assigned ASes as valid mappings and treat the case as BGP MOAS.

## How to run
For example, to download datasets for the time period between 2022-02-01 and 2022-03-01 (Y-M-D), spawning two processes:

`$ python3 bgpblend.py -id ./ -s 2022-02-01 -e 2022-03-01 download -m 2`

It will create two directories (ris and routeviews) containing all the ASes with their announced prefixes for each date.

Then, to merge the retrieved datasets for the time period between 2022-02-01 and 2022-03-01 (for which you have previously downloaded the relative datasets):

`$ python3 bgpblend.py  -id ./ -s 2022-02-01 -e 2022-03-01 merge -o test -ex private_reserved_v4.txt -t 23`

It will create two directories, the **merged** directory containing the two merged .json files after merging the datasets from RIS and Routeviews respectively based on the selected consistency threshold (23% in our example, which is barely 1 week)
and the **final** directory, containing the final merged file as derived merging the two files from the merged folder. This final merged file contains all the AS-to-prefix mappings taking into account the applied consistency factor.

## Requirements
- Python 3.6 or greater
- PyTricia
- UltraJSON

## Limitation
It only works with the IPv4 address space.

## License

This repository is licensed under the [GNU AGPLv3](LICENSE). All code in this repository belongs to [Georgios Nomikos](https://www.linkedin.com/in/georgenomikos).
