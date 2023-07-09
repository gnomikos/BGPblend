
## Example commands
To download datasets for the time period between 2022-02-01 and 2022-03-01 (Y-M-D):

`$ python3 bgpblend.py -id ./ -s 2022-02-01 -e 2022-03-01 download -m 2`

To merge the retrieved datasets for the time period between 2022-02-01 adn 2022-03-01 (Y-M-D):

`$ python3 bgpblend.py  -id ./ -s 2022-02-01 -e 2022-02-01 merge -o test -ex private_reserved_v4.txt -t 10`

## Requirements
- Python 3.6 or greater

## Limitation
It only works with v4 IP prefixes.

## License

This repository is licensed under the [MIT License](LICENSE). All code in this repository belongs to Georgios Nomikos unless otherwise stated.
