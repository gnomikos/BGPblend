#!/usr/bin/env python3

import sys
import argparse
import os
import ip_to_as_lib as lib
import ripe_ris 
import routeviews
import merger

    
def main():

    parser = argparse.ArgumentParser(description="Script to retrieve parse and merge RIPE RIS and Routeviews snapshots to produce merged IP prefix to AS mappings per dataset")
    subparsers = parser.add_subparsers(help='choose either download or merge for help', dest='subparser_name')

    parser_download = subparsers.add_parser('download', help='download --help')
    parser_merge = subparsers.add_parser('merge', help='merge --help')

    parser.add_argument('-s', '--start_date', type=str, help='First date of datasets to retrieve', required=True)
    parser.add_argument('-e', '--end_date', type=str, help='Final date of datasets to retrieve', required=True)
    parser.add_argument('-id', '--input_dir', type=str, help='name of the input directory containing all the input files', required=True)
    

    parser_download.add_argument('-m', '--max_workers', type=str, help='number of processes and threads to be spawned', default=2)

    parser_merge.add_argument('-o', '--output_filename', type=str, help='suffix of the .json output filename as stored in the final directory after merging ris and routeviews snapshots for the selected time window', required=True)
    parser_merge.add_argument('-t', '--threshold', type=int, help='merging threshold', required=True)
    parser_merge.add_argument('-ex', '--exclude_file_name', type=str, help='file with prefixes to exclude from db/sub_radb', required=True)
    
    args = parser.parse_args()

    # Create initial directories for RIPE and Routeviews datasets
    if not os.path.isdir(args.input_dir+'ris/'):
        os.mkdir(input_dir+'ris/')
    if not os.path.isdir(args.input_dir+'routeviews/'):
        os.mkdir(input_dir+'routeviews/')

    '''
    python3 bgpblend.py -id ./ -s 2022-02-01 -e 2022-02-01 download -m 2
    '''
    if args.subparser_name == 'download':
        # Step 1: Download RIPE RIS ASN snapshots. 
        # Skips existing AS snapshots. To re-download remove the sub(directory)
        ripe_ris_ = ripe_ris.ripe_ris()
        ripe_ris_.ris_asns_scheduler(args.start_date, args.end_date, args.input_dir)    

        # Step 2: Parse RIPE RIS ASN snapshots to fetch prefix-to-AS mappings
        # Step 3: Download RIPE RIS prefix snapshots based on the RIS ASN snapshots. That means, for each date for each ASN fetch the respective announced prefixes.
        # Skips existing RIS snapshots, to re-download remove the (sub)directory
        ripe_ris_.ris_prefixes_scheduler(args.start_date, args.end_date, args.input_dir, max_workers=int(args.max_workers))
        
        routeviews_ = routeviews.routeviews()
        # Step 4: Download and parse RV snapshots to extract ASNs
        # Skips existing snapshots BUT it needs the raw .gz files
        routeviews_.routeviews_scheduler(args.start_date, args.end_date, args.input_dir, int(args.max_workers))    

    '''
    python3 bgpblend.py  -id ./ -s 2022-02-01 -e 2022-02-01 merge -o test -ex private_reserved_v4.txt -t 10 -odir ./
    '''
    if args.subparser_name == 'merge':
        merger_ = merger.merger()
        # Step 5: Merge RV snapshots
        merger_.merge_snapshots(args.start_date, args.end_date, args.input_dir, 'routeviews/snapshots/', args.output_filename, args.threshold)
        
        # Step 6: Merge RIS snapshots
        merger_.merge_snapshots(args.start_date, args.end_date, args.input_dir, 'ris/snapshots/', args.output_filename, args.threshold)

        # Step 7: Merge the merged RIPE and routeviews snapshots for the given time window
        merger_.merge_ris_routeviews(args.start_date, args.end_date, args.input_dir, args.exclude_file_name)

if __name__ == '__main__':
    main()
