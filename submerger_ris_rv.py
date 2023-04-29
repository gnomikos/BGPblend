#!/usr/bin/env python3

import sys
import argparse
import os
import gzip
from dateutil import rrule
import datetime
import concurrent.futures
import shutil
import json
import requests
import re
import netaddr
from bs4 import BeautifulSoup
import urllib
import wget
import socket, ipaddress
import ip_to_as_lib as lib

'''
Example Command:
python3 submerger_ris_rv.py -id ./ -s 2022-02-01 -e 2022-02-28 -m 2 -o 2022_02_25thres -t 25

I would suggest to keep the number of workers up to 5 (-m option). Only for strong machines you can increase the number because it fires up multiple of threads based on the number of given processes
'''

class SubMerger():

    def __init__(self, input_dir):
        # Create initial directories for RIPE and Routeviews datasets
        if not os.path.isdir(input_dir+'ris/'):
            os.mkdir(input_dir+'ris/')
        if not os.path.isdir(input_dir+'routeviews/'):
            os.mkdir(input_dir+'routeviews/')

    def get_dates(self, start_date, end_date):
        # Creates a list of dates for the given period, in order to download daily snapshots where it is necessary
        dates = []

        starttime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        endtime   = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        for time in rrule.rrule(rrule.DAILY, dtstart=starttime, until=endtime):
            date = str(time.date()).replace('-','')
            dates.append(date)
        return dates

    def get_and_write_ris_asn_prefixes(self, params):
        # Exports the retrieved prefixes based on the queried ASes
        prefixes_set = set()

        asn              = params['asn']
        min_peers_seeing = params['min_peers_seeing']
        starttime        = params['starttime']
        endtime          = params['endtime']

        ripe_ris_widget_url = "https://stat.ripe.net/data/announced-prefixes/data.json?min_peers_seeing=" + min_peers_seeing 
        ripe_ris_widget_url += "&resource=" + asn
        ripe_ris_widget_url += "&starttime=" + starttime
        ripe_ris_widget_url += "&endtime=" + endtime
        
        request_response = requests.get(url=ripe_ris_widget_url)
        json_data = request_response.json()
        prefixes_raw_list = set()
       
        if 'data' in json_data:
            if 'resource' in json_data['data'] and json_data['data']['resource'] == asn:
                if 'prefixes' in json_data['data']:
                    for item in json_data['data']['prefixes']:
                        prefixes_raw_list.add(item['prefix'])
        prefixes_set = set(prefixes_raw_list)
        prefixes_list = list(prefixes_set)

        if prefixes_list:
            prefixes_to_remove = set()
            for prefix in prefixes_list:
                ipv4_net_match = re.match('^([0-9.]+){4}\/[0-9]+$', prefix)
                if not ipv4_net_match:
                    prefixes_to_remove.add(prefix)
                else:
                    try:
                        netaddr_prefix = netaddr.IPNetwork(prefix)
                    except:
                        prefixes_to_remove.add(prefix)
            for prefix in prefixes_to_remove:
                prefixes_list.remove(prefix)
            
            if prefixes_list:
                with open("{}/AS{}.json".format(params["ris_data_dir"], asn), 'w') as f:
                    json.dump(prefixes_list, f)

    def get_and_write_ris_asns(self, params):
        # Exports the retrieved ASNs
        starttime = params['starttime']
        list_asns = params['list_asns']
        ris_data_dir = params['ris_data_dir']

        if os.path.exists(ris_data_dir+starttime+'.json'):
            print('Skipping %s RIS ASN snapshot' % starttime)
        else:
            print('Retrieving %s RIS ASN snapshot' % starttime)
            ripe_ris_widget_url = "https://stat.ripe.net/data/ris-asns/data.json?list_asns=" + list_asns 
            ripe_ris_widget_url += "&query_time=" + starttime
            
            request_response = requests.get(url=ripe_ris_widget_url)
            lib.export_json(request_response.json(),ris_data_dir+starttime+'.json')

    def ris_asns_scheduler(self, starttime, endtime, input_dir):
        # Initiates the directories to download for each ASN the corresponding prefixes
        
        input_dir+='ris/'
        
        # Create directory
        if not os.path.isdir(input_dir+'asns/'):
            os.mkdir(input_dir+'asns/')

        starttime = datetime.datetime.strptime(starttime, "%Y-%m-%d")
        endtime   = datetime.datetime.strptime(endtime, "%Y-%m-%d")
        
        list_of_param_dicts = []
        for time in rrule.rrule(rrule.DAILY, dtstart=starttime, until=endtime):
            date = str(time.date())

            # prepare arguments for parallelized crawl
            param_dict = {
                "starttime"       : date,
                "list_asns"       : "true",
                "ris_data_dir"    : input_dir+'asns/'
            }
            list_of_param_dicts.append(param_dict)

        # With max_workers=1 parallelization is disabled
        with concurrent.futures.ThreadPoolExecutor(max_workers=31) as executor:
            executor.map(self.get_and_write_ris_asns, list_of_param_dicts)

    def ris_prefixes_scheduler(self, starttime, endtime, ris_data_dir, asns = [], min_peers_seeing=2):
        # Retrieve RIS prefixes of the candidate AS list for the given date
    
        starttime = datetime.datetime.strptime(starttime, "%Y-%m-%d")
        endtime   = datetime.datetime.strptime(endtime, "%Y-%m-%d")

        for time in rrule.rrule(rrule.DAILY, dtstart=starttime, until=endtime):
            date = str(time.date())
            print('Retrieving %s RIS prefix snapshot' % date)
            
            # Create date directory
            if not os.path.isdir(ris_data_dir+date):
                os.mkdir(ris_data_dir+date)
            # prepare arguments for parallelized crawl
            list_of_param_dicts = []
            for asn in asns:
                param_dict = {
                    "asn"             : str(asn),
                    "ris_data_dir"    : ris_data_dir+date,
                    "min_peers_seeing": str(min_peers_seeing),
                    "starttime"       : date,
                    "endtime"         : date
                }
                list_of_param_dicts.append(param_dict)
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(self.get_and_write_ris_asn_prefixes, list_of_param_dicts)

    def parser_ris_asns(self, params):
            date = params['starttime']
            asns = lib.import_json(params['input_dir']+'asns/'+date+'.json')['data']['asns']
            if os.path.isdir(params['input_dir']+'snapshots/'+date):
                print('Skipping %s RIS prefix snapshot' % date)
            else:
                self.ris_prefixes_scheduler( date, date, ris_data_dir=params['input_dir']+'snapshots/', asns = asns)

    def parse_ris_asns(self, start_date, end_date, input_dir, max_workers):
        # For each ASN snapshot it orchestrates to fetch the prefix list per ASN.

        input_dir+='ris/'
        
        # Create snapshots directory
        if not os.path.isdir(input_dir+'snapshots/'):
            os.mkdir(input_dir+'snapshots/')
        
        starttime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        endtime   = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        
        list_of_param = []
        for time in rrule.rrule(rrule.DAILY, dtstart=starttime, until=endtime):
            date = str(time.date())
            param_dict = {
                "starttime"       : date,
                "input_dir"       : input_dir,
                "max_workers"     : max_workers
            }
            list_of_param.append(param_dict)

         # With max_workers=1 parallelization is disabled
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self.parser_ris_asns, list_of_param)

    def parser_rv_snaps(self, params):
        # Parses the raw routeviews snapshots and extracts for each AS its available prefixes

        filename  = params['filename']
        input_dir = params['input_dir']
        date = filename.split('-')[2]
        date = str(datetime.datetime.strptime(date, "%Y%m%d"))[0:10]

        if os.path.isdir(input_dir+'snapshots/'+date):
            print('Skipping %s RV snapshot' % date)            
        else:
            print('Parsing %s' % filename)
            
            asn_to_prefixes = {}
            with gzip.open(input_dir+'raw/'+filename) as f:
                
                for line in f:
                    line = line.strip().decode().split()
                    # Skip AS sets
                    if ',' in line[2]: continue
                    asns   = line[2].split('_')
                    prefix = line[0]+'/'+line[1]
                    if lib.is_valid_ip_address(prefix, 'prefix'):
                        # In case of MOAS
                        for asn in asns:
                            if asn in asn_to_prefixes:
                                asn_to_prefixes[asn].append(prefix)
                            else:
                                asn_to_prefixes[asn] = []
                                asn_to_prefixes[asn].append(prefix)
            
            if asn_to_prefixes:
                os.mkdir(input_dir+'snapshots/'+date)

                for asn in asn_to_prefixes:
                    output_filename = input_dir+'snapshots/'+date+'/AS'+asn+'.json'
                    lib.export_json(asn_to_prefixes[asn],output_filename)

    def download_rv_raw_snaps(self, input_dir, start_date, end_date):
        # Downloads the raw routeviews snapshots for the given period

        def download(params):
            directory = params[0].rsplit('/',1)[0]+'/'
            candidate_filename = params[0].split('/')[-1]

            # We have this parsing party in order to find the correct url because they dont have consistent filenames. e.g. routeviews-rv2-20220301-0200.pfx2as.gz, routeviews-rv2-20220302-1200.pfx2as.gz  
            html_page = requests.get(directory)
            soup = BeautifulSoup(html_page.text, "lxml")
            for link in soup.findAll('a'):
                if link.has_attr('href'):
                    if candidate_filename in link['href']:
                        url = directory+link['href']
                        filename = params[1]+link['href']
                        if os.path.exists(filename):
                            print('Skipping routeviews snap:', filename)
                        else:
                            print('Downloading routeviews snap:', filename)
                            wget.download(url, params[1])
                        break
        
         # Create raw directory to download the raw routeviews snapshots with the prefix-to-AS mappings
        if not os.path.isdir(input_dir+'raw/'):
            os.mkdir(input_dir+'raw/')

        starttime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        endtime   = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        
        list_of_param = []
        for time in rrule.rrule(rrule.DAILY, dtstart=starttime, until=endtime):
            day = '{:02d}'.format(time.day)
            month = '{:02d}'.format(time.month)
            year = str(time.year)
            
            list_of_param.append((
                'https://publicdata.caida.org/datasets/routing/routeviews-prefix2as/'              +
                year+'/' +
                month+'/'+
                'routeviews-rv2-'+
                year +
                month +
                day
                , input_dir+'raw/'))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(download, list_of_param)

    def parse_rv_snaps(self, start_date, end_date, input_dir, max_workers):
        # Initializes the routeviews directories and the parameters to start downloading the raw 
        # routeviews snapshots
        
        list_of_param = []
        input_dir+='routeviews/'

        # Download first the raw routeviews snapshots
        self.download_rv_raw_snaps(input_dir, start_date, end_date)
        
        # Create snapshots directory
        if not os.path.isdir(input_dir+'snapshots/'):
            os.mkdir(input_dir+'snapshots/')

        # Parse only RV snapshots for the specified time window
        dates = self.get_dates(start_date, end_date)        

        # Construct the file list with the RV snapshots to parse in the following step based on the selected dates
        for filename in sorted(os.listdir(input_dir+'raw/')):
            if filename.startswith('routeviews') and filename.endswith('.gz'):
                
                snap_date = filename.split('-')[2]
                if snap_date in dates:
                    print('Preparing to parse %s RV snapshot' % filename)
                    param_dict = {
                        "filename"  : filename,
                        "input_dir" : input_dir
                    }
                    list_of_param.append(param_dict)

         # With max_workers=1 parallelization is disabled
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self.parser_rv_snaps, list_of_param)

    def merge_snapshots(self, start_date, end_date, input_dir, dataset, output_filename, threshold):
        # Merges for each dataset (RIS, routeviews) the daily snapshots extracting
        # two different merged IP prefix to AS mapping files for each dataset

        # Create snapshots directory
        if not os.path.isdir(input_dir+'merged/'):
            os.mkdir(input_dir+'merged/')

        number_of_snaps = 0
        merged_snaps = {}

        starttime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        endtime   = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        for time in rrule.rrule(rrule.DAILY, dtstart=starttime, until=endtime):
            date = str(time.date())
            if os.path.isdir(input_dir+dataset+date):

                number_of_snaps+=1

                print('Merging %s from %s' % (date,dataset))

                 # Calculate prefix frequency announced by a certain ASN
                for filename in sorted(os.listdir(input_dir+dataset+date)):
                    asn         = int(filename.lstrip('AS').rstrip('.json'))
                    prefixes    = lib.import_json(input_dir+dataset+date+'/'+filename)
                    for prefix in prefixes:
                        if (prefix,asn) in merged_snaps:
                            merged_snaps[(prefix,asn)]+=1
                        else:
                            merged_snaps[(prefix,asn)]=1.0 

        db = {}
        # Keep only ip2as mappings complied with the specified threshold
        for prefix,asn in merged_snaps:
            if (merged_snaps[(prefix,asn)]/number_of_snaps)*100 >= threshold:
                if prefix in db:
                    db[prefix].append(asn)
                else:
                    db[prefix] = []
                    db[prefix].append(asn)
    
        lib.export_json(db, input_dir+'merged/'+ dataset.split('/')[0]+'_'+output_filename+'.json')
    
def main():

    parser = argparse.ArgumentParser(description="Script to retrieve parse and merge RIPE RIS and Routeviews snapshots to produce merged IP prefix to AS mappings per dataset")
    parser.add_argument('-id', '--input_dir', type=str, help='name of the input directory containing all the input files', required=True)
    parser.add_argument('-s', '--start_date', type=str, help='First date of datasets to retrieve', required=True)
    parser.add_argument('-e', '--end_date', type=str, help='Final date of datasets to retrieve', required=True)
    parser.add_argument('-m', '--max_workers', type=str, help='number of processes and threads to be spawned', default=2)
    parser.add_argument('-o', '--output_filename', type=str, help='suffix of the .json output filename concerning the merged snapshots of the candidate database', required=True)
    parser.add_argument('-t', '--threshold', type=int, help='merging threshold', required=True)
    args = parser.parse_args()
    
    max_workers = int(args.max_workers)
    
    submerger = SubMerger(args.input_dir)

    
    # Step 1: Download RIPE RIS ASN snapshots. 
    # Skips existing AS snapshots
    submerger.ris_asns_scheduler(args.start_date,args.end_date, args.input_dir)    

    # Step 2: Parse RIPE RIS ASN snapshots to fetch prefix-to-AS mappings
    # Step 3: Download RIPE RIS prefix snapshots based on the RIS ASN snapshots
    # Skips existing RIS snapshots
    submerger.parse_ris_asns(args.start_date, args.end_date, args.input_dir, max_workers=max_workers)
    
    # Step 4: Download and parse RV snapshots to extract ASNs
    # Skips existing snapshots BUT it needs the raw .gz files
    submerger.parse_rv_snaps(args.start_date, args.end_date, args.input_dir, max_workers)    
    
    # Step 5: Merge RV snapshots
    submerger.merge_snapshots(args.start_date, args.end_date, args.input_dir, 'routeviews/snapshots/', args.output_filename, args.threshold)
    
    # Step 6: Merge RIS snapshots
    submerger.merge_snapshots(args.start_date, args.end_date, args.input_dir, 'ris/snapshots/', args.output_filename, args.threshold)

if __name__ == '__main__':
    main()
