#!/usr/bin/env python3

import lib
import os
import concurrent.futures
from dateutil import rrule
import gzip
from bs4 import BeautifulSoup
import wget
import requests
import datetime

class routeviews():

    def download_rv_raw_snaps(self, input_dir, start_date, end_date):
        # Downloads the raw routeviews snapshots for the given period

        def download(params):
            directory = params[0].rsplit('/',1)[0]+'/'
            candidate_filename = params[0].split('/')[-1]

            #due to non consistent filenames  e.g. routeviews-rv2-20220301-0200.pfx2as.gz, routeviews-rv2-20220302-1200.pfx2as.gz we resolve the issue with the following piece of code
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

    def get_dates(self, start_date, end_date):
        # Creates a list of dates for the given period to download daily snapshots when necessary
        dates = []

        starttime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        endtime   = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        for time in rrule.rrule(rrule.DAILY, dtstart=starttime, until=endtime):
            date = str(time.date()).replace('-','')
            dates.append(date)
        return dates


    def routeviews_scheduler(self, start_date, end_date, input_dir, max_workers):
        # Initializes the routeviews directories and the parameters to start downloading the raw 
        # routeviews snapshots
        
        list_of_param = []
        input_dir+='routeviews/'

        # Download the raw routeviews snapshots
        self.download_rv_raw_snaps(input_dir, start_date, end_date)
        
        # Create the snapshots directory
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
            executor.map(self.routeviews_parser, list_of_param)


    def routeviews_parser(self, params):
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
