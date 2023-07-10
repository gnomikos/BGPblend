#!/usr/bin/env python3

import lib
import os
import datetime
from dateutil import rrule
import concurrent.futures
import requests
import netaddr
import re

class ripe_ris():
    def __init__(self, input_dir):
        # Create initial directories for RIPE and Routeviews datasets
        if not os.path.isdir(input_dir+'ris/'):
            os.mkdir(input_dir+'ris/')
        if not os.path.isdir(input_dir+'routeviews/'):
            os.mkdir(input_dir+'routeviews/')


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
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.get_and_write_ris_asns, list_of_param_dicts)
        

    def get_and_write_ris_asns(self, params):
        # For each date collect all the available/announced ASNs and export them in ris/ dir in .json files

        starttime = params['starttime']
        list_asns = params['list_asns']
        ris_data_dir = params['ris_data_dir']

        if os.path.exists(ris_data_dir+starttime+'.json'):
            print('Skipping %s RIS ASN snapshot' % starttime)
        else:
            print('Retrieving RIS ASNs for the snapshot: %s' % starttime)
            ripe_ris_widget_url = "https://stat.ripe.net/data/ris-asns/data.json?list_asns=" + list_asns 
            ripe_ris_widget_url += "&query_time=" + starttime
            
            request_response = requests.get(url=ripe_ris_widget_url)
            lib.export_json(request_response.json(),ris_data_dir+starttime+'.json')


    def ris_prefixes_scheduler(self, start_date, end_date, input_dir, max_workers):
        # For each date-ASN snapshot it orchestrates to fetch the prefix list per ASN.

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
        # Assign to different worker to fetch prefixes for a certain date snapshot
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self.ris_asns_parser, list_of_param)


    def ris_asns_parser(self, params):
            date = params['starttime']
            asns = lib.import_json(params['input_dir']+'asns/'+date+'.json')['data']['asns']
            if os.path.isdir(params['input_dir']+'snapshots/'+date):
                print('Skipping %s RIS prefix snapshot' % date)
            else:
                self.get_and_write_ris_prefixes( date, date, ris_data_dir=params['input_dir']+'snapshots/', asns = asns)


    def get_and_write_ris_prefixes(self, starttime, endtime, ris_data_dir, asns = [], min_peers_seeing=2):

        # Retrieve RIS prefixes of the candidate AS list for the given date
    
        starttime = datetime.datetime.strptime(starttime, "%Y-%m-%d")
        endtime   = datetime.datetime.strptime(endtime, "%Y-%m-%d")

        for time in rrule.rrule(rrule.DAILY, dtstart=starttime, until=endtime):
            date = str(time.date())
            print('Retrieving RIS prefixes for the ASN snapshot: %s' % date)
            
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
        
        # for each date the number of max_workers correspond to the number of ASes to concurrently  download their prefixes
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.export_prefixes, list_of_param_dicts)
        

    def export_prefixes(self, params):
        # Exports the retrieved prefixes for the queried ASN
        prefixes_set = set()

        asn              = params['asn']
        min_peers_seeing = params['min_peers_seeing']
        starttime        = params['starttime']
        endtime          = params['endtime']

        ripe_ris_widget_url = "https://stat.ripe.net/data/announced-prefixes/data.json?min_peers_seeing=" + min_peers_seeing 
        ripe_ris_widget_url += "&resource=" + asn
        ripe_ris_widget_url += "&starttime=" + starttime
        ripe_ris_widget_url += "&endtime=" + endtime
        
        try:
            request_response = requests.get(url=ripe_ris_widget_url, timeout=10)

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
                    filename = "{}/AS{}.json".format(params["ris_data_dir"], asn)
                    lib.export_json(prefixes_list, filename)
        except ConnectTimeout:
            print('Request has timed out for AS', asn, starttime)

        
