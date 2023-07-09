#!/usr/bin/env python3

import argparse
import pytricia
import ip_to_as_lib as lib
from dateutil import rrule
import os
import datetime


class merger():

    def __init__(self):
        self.db = pytricia.PyTricia()

    def clean_db(self, exclude_prefixes):

        # Clean databases from reserved prefixes/subprefixes
        for prefix in self.db:
            if prefix in exclude_prefixes:
                del self.db[prefix]
        
        # Fix superprefixes in databases
        for prefix in exclude_prefixes:
            if prefix in self.db:
                prefix_parent = self.db.get_key(prefix)

                # Make a new sub_tree to keep consistency in prefix-to-as mappings when new subprefixes are created under diff process for all parents of the given prefix
                sub_tree = pytricia.PyTricia()
                while prefix_parent:
                    # Take diff between superprefix and prefix
                    diff_prefixes = lib.calc_prefix_diff(prefix_parent, prefix)
                    next_parent = self.db.parent(prefix_parent)
                    parent_ases = self.db[prefix_parent]
                    del self.db[prefix_parent]
                    
                    # Make a new tree with the new sub prefixes after diff process
                    for diff_prefix in diff_prefixes:
                        # Do not touch existing prefixes/subprefixes
                        if diff_prefix not in sub_tree:
                            sub_tree[diff_prefix] = parent_ases
                    prefix_parent = next_parent

                # Update database with new sub tree
                for new_sub_prefix in sub_tree:
                    if new_sub_prefix not in self.db:
                        self.db[new_sub_prefix] = sub_tree[new_sub_prefix]  
                    
        #return database

    def import_prefixes_to_exclude(self, filename):
        with open(filename) as f:
            exclude_prefixes = pytricia.PyTricia()

            for line in f:
                prefix = line.strip()
                if lib.is_valid_ip_address(prefix, 'prefix'):
                    exclude_prefixes[prefix] = prefix
                else:
                    print('Error with %s prefix when importing list prefixes to exclude' % prefix)
                
        return exclude_prefixes

    def clean_from_invalid_prefixes(self, data):
        # Sanitizes dataset from invalid/malformed IPv4 prefixes
        invalid_prefixes_to_remove = []
        for prefix in data:
            if not lib.is_valid_ip_address(prefix, 'prefix'):
                print('Error when importing prefixes from sub-databases (e.g. ris or routeviews)')
                invalid_prefixes_to_remove.append(prefix)
        
        for invalid_prefix in invalid_prefixes_to_remove:
            del data[invalid_prefix] 

        return data

    def add_to_db(self, prefixes, json):
        
        for prefix in prefixes:
            # New prefix in db
            if prefix not in self.db:
                self.db[prefix] = json[prefix]
            # (Sub)prefix already in db
            else:
                # In case they have conflict
                if self.db[prefix] != json[prefix]:
                    #Case with same prefixes
                    if self.db.get_key(prefix) == prefix:
                        self.db[prefix] = self.db[prefix].union(json[prefix])
                    #Case with superprefix and subprefix
                    else:
                        self.db[prefix] = json[prefix]
                # New subprefix, in case of prefix it just overwrites
                else:
                    self.db[prefix] = json[prefix]


    def merge_ris_routeviews(self, start_date, end_date, input_dir, exclude_file_name):

        # Create snapshots directory
        if not os.path.isdir(input_dir+'final/'):
            os.mkdir(input_dir+'final/')

        print('Start merging datasets...')    

        routeviews_file = [filename for filename in os.listdir(input_dir+'merged/') if filename.startswith('routeviews_'+start_date+'_'+end_date+'_')][0]
        ripe_ris_file = [filename for filename in os.listdir(input_dir+'merged/') if filename.startswith('ris_'+start_date+'_'+end_date+'_')][0]
        
        # Import merged IP prefix to AS mappings from Routeviews
        rv_json = lib.dict_list_to_set(lib.import_json(input_dir+'merged/'+ routeviews_file ))
        rv_json = self.clean_from_invalid_prefixes(rv_json)
        print('Routeviews snapshot has been imported.')

        # Import merged IP prefix to AS mappings from RIPE RIS
        ripe_json = lib.dict_list_to_set(lib.import_json(input_dir+'merged/'+ripe_ris_file))
        ripe_json = self.clean_from_invalid_prefixes(ripe_json)
        print('RIS snapshot has been imported.')


        rv_masks_to_prefixes   = lib.dict_mask_to_prefixes(rv_json.keys())
        ripe_masks_to_prefixes = lib.dict_mask_to_prefixes(ripe_json.keys())

        # Merging routeviews and ripe
        for mask in sorted(set(rv_masks_to_prefixes.keys()).union(set(ripe_masks_to_prefixes.keys()))):
            # Adding routeviews
            if mask in rv_masks_to_prefixes:
                self.add_to_db(rv_masks_to_prefixes[mask], rv_json)
            # Adding ripe
            if mask in ripe_masks_to_prefixes:
                self.add_to_db(ripe_masks_to_prefixes[mask], ripe_json)
        
        # Import reserved prefixes to exclude from databases
        exclude_prefixes = self.import_prefixes_to_exclude(exclude_file_name)

        self.clean_db(exclude_prefixes)
        filename = input_dir+'final/'+'final_'+start_date+'_'+end_date+'_db.json'
        lib.export_pyt_to_json(self.db,filename)
        
        print('Merging has finished')

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

                print('Merging from %s%s' % (dataset,date))

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
                    db[prefix] = [asn]
    
        
        filename = input_dir+'merged/'+ dataset.split('/')[0]+'_'+start_date+'_'+end_date+'_'+output_filename+'.json'
        lib.export_json(db, filename)

