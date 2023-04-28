#!/usr/bin/env python3

import argparse
import pytricia
import ip_to_as_lib as lib

'''
Example command:
python3 merger.py -e private_reserved_v4.txt -o final_2022_02_25thres -iris ./merged/ris_2022_02_25thres.json -irv ./merged/routeviews_2022_02_25thres.json -odir ./
'''

db = pytricia.PyTricia()

def clean_db(exclude_prefixes, database):

    # Clean databases from reserved prefixes/subprefixes
    for prefix in database:
        if prefix in exclude_prefixes:
            del database[prefix]
    
    # Fix superprefixes in databases
    for prefix in exclude_prefixes:
        if prefix in database:
            prefix_parent = database.get_key(prefix)

            # Make a new sub_tree to keep consistency in prefix-to-as mappings when new subprefixes are created under diff process for all parents of the given prefix.
            sub_tree = pytricia.PyTricia()
            while prefix_parent:
                # Take diff between superprefix and prefix
                diff_prefixes = lib.calc_prefix_diff(prefix_parent, prefix)
                next_parent = database.parent(prefix_parent)
                parent_ases = database[prefix_parent]
                del database[prefix_parent]
                
                # Make a new tree with the new sub prefixes after diff process.
                for diff_prefix in diff_prefixes:
                    # Do not touch existing prefixes/subprefixes
                    if diff_prefix not in sub_tree:
                        sub_tree[diff_prefix] = parent_ases
                prefix_parent = next_parent

            # Update database with new sub tree
            for new_sub_prefix in sub_tree:
                if new_sub_prefix not in database:
                    database[new_sub_prefix] = sub_tree[new_sub_prefix]  
                
    return database

def import_prefixes_to_exclude(filename):
    with open(filename) as f:
        exclude_prefixes = pytricia.PyTricia()

        for line in f:
            prefix = line.strip()
            if lib.is_valid_ip_address(prefix, 'prefix'):
                exclude_prefixes[prefix] = prefix
            else:
                print('Error with %s prefix when importing list prefixes to exclude' % prefix)
            
    return exclude_prefixes

def clean_from_invalid_prefixes(data):
    # Sanitizes dataset from invalid/malformed IPv4 prefixes
    invalid_prefixes_to_remove = []
    for prefix in data:
        if not lib.is_valid_ip_address(prefix, 'prefix'):
            print('Error when importing prefixes from sub-databases (e.g. ris or routeviews)')
            invalid_prefixes_to_remove.append(prefix)
    
    for invalid_prefix in invalid_prefixes_to_remove:
        del data[invalid_prefix] 

    return data

def add_to_db(prefixes, json):
    global db
    for prefix in prefixes:
        # New prefix in db
        if prefix not in db:
            db[prefix] = json[prefix]
        # (Sub)prefix already in db
        else:
            # In case they have conflict
            if db[prefix] != json[prefix]:
                #Case with same prefixes
                if db.get_key(prefix) == prefix:
                    db[prefix] = db[prefix].union(json[prefix])
                #Case with superprefix and subprefix
                else:
                    db[prefix] = json[prefix]
            # New subprefix, in case of prefix it just overwrites
            else:
                db[prefix] = json[prefix]

def main():

    global db

    parser = argparse.ArgumentParser(description="Script to merge Routeviews, Ripe RIS")
    parser.add_argument('-e', '--exclude_file_name', type=str, help='file with prefixes to exclude from db/sub_radb', required=True)
    parser.add_argument('-o', '--output_filename', type=str, help='suffix of the merged output filenames of the databases', required=True)
    parser.add_argument('-odir', '--output_directory', type=str, help='output directory of the final merged database', required=True)
    parser.add_argument('-irv', '--input_routeviews', type=str, help='routeviews .json filename with prefix2as', required=True)
    parser.add_argument('-iris', '--input_ris', type=str, help='ripe ris .json filename with prefix2as', required=True)
    args = parser.parse_args()

    print('Start merging datasets...')    

    # Import merged IP prefix to AS mappings from Routeviews
    rv_json = lib.dict_list_to_set(lib.import_json(args.input_routeviews))
    rv_json = clean_from_invalid_prefixes(rv_json)
    print('Routeviews snapshot has been imported.')

    # Import merged IP prefix to AS mappings from RIPE RIS
    ripe_json = lib.dict_list_to_set(lib.import_json(args.input_ris))
    ripe_json = clean_from_invalid_prefixes(ripe_json)
    print('RIS snapshot has been imported.')


    rv_masks_to_prefixes   = lib.dict_mask_to_prefixes(rv_json.keys())
    ripe_masks_to_prefixes = lib.dict_mask_to_prefixes(ripe_json.keys())

    # Merging routeviews and ripe
    for mask in sorted(set(rv_masks_to_prefixes.keys()).union(set(ripe_masks_to_prefixes.keys()))):
        # Adding routeviews
        if mask in rv_masks_to_prefixes:
            add_to_db(rv_masks_to_prefixes[mask], rv_json)
        # Adding ripe
        if mask in ripe_masks_to_prefixes:
            add_to_db(ripe_masks_to_prefixes[mask], ripe_json)
    
    # Import reserved prefixes to exclude from databases
    exclude_prefixes = import_prefixes_to_exclude(args.exclude_file_name)

    db = clean_db(exclude_prefixes, db)
    lib.export_pyt_to_json(db, args.output_directory+args.output_filename+'_db.json')
    
    print('Merging has finished')

if __name__ == '__main__':
    main()
