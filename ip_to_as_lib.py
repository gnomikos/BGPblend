#!/usr/bin/env python3

import ujson as json
import netaddr
import ipaddress
import socket
import pytricia

def calc_prefix_diff(super_prefix, sub_prefix):
    super_prefix_netaddr = netaddr.IPNetwork(super_prefix)
    sub_prefix_netaddr = netaddr.IPNetwork(sub_prefix)
    assert sub_prefix_netaddr in super_prefix_netaddr, "'{}' is not a super-prefix of '{}'".format(super_prefix, sub_prefix)
    prefix_diff_list = netaddr.cidr_exclude(super_prefix_netaddr, sub_prefix_netaddr)
    return set(map(str, prefix_diff_list))

def convert_json_to_pyt(json):
    pyt = pytricia.PyTricia(128)

    for entry in json:
        pyt[entry] = json[entry]
    return pyt

def dict_list_to_set(data):
    for entry in data:
        data[entry] = set(data[entry])
    return data

def dict_mask_to_prefixes(prefixes):
    mask_to_prefixes = {}

    for prefix in prefixes:
        mask = int(prefix.split('/')[1])
        if mask in mask_to_prefixes:
            mask_to_prefixes[mask].add(prefix)
        else:
            mask_to_prefixes[mask] = set()
            mask_to_prefixes[mask].add(prefix)

    return {mask:mask_to_prefixes[mask] for mask in sorted(mask_to_prefixes)}

def import_json(filename):
    try:
        with open(filename, 'r') as fp:
            data = json.load(fp)
        return data
    except Exception:
        print('Import Error with file:', filename)
        exit()

def export_json(data, filename):
    try:
        with open(filename, 'w') as fp:
            json.dump(data, fp)
    except Exception:
        print('Export Error with file:', filename)
        exit()

def export_pyt_to_json(pyt, filename):
    data = {}
    try:
        with open(filename, 'w') as fp:
            for prefix in pyt:
                data[prefix] = pyt[prefix]
            json.dump(data, fp)
    except Exception:
        print('Export Error with file:', filename)
    
def is_valid_ip_address(address, kind):
    '''
    Checks if the given ip/prefix is valid.
    Input: 
        a) address: ip or prefix.
        b) kind: "ip" for ip address or "prefix" for prefix.
    Output:
        a) True if the given input is valid, False otherwise.
    '''

    if not address:
        return False
    else:
        # For IPs handling.
        if kind == 'ip':
            try:
                socket.inet_aton(address)
                return True
            except socket.error as e:
                print('error with IP:', address, '-', e)
                return False
            
        # For Prefixes Handling.
        elif kind == 'prefix':
            try:
                if ':' not in address and ipaddress.IPv4Network(address):
                    return True
                elif ':' in address and ipaddress.IPv6Network(address):
                    return True
            except ValueError as e:
                print('Error 1 with prefix:', address, '-', e)
            except ipaddress.AddressValueError as e:
                print('Error 2 with prefix:', address, '-', e)
            except ipaddress.NetmaskValueError as e:
                print('Error 3 with prefix:', address, '-', e)

            return False

