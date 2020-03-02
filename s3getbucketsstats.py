#!/usr/local/bin/python3

import csv
import gzip
import json
import math
import os
import re
import sys
import time
from argparse import ArgumentParser
from datetime import datetime, timedelta
from io import BytesIO, TextIOBase, StringIO

import boto3
import dateutil.parser
import pandas as pd
import requests
from botocore.config import Config

import itertools

try:
    s3 = boto3.client('s3')
except Exception as e:
    print("Exception on s3 instantiation ", e)

groups_dict = {'REDUCED_REDUNDANCY', 'STANDARD', 'STANDARD_IA'}
size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
csv_columns = ['Bucket', 'Key', 'ETag', 'Size', 'LastModified', 'StorageClass']


class Settings(object):
    def __init__(self):
        self._REGEX = ".*"
        self._BUCKET_LIST_REGEX = '.*'
        self._KEY_PREFIX = '/'
        self._DISPLAY_SIZE = 3
        self._REGION_FILTER = '.*'
        self._OUTPUT_FILE = ''
        self._CACHE = False
        self._VERBOSE = 1
        self._REFRESHCACHE = False
        self._INVENTORY = False

    def set_inventory(self, value):
        self._INVENTORY = value

    def set_refresh_cache(self, value):
        self._REFRESHCACHE = value

    def set_verbose(self, value):
        self._VERBOSE = value

    def set_cache(self, value):
        self._CACHE = value

    def set_output_file(self, output_file):
        self._OUTPUT_FILE = output_file

    def set_region_filter(self, regex):
        self._REGION_FILTER = regex

    def set_display_size(self, value):
        self._DISPLAY_SIZE = value

    def set_regex(self, regex):
        self._REGEX = regex

    def set_bucket_list_regex(self, regex):
        self._BUCKET_LIST_REGEX = regex

    def set_key_prefix(self, regex):
        self._KEY_PREFIX = regex


def append_output(results):
    with open(settings._OUTPUT_FILE, "a") as output:
        output.write(results)


def get_region(bucket_name):
    try:
        response = requests.get('http://' + bucket_name + '.s3.amazonaws.com/')
        region = response.headers.get('x-amz-bucket-region')
        return region
    except Exception as e:
        print("Error: couldn't connect to '{0}' bucket. Details: {1}".format(response, e))


def get_encryption(bucket_name):
    try:
        encryption = {
            "ServerSizeEncryption": s3.get_bucket_encryption(Bucket=bucket_name)["ServerSideEncryptionConfiguration"][
                'Rules']}
    except Exception as e:
        encryption = "Disabled"
    return encryption


def get_website(bucket_name):
    try:
        website = s3.get_bucket_website(Bucket=bucket_name)
        bucket_index = website.get("IndexDocument", None)
        bucket_error = website.get("ErrorDocument", None)
        bucket_has_website = 'Enabled'
    except Exception as e:
        bucket_has_website = 'Disabled'
    return bucket_has_website


def get_location(bucket_name):
    try:
        location = s3.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
    except Exception as e:
        location = "n/a"
    return location


def get_versioning(bucket_name):
    try:
        versioning = s3.get_bucket_versioning(Bucket=bucket_name)["Status"]
    except Exception as e:
        versioning = False
    return versioning


def get_grantees(bucket_name):
    acl = boto3.resource('s3').Bucket(bucket_name).Acl()
    grantees = []
    groups = itertools.groupby(sorted(acl.grants, key=lambda k: k['Permission']), lambda k: k['Permission'])
    for k, g in groups:
        perm = {}
        gt = []
        for x in g:
            try:
                gt.extend([x["Grantee"]['DisplayName']])
            except Exception as e:
                try:
                    gt.extend([x["Grantee"]['URI']])
                except Exception as e:
                    print(e)
                continue
        grantees.append({'Permission': k, 'Grantees': gt})
    return grantees


def get_acceleration(bucket_name):
    try:
        s3_client = boto3.client("s3", config=Config(s3={"use_accelerate_endpoint": True}))
        status = s3_client.get_bucket_accelerate_configuration(Bucket=bucket_name)["Status"]
    except Exception as e:
        status = "Disabled"
    return status


def get_analytics(bucket_name):
    try:
        bucket_analytics = s3.list_bucket_analytics_configurations(Bucket=bucket_name)["AnalyticsConfigurationList"]
        analytics = 'Enabled'
    except Exception as e:
        analytics = 'Disabled'
    return analytics


def read_manifest(bucket_name, key):
    kwargs = {'Bucket': bucket_name, 'Key': key}
    data = s3.get_object(**kwargs)
    contents = json.loads(data['Body'].read())
    return contents


def get_inventory_manifest(bucket_name, inventory_bucket, inventory_id):
    kwargs = {'Bucket': inventory_bucket, 'Prefix': bucket_name + "/" + inventory_id + "/"}
    objs = s3.list_objects_v2(**kwargs)['Contents']
    get_last_modified = lambda obj: obj['LastModified']
    latest = sorted(objs, key=get_last_modified, reverse=True)

    if settings._VERBOSE > 3:
        print("AAA: {}".format(json.dumps(json.loads(json.dumps(latest,default=str)), default=str, indent=2)))
    for obj in latest:
        if obj['Key'].endswith('manifest.json'):
            #print("found {} ".format(obj))
            return obj['Key']
    return ''

def load_inventory(bucket_name, inventories):
    inv = []
    for inventory in inventories:
        if settings._VERBOSE > 2:
            print("load_inventory {}".format(inventory))
        if inventory["Format"] == "CSV" and inventory["IsEnabled"]:
            if settings._VERBOSE > 2:
                print("load_inventory {}".format(inventory["Id"]))
            try:
                inventory_bucket = inventory["Bucket"]
                inventory_id = inventory["Id"]
                inventory_manifest = get_inventory_manifest(bucket_name, inventory_bucket, inventory_id)
                print("Using Inventory Id {} for bucket {}".format(inventory_id, bucket_name))
                if inventory_manifest.__len__() == 0:
                    continue
                if settings._VERBOSE > 2:
                    print("inventory_manifest: {}".format(inventory_manifest))
                manifest = read_manifest(inventory_bucket, inventory_manifest)
                if settings._VERBOSE > 2:
                    print("manifest: {}".format(manifest))
                schema = [item.strip() for item in manifest["fileSchema"].split(',')]
                if settings._VERBOSE > 2:
                    print("schema: {}".format(schema))
                    print("files: {}".format(manifest["files"][0]["key"]))
                inv = read_inventory(inventory_bucket, manifest, schema)
            except Exception as e:
                print("load_inventory exception:", e)
                continue
            #print("inventory for {} len={} for {}".format(inv,inv.__len__(), inventory))
            if inv.__len__() > 0:
                break
    return inv


def get_inventory(bucket_name):
    try:
        inventory = []
        bucket_inventory = s3.list_bucket_inventory_configurations(Bucket=bucket_name)["InventoryConfigurationList"]
        if settings._VERBOSE > 3:
            print("INVENTORY: {}".format(bucket_inventory))
        for inventory_bucket in bucket_inventory:
            if settings._VERBOSE > 3:
                print("inventory_bucket: {}".format(inventory_bucket))
            is_enabled = inventory_bucket["IsEnabled"]
            inventory_id = inventory_bucket["Id"]
            inventory_allversions = inventory_bucket["IncludedObjectVersions"]
            if settings._VERBOSE > 3:
                print("Header= {}".format(inventory_bucket["OptionalFields"]))
            inventory_format = inventory_bucket["Destination"]["S3BucketDestination"]["Format"]
            inventory_bucket = inventory_bucket["Destination"]["S3BucketDestination"]["Bucket"].split(':')[-1]
            if settings._VERBOSE > 3:
                print("Bucket= {} format {}".format(inventory_bucket, inventory_format))
            inventory.append({'IsEnabled': is_enabled, 'Bucket': inventory_bucket, 'Id': inventory_id, 'Format': inventory_format})
    except KeyError as e:
        return inventory
    except Exception as e:
        print("ERROR", e)
        inventory = 'Disabled'
    return inventory


def get_replication(bucket_name):
    try:
        # print("REPLICATION: {}".format(s3.get_bucket_replication(Bucket=bucket_name)))
        replication = "Enabled"
    except Exception as e:
        replication = "Disabled"
    return replication


def get_policy(bucket_name):
    try:
        policy = s3.get_bucket_policy(Bucket=bucket_name)["Policy"]
        ispolicy = "Enabled"
    except Exception as e:
        ispolicy = "Disabled"
    return ispolicy


def display_size(size_bytes, sizeformat=-1):
    if sizeformat == -1:
        sizeformat = settings._DISPLAY_SIZE
    i = sizeformat
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "{0}{1}".format(s, size_name[i])


def write_csv(bucket_name, objects):
    with open(bucket_name + ".cache", 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in objects:
            writer.writerow(data)


def panda_read_csv(bucket_name):
    data = []
    df = pd.read_csv(bucket_name + ".cache")
    for index, row in df.iterrows():
        d = row.to_dict()
        data.append(d)
    return data


def read_inventory(bucket_name, manifest, cols_names):
    data = []
    for files in manifest["files"]:
        key = files["key"]
        if settings._VERBOSE > 2:
            print("read_inventory: {} {} {}".format(bucket_name,key,cols_names))
        read_file = s3.get_object(Bucket=bucket_name, Key=key)
        gzipfile = gzip.GzipFile(fileobj=BytesIO(read_file['Body'].read()))
        df = pd.read_csv(gzipfile, sep=',', header=None, names=cols_names)
        for index, row in df.iterrows():
            d = row.to_dict()
            if settings._VERBOSE > 3:
                print({'LastModifiedDate': d["LastModifiedDate"], 'Size': d["Size"], 'StorageClass': d["StorageClass"]})
            data.append({'LastModifiedDate': d["LastModifiedDate"], 'Size': d["Size"], 'StorageClass': d["StorageClass"]})
        if settings._VERBOSE > 2:
            print("read_inventory {} completed {}".format(key, data.__len__()))
    return data


def add_bool_arg(parser, name, default=False, description=""):
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('-' + name, dest=name, action='store_true',
                       help=description + (" (DEFAULT) " if default else ''))
    group.add_argument('-no-' + name, dest=name, action='store_false',
                       help='Do not ' + description + (" (DEFAULT) " if not default else ''))
    parser.set_defaults(**{name: default})


def analyse_bucket_contents(bucket, prefix='/', delimiter='/', start_after=''):
    all_objects = []
    bucket_name = bucket["Name"]
    inventory = get_inventory(bucket_name)
    if settings._REFRESHCACHE and os.path.isfile(bucket_name + ".cache"):
        os.remove(bucket_name + ".cache")
        print("removed")
    if settings._CACHE and os.path.isfile(bucket_name + ".cache"):
        all_objects = panda_read_csv(bucket_name)
    elif settings._INVENTORY and inventory != 'Disabled' and inventory.__len__() > 0:
        print("Try via Inventory for bucket {}".format(bucket_name), end="\r")
        all_objects = load_inventory(bucket_name, inventory)
    if all_objects.__len__() == 0:
        prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
        start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
        s3_paginator = s3.get_paginator('list_objects_v2')
        if settings._CACHE:
            write_csv(bucket_name, [])
        for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after,
                                          PaginationConfig={'PageSize': 1000}):
            if settings._CACHE:
                write_csv(bucket_name, page.get('Contents', ()))
            all_objects.extend(page.get('Contents', ()))
            print("{}: Objects so far: {}".format(bucket_name, all_objects.__len__()), end="\r")
    groups = itertools.groupby(sorted(all_objects, key=lambda k: k['StorageClass']), lambda k: k['StorageClass'])
    bucket_content = []
    bucket_size = 0
    for k, g in groups:
        latest = {}
        cnt = 0
        size = 0
        for x in g:
            bucket_size += x['Size']
            size += x['Size']
            cnt += 1
            if "LastModified" in x:
                lastmodified = datetime.fromisoformat(str(x["LastModified"]))
            else:
                lastmodified = dateutil.parser.parse(x["LastModifiedDate"])
            if latest == {} or lastmodified > latest:
                latest = lastmodified

        bucket_objects = [
            {
                'Group': k,
                'Count': cnt,
                'Size': display_size(size),
                'LastModified': str(latest)
            }
        ]
        bucket_content.extend(bucket_objects)

    bucket_objects = sum(item["Count"] for item in bucket_content)

    bucket_stats = [
        {
            'Name': bucket_name,
            'CreationDate': str(bucket["CreationDate"]),
            'Versioning': get_versioning(bucket_name),
            'WebSite': get_website(bucket_name),
            'Analytics': get_analytics(bucket_name),
            'Acceleration': get_acceleration(bucket_name),
            'Replication': get_replication(bucket_name),
            'Policy': get_policy(bucket_name),
            'Inventory': get_inventory(bucket_name),
            'Region': get_region(bucket_name),
            'LocationConstraint': get_location(bucket_name),
            'Grantee': get_grantees(bucket_name),
            'Encryption': get_encryption(bucket_name),
            'Size': display_size(bucket_size),
            'Count': bucket_objects,
            'Content': bucket_content
        }
    ]
    yield bucket_stats


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-v", dest="verbose", required=False, default=1, help="Verbose level, 0 for quiet.")
    parser.add_argument("-l", dest="bucket_list", required=False, default='.*',
                        help="Regex to filter which buckets to process.")
    parser.add_argument("-k", dest="key_prefix", required=False, default='/',
                        help="Key prefix to filter on, default='/'")
    parser.add_argument("-r", dest="region_filter", required=False, default='.*', help="Regex Region filter")
    parser.add_argument("-o", dest="output", required=False, default='', help="Output to File")
    parser.add_argument("-s", dest="display_size", type=int, required=False, default=3,
                        help="Display size in 0:B, 1:KB, 2:MB, 3:GB, 4:TB, 5:PB, 6:EB, 7:ZB, 8:YB")

    add_bool_arg(parser, "cache", False, "Use Cache file if available")
    add_bool_arg(parser, "refresh", False, "Force Refresh Cache")
    add_bool_arg(parser, "inventory", True, "Use Inventory if exist")

    settings = Settings()
    arguments = parser.parse_args()

    if arguments.verbose:
        settings.set_verbose(int(arguments.verbose))
    if arguments.display_size:
        settings.set_display_size(int(arguments.display_size))
    if arguments.bucket_list:
        settings.set_bucket_list_regex(arguments.bucket_list)
    if arguments.region_filter:
        settings.set_region_filter(arguments.region_filter)
    if arguments.refresh:
        settings.set_refresh_cache(arguments.refresh)
    if arguments.cache:
        settings.set_cache(arguments.cache)
    if arguments.key_prefix:
        settings.set_key_prefix(arguments.key_prefix)
    if arguments.output is not "output.txt":
        settings.set_output_file(arguments.output)
    if arguments.inventory:
        settings.set_inventory(arguments.inventory)

    buckets = s3.list_buckets()
    all_buckets = []

    realstart = time.perf_counter()
    for bucket in buckets["Buckets"]:
        bucket_name = bucket['Name']
        if not re.match(settings._BUCKET_LIST_REGEX, bucket_name):
            continue
        if not re.match(settings._REGION_FILTER, get_region(bucket_name)):
            continue

        print("Name: {} Created: {}".format(bucket_name, bucket["CreationDate"]))
        start = time.perf_counter()
        for object in analyse_bucket_contents(bucket, settings._KEY_PREFIX):
            all_buckets.extend(object)
            json_object = json.loads(json.dumps(object))
            json_formatted_str = json.dumps(json_object, indent=2)
            if settings._VERBOSE > 1:
                print(json_formatted_str)
            print('bucket {} process time: {}'.format(bucket_name, time.perf_counter() - start))
            start = time.perf_counter()

    all_buckets_stats = {"Buckets": all_buckets}
    all_buckets_stats_formatted = json.dumps(json.loads(json.dumps(all_buckets_stats)), indent=2)
    if settings._OUTPUT_FILE.__len__() > 0:
        append_output(all_buckets_stats_formatted)
    if settings._VERBOSE > 0:
        print("AllBuckets: {} ".format(all_buckets_stats_formatted))
    print('Total process time:', time.perf_counter() - realstart)
