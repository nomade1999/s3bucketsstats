#!/usr/local/bin/python3
'''
S3GetBucketStats
version 1.0.0
By Andre Couture
'''

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
from io import BytesIO, StringIO

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
        self._DISPLAY_SIZE = 0
        self._REGION_FILTER = '.*'
        self._OUTPUT_FILE = ''
        self._VERBOSE = 1
        self._CACHE = None
        self._REFRESHCACHE = None
        self._INVENTORY = None
        self._S3SELECT = None

    def set_s3select(self, value):
        self._S3SELECT = value

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
        response = requests.get("http://" + bucket_name + ".s3.amazonaws.com/")
        region = response.headers.get("x-amz-bucket-region")
        return region
    except Exception as e:
        print("Error: couldn't connect to '{0}' bucket. Details: {1}".format(response, e))


def get_encryption(bucket_name):
    try:
        encryption = {
            "ServerSizeEncryption": s3.get_bucket_encryption(Bucket=bucket_name)['ServerSideEncryptionConfiguration'][
                'Rules']}
    except Exception:
        encryption = "Disabled"
    return encryption


def get_website(bucket_name):
    try:
        website = s3.get_bucket_website(Bucket=bucket_name)
        response = {
            "IndexDocument": website.get("IndexDocument", None),
            "ErrorDocument": website.get("ErrorDocument", None)
        }
    except Exception:
        response = {}
    return response


def get_location(bucket_name):
    try:
        location = s3.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
    except Exception:
        location = None
    return location


def get_versioning(bucket_name):
    try:
        versioning = s3.get_bucket_versioning(Bucket=bucket_name)['Status']
    except Exception:
        versioning = "Disabled"
    return versioning


def get_grantees(bucket_name):
    acl = boto3.resource("s3").Bucket(bucket_name).Acl()
    grantees = []
    try:
        groups = itertools.groupby(sorted(acl.grants, key=lambda k: k['Permission']), lambda k: k['Permission'])
    except Exception:
        return grantees
    for k, g in groups:
        perm = {}
        gt = []
        for x in g:
            try:
                gt.extend([x['Grantee']['DisplayName']])
            except Exception as e:
                try:
                    if "URI" in x['Grantee']:
                        gt.extend([x['Grantee']['URI']])
                except Exception as e:
                    print(e)
                continue
        grantees.append({'Permission': k, 'Grantees': gt})
    return grantees


def get_acceleration(bucket_name):
    try:
        s3_client = boto3.client("s3", config=Config(s3={'use_accelerate_endpoint': True}))
        status = s3_client.get_bucket_accelerate_configuration(Bucket=bucket_name)['Status']
    except Exception:
        status = "Disabled"
    return status


def get_object_lock_configuration(bucket_name):
    try:
        bucket_configuration = s3.get_object_lock_configuration(Bucket=bucket_name)
        response = bucket_configuration['ObjectLockConfiguration']['ObjectLockEnabled']
    except Exception:
        response = "Disabled"
    return response


def get_inventory_configurations(bucket_name):
    response = []
    try:
        bucket_configuration = s3.list_bucket_inventory_configurations(Bucket=bucket_name)
        if "InventoryConfigurationList" in bucket_configuration:
            for inventory_bucket in bucket_configuration['InventoryConfigurationList']:
                response.append(
                    {
                        'Id': inventory_bucket['Id'],
                        'IsEnabled': inventory_bucket['IsEnabled'],
                        'Bucket': inventory_bucket['Destination']['S3BucketDestination']['Bucket'].split(':')[-1],
                        'Format': inventory_bucket['Destination']['S3BucketDestination']['Format'],
                        'Versions': inventory_bucket['IncludedObjectVersions']
                    }
                )
    except Exception:
        return response
    return response


def get_replication(bucket_name):
    response = []
    try:
        configurations = s3.get_bucket_replication(Bucket=bucket_name)
        if "ReplicationConfiguration" in configurations and "Rules" in configurations['ReplicationConfiguration']:
            for configuration in configurations['ReplicationConfiguration']['Rules']:
                response.append(
                    {
                       'Id': configuration['ID'],
                       'Status': configuration['Status'],
                       'Destination': configuration['Destination']
                    }
                )
    except Exception:
        response = []
    return response


def get_policy(bucket_name):
    try:
        policy = s3.get_bucket_policy(Bucket=bucket_name)['Policy']
        response = "Enabled"
    except Exception:
        response = "Disabled"
    return response


def get_analytics(bucket_name):
    try:
        bucket_analytics = s3.list_bucket_analytics_configurations(Bucket=bucket_name)['AnalyticsConfigurationList']
        response = 'Enabled'
    except Exception:
        response = 'Disabled'
    return response


def find_latest_inventory_manifest_key(bucket_name, inventory_bucket, inventory_id):
    kwargs = {'Bucket': inventory_bucket, 'Prefix': bucket_name + "/" + inventory_id + "/"}
    latest = sorted(s3.list_objects_v2(**kwargs)['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
    manifest = next(key['Key'] for key in latest if key['Key'].endswith("manifest.json"))
    return manifest


def load_manifest(bucket_name, key):
    kwargs = {'Bucket': bucket_name, 'Key': key}
    data = s3.get_object(**kwargs)
    contents = json.loads(data['Body'].read())
    return contents


def load_inventory_csv(bucket_name, inventory_ids):
    inv = []
    for inventory in inventory_ids:
        if inventory['Format'] == "CSV" and inventory['IsEnabled']:
            try:
                if settings._VERBOSE > 0:
                    print("Using Inventory Id '{}' for bucket '{}'".format(inventory['Id'], bucket_name))
                inventory_manifest = find_latest_inventory_manifest_key(bucket_name, inventory['Bucket'], inventory['Id'])
                if inventory_manifest.__len__() == 0:
                    continue
                manifest = load_manifest(inventory['Bucket'], inventory_manifest)
                if settings._VERBOSE > 2:
                    print("manifest: {}".format(manifest))
                schema = [item.strip() for item in manifest['fileSchema'].split(",")]
                if settings._VERBOSE > 2:
                    print("schema: {}".format(schema))
                    print("files: {}".format(manifest['files'][0]['key']))
                inv = read_inventory(inventory['Bucket'], manifest, schema)
            except Exception as e:
                print("load_inventory exception:", e)
                continue
            if inv.__len__() > 0:
                break
    return inv


def display_size(size_bytes, sizeformat=-1):
    if sizeformat == -1:
        sizeformat = settings._DISPLAY_SIZE
    i = sizeformat
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "{0}{1}".format(s, size_name[i])


def write_cache_csv(bucket_name, objects):
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


def s3select_inventory(bucket_name, key, cols_names):
    content_options = {"FieldDelimiter": ",", 'AllowQuotedRecordDelimiter': False}
    # expression = "select * from s3object"
    expression = "select _6,_7,_8,_9 from s3object"
    req = s3.select_object_content(
        Bucket=bucket_name,
        Key=key,
        ExpressionType='SQL',
        Expression=expression,
        InputSerialization={'CompressionType': "GZIP", "CSV": content_options},
        OutputSerialization={'CSV': {}},
    )
    records = []
    for event in req['Payload']:
        if "Records" in event:
            records.append(event['Records']['Payload'].decode('utf-8'))
        elif "Stats" in event:
            stats = event['Stats']['Details']
    file_str = "".join(r for r in records)
    select_df = pd.read_csv(StringIO(file_str), names=['Size', 'LastModifiedDate', 'EncryptionStatus', 'StorageClass'])
    aggr_size = select_df.groupby('StorageClass').agg({'Size': [('TotalSize', 'sum'), ('ObjectsCount', 'count')]})
    print("XXX")
    print(aggr_size)
    print("XXX")
    print(aggr_size['Size']['TotalSize'])

    aggr_latest = select_df.agg({'LastModifiedDate': ['max']})
    return select_df, aggr_size, aggr_latest


def read_inventory(bucket_name, manifest, cols_names):
    data = []
    summary = None
    if settings._S3SELECT:
        data_array1 = []
        data_array2 = []
    for files in manifest['files']:
        key = files['key']
        if settings._S3SELECT:
            data2, data_size, data_latest = s3select_inventory(bucket_name, key, cols_names)
            data_array1.append(data_size)
            data_array2.append(data_latest)
        else:
            data1 = read_inventory_file(bucket_name, key, cols_names)
            data.extend(data1)
    if settings._S3SELECT:
        latest = pd.concat(data_array2)
        summary = pd.concat(data_array1)
        summary.groupby('StorageClass', level=1).agg({'TotalSize': [('TotalSize', 'sum'), ('ObjectsCount', 'sum')]})
        # aggr_latest = latest.agg({'LastModifiedDate': ['max']})
        data = summary.to_dict(orient='list')
        # summary = pd.merge(data_array1, on=['StorageClass','Size'])

    return data


def read_inventory_file(bucket_name, key, cols_names):
    if settings._VERBOSE > 1:
        print("read_inventory_file: {} {} {}".format(bucket_name, key, cols_names))
    s3_client = boto3.client("s3", config=Config(s3={'use_accelerate_endpoint': True}))
    try:
        status = s3_client.get_bucket_accelerate_configuration(Bucket=bucket_name)['Status']
        if settings._VERBOSE > 1:
            print("Loading inventory '{:50}' using acceleration {}".format(key, status))
    except Exception:
        # Acceleration not possible
        s3_client = s3
    data = []
    if settings._VERBOSE > 2:
        print("read_inventory file: s3://{}/{}  Schema:{}".format(bucket_name, key, cols_names))
    read_file = s3_client.get_object(Bucket=bucket_name, Key=key)
    gzipfile = gzip.GzipFile(fileobj=BytesIO(read_file['Body'].read()))
    df = pd.read_csv(gzipfile, sep=',', header=None, names=cols_names)
    for index, row in df.iterrows():
        d = row.to_dict()
        data.append({'LastModifiedDate': d['LastModifiedDate'], 'Size': d['Size'], 'StorageClass': d['StorageClass']})
    if settings._VERBOSE > 2:
        print("read_inventory read {} objects from {}.".format(data.__len__(), key))
    return data


def add_bool_arg(parser, name, default=False, description=""):
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("-" + name, dest=name, action="store_true",
                       help=description + (" (DEFAULT) " if default else ""))
    group.add_argument("-no-" + name, dest=name, action="store_false",
                       help="Do not " + description + (" (DEFAULT) " if not default else ""))
    parser.set_defaults(**{name: default})


def analyse_bucket_contents(bucket_name, prefix="/", delimiter="/", start_after=""):
    all_objects = []
    inventory = get_inventory_configurations(bucket_name)
    if settings._REFRESHCACHE and os.path.isfile(bucket_name + ".cache"):
        os.remove(bucket_name + ".cache")
        print("removed")
    if settings._CACHE and os.path.isfile(bucket_name + ".cache"):
        all_objects = panda_read_csv(bucket_name)
    elif settings._INVENTORY and inventory != "Disabled" and inventory.__len__() > 0:
        print("Try via Inventory for bucket {}".format(bucket_name), end="\r")
        all_objects = load_inventory_csv(bucket_name, inventory)
    if all_objects.__len__() == 0:
        prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
        start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
        s3_paginator = s3.get_paginator("list_objects_v2")
        if settings._CACHE:
            write_cache_csv(bucket_name, [])
        try:
            for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after,
                                              PaginationConfig={'PageSize': 1000}):
                if settings._CACHE:
                    write_cache_csv(bucket_name, page.get("Contents", ()))
                all_objects.extend(page.get("Contents", ()))
                print("{}: Objects so far: {}".format(bucket_name, all_objects.__len__()), end="\r")
        except Exception:
            return []
    groups = itertools.groupby(sorted(all_objects, key=lambda k: k['StorageClass']), lambda k: k['StorageClass'])
    bucket_content = []
    bucket_size = 0
    bucket_lastmodified = None
    for k, g in groups:
        latest = {}
        cnt = 0
        size = 0
        for x in g:
            size += x['Size']
            cnt += 1
            if "LastModified" in x:
                lastmodified = datetime.fromisoformat(str(x['LastModified']))
            else:
                lastmodified = dateutil.parser.parse(x['LastModifiedDate'])
            if latest == {} or lastmodified > latest:
                latest = lastmodified
        bucket_size += size
        if bucket_lastmodified is None:
            bucket_lastmodified = latest
        elif bucket_lastmodified < latest:
            bucket_lastmodified = latest

        bucket_objects = [
            {
                'Group': k,
                'Count': cnt,
                'Size': display_size(size),
                'LastModifiedDate': str(latest)
            }
        ]
        bucket_content.extend(bucket_objects)

    bucket_objects = sum(item['Count'] for item in bucket_content)

    bucket = boto3.resource("s3").Bucket(bucket_name)

    bucket_stats = [
        {
            'Name': bucket_name,
            'CreationDate': str(bucket.creation_date),
            'LastModified': str(bucket_lastmodified),
            'Versioning': get_versioning(bucket_name),
            'WebSite': get_website(bucket_name),
            'Analytics': get_analytics(bucket_name),
            'Acceleration': get_acceleration(bucket_name),
            'Replication': get_replication(bucket_name),
            'Policy': get_policy(bucket_name),
            'ObjectLock': get_object_lock_configuration(bucket_name),
            'Inventory': get_inventory_configurations(bucket_name),
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
    parser.add_argument("-s", dest="display_size", type=int, required=False, default=0,
                        help="Display size in 0:B, 1:KB, 2:MB, 3:GB, 4:TB, 5:PB, 6:EB, 7:ZB, 8:YB")

    add_bool_arg(parser, "cache", False, "Use Cache file if available")
    add_bool_arg(parser, "refresh", False, "Force Refresh Cache")
    add_bool_arg(parser, "inventory", True, "Use Inventory if exist")
    add_bool_arg(parser, "s3select", False, "Use S3 Select to parse inventory result files (EXPERIMENTAL)")

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
    if arguments.key_prefix:
        settings.set_key_prefix(arguments.key_prefix)
    if arguments.output is not "output.txt":
        settings.set_output_file(arguments.output)
    settings.set_refresh_cache(arguments.refresh)
    settings.set_cache(arguments.cache)
    settings.set_inventory(arguments.inventory)
    settings.set_s3select(arguments.s3select)

    buckets = s3.list_buckets()
    buckets_stats_array = []

    bucket_list = [i['Name'] for i in buckets['Buckets'] if re.match(settings._BUCKET_LIST_REGEX, i['Name'])]
    if bucket_list.__len__() == 0:
        bucket_list.append(settings._BUCKET_LIST_REGEX)

    realstart = time.perf_counter()
    print("{:60}{:>30}{:>20}{:>20}{:>30}{:>40}".format("Bucket", "Created", "Objects", "Size", "LastModified", "Processing Time"), file=sys.stderr)

    for bucket_name in bucket_list:
        try:
            if not re.match(settings._REGION_FILTER, get_region(bucket_name)):
                continue
        except Exception:
            # Bucket does not exist
            continue

        print("{0:60}".format(bucket_name), file=sys.stderr, end="")
        bucket_creation = boto3.resource("s3").Bucket(bucket_name).creation_date
        start = time.perf_counter()
        for object in analyse_bucket_contents(bucket_name, settings._KEY_PREFIX):
            print("{:>30}{:>20}{:>20}{:>30}".format(object[0]['CreationDate'], object[0]['Count'], object[0]['Size'], object[0]['LastModified']), file=sys.stderr, end="")
            buckets_stats_array.extend(object)
            json_object = json.loads(json.dumps(object))
            json_formatted_str = json.dumps(json_object, indent=2)
            print("{:>40} !".format(str(timedelta(milliseconds=round(1000*(time.perf_counter() - start))))), file=sys.stderr)
            if settings._VERBOSE > 1:
                print(json_formatted_str)
            start = time.perf_counter()

    all_buckets_stats_json = json.loads(json.dumps({'Buckets': buckets_stats_array}))
    all_buckets_stats = json.dumps(all_buckets_stats_json, indent=2)
    if settings._OUTPUT_FILE.__len__() > 0:
        append_output(all_buckets_stats)
    if settings._VERBOSE > 0:
        print(all_buckets_stats)
    print("Processed {1:60} buckets in {0:>20}.".format(str(timedelta(milliseconds=round(1000*(time.perf_counter() - realstart)))), len(all_buckets_stats_json['Buckets'])), file=sys.stderr)
