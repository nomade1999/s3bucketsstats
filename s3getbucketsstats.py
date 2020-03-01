import json
import math
import re
import sys
import time
from argparse import ArgumentParser
from datetime import datetime

import boto3
import requests
from botocore.config import Config

import itertools

try:
    s3 = boto3.client('s3')
except Exception as e:
    print("Exception on s3 instantiation ", e)

groups_dict = {'REDUCED_REDUNDANCY', 'STANDARD', 'STANDARD_IA'}
size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")


class Settings(object):

    def __init__(self):
        self._REGEX = ".*"
        self._BUCKET_LIST_REGEX = '.*'
        self._KEY_PREFIX = '/'
        self._DISPLAY_SIZE = 3
        self._REGION_FILTER = '.*'

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
    print(acl.grants)
    groups = itertools.groupby(sorted(acl.grants, key=lambda k: k['Permission']), lambda k: k['Permission'])
    for k, g in groups:
        perm = {}
        gt = []
        for x in g:
            try:
                gt.extend([x["Grantee"]['DisplayName']])
            except Exception as e:
                # print("XX: {}".format(x["Grantee"]))
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


def get_inventory(bucket_name):
    try:
        bucket_inventory = s3.list_bucket_inventory_configurations(Bucket=bucket_name)["InventoryConfigurationList"]
        inventory = 'Enabled'
    except Exception as e:
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


def contents(bucket, prefix='/', delimiter='/', start_after=''):
    bucket_name = bucket["Name"]
    start = time.perf_counter()
    all_objects = []
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    s3_paginator = s3.get_paginator('list_objects_v2')
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after,
                                      PaginationConfig={'PageSize': 1000}):
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
            lastmodified = datetime.fromisoformat(str(x["LastModified"]))
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
    print('time:', time.perf_counter() - start)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-l", dest="bucket_list", required=False, default='.*', help="Regex to filter which buckets to process.")
    parser.add_argument("-k", dest="key_prefix", required=False, default='/', help="Key prefix to filter on, default='/'")
    parser.add_argument("-r", dest="region_filter", required=False, default='.*', help="Regex Region filter")
    #parser.add_argument("-g", dest="groupby_region", required=False, default='', help="Group by Region")
    parser.add_argument("-s", dest="display_size", type=int, required=False, default=3,
                        help="Display size in 0:B, 1:KB, 2:MB, 3:GB, 4:TB, 5:PB, 6:EB, 7:ZB, 8:YB")

    #if len(sys.argv) == 1:
    #    print_help()
    #    sys.exit()

    settings = Settings()
    arguments = parser.parse_args()
    if arguments.display_size:
        settings.set_display_size(int(arguments.display_size))
    if arguments.bucket_list:
        settings.set_bucket_list_regex(arguments.bucket_list)
    if arguments.region_filter:
        settings.set_region_filter(arguments.region_filter)
    if arguments.key_prefix:
        settings.set_key_prefix(arguments.key_prefix)

    buckets = s3.list_buckets()

    for bucket in buckets["Buckets"]:
        bucket_name = bucket['Name']
        if not re.match(settings._BUCKET_LIST_REGEX, bucket_name):
            continue
        if not re.match(settings._REGION_FILTER, get_region(bucket_name)):
            continue

        print("Name: {} Created: {}".format(bucket_name, bucket["CreationDate"]))
        all_buckets = []
        for object in contents(bucket, settings._KEY_PREFIX):
            all_buckets.extend(object)
            json_object = json.loads(json.dumps(object))
            json_formatted_str = json.dumps(json_object, indent=2)
            print(json_formatted_str)
