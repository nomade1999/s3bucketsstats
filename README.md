# S3GetBucketsStatistics

Cross platform tool to report some statistics on S3 Buckets.

The tool will accept arguments and output statistics in JSON format.

Statistics that will be returned;

  For each bucket:
  
   * Name
   * Creation date
   * Number of files
   * Total size of files
   * Last modified date of the most recent file
   * Estimate of cost of storage
      
## Getting Started

The challenge was to build a tool that could report some metrics and statistics on a list of buckets.
While most metrics are directly available from single API calls some others are more difficult to get without heavier processing.

For example it is possible to get the total size of a buckets per storage type via cloudwatch but not the count of objects per storage type.
For those statistics it will require to get an inventory of the bucket and make the calculations.

Now it is possible to gather some more analytics and also inventory from AWS but this require to be configured and take about 24 hours to get the first results. 

If an inventory was already setup and enabled it will try to make use of it to reduce processing time. Just make sure that getObjects access is permitted for the user running this script.

### Prerequisites

There are no installations required, just download the script and run. 

You will need to have python3 installed with the boto3 sdk. Most of the other import should be there by default

Here's the list of import;

```
import boto3
import requests
import pandas
```

On a clean linux server I had to first install python3 then boto3 and requests.
```
yum install python3
pip3 install boto3 requests pandas
```

You will also need to get you environment setup to access AWS CLI. 
You have to make sure that you have a minimum of READ ACCESS to all S3 buckets.
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:Get*",
                "s3:List*"
            ],
            "Resource": "*"
        }
    ]
}
```

### Installing

git clone $REPO_URL && cd s3bucketsstats
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python3 s3bucketstats.py -h

## Running examples

Just setup your environement to connect to AWS and test it to make sure you have access to your S3 buckets.
If running on an EC2 instance I would suggest to use IAM Role attached to your EC2 instance.
```
usage: s3getbucketsstats.py [-h] [-v VERBOSE] [-l BUCKET_LIST] [-k KEY_PREFIX]
                            [-r REGION_FILTER] [-o OUTPUT] [-s DISPLAY_SIZE]
                            [-cache | -no-cache] [-refresh | -no-refresh]
                            [-inventory | -no-inventory]
                            [-s3select | -no-s3select]

optional arguments:
  -h, --help        show this help message and exit
  -v VERBOSE        Verbose level, 0 for quiet.
  -l BUCKET_LIST    Regex to filter which buckets to process.
  -k KEY_PREFIX     Key prefix to filter on, default='/'
  -r REGION_FILTER  Regex Region filter
  -o OUTPUT         Output to File
  -s DISPLAY_SIZE   Display size in 0:B, 1:KB, 2:MB, 3:GB, 4:TB, 5:PB, 6:EB,
                    7:ZB, 8:YB
  -cache            Use Cache file if available
  -no-cache         Do not Use Cache file if available (DEFAULT)
  -refresh          Force Refresh Cache
  -no-refresh       Do not Force Refresh Cache (DEFAULT)
  -inventory        Use Inventory if exist (DEFAULT)
  -no-inventory     Do not Use Inventory if exist
  -s3select         Use S3 Select to parse inventory result files
                    (EXPERIMENTAL)
  -no-s3select      Do not Use S3 Select to parse inventory result files
                    (EXPERIMENTAL) (DEFAULT)
```
You can then try the commandline as follow;

Get statistics for all buckets which name start with "a" and reports sizes in GB
```
python3 getstats.py -l 'a.*' -s 3
```

Get statistics for all buckets which name start with "mybucket" , key prefix /Folder/SubFolder/log* and reports sizes in GB
(e.g.: s3://mybucket/Folder/SubFolder/log*)
```
python3 getstats.py -l 'mybucket' -k '/Folder/SubFolder/log' -s 3
```

```
Give an example
```

## Built With

* [Python](https://www.python.org/) - The Python programming language
* [boto3](https://aws.amazon.com/sdk-for-python/) - AWS SDK for Python (Boto3)

## Authors

* **Andre Couture** - *Initial work* - [nomade1999](https://github.com/nomade1999)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code inspired me

