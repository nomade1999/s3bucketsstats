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

Now it is possible to gather some more analytics and also inventory from AWS but this require to be configured and take about 24 hours to get the first results. That is not a viable solution when statistics are requested on the spot.

### Prerequisites

There are no installations required, just download the script and run. 

You will need to have python3 installed with the boto3 sdk. Most of the other import should be there by default

Here's the list of import;

```
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
```

You will also need to get you environment setup to access AWS CLI. 

```
Give examples
```

### Installing

Just setup your environement to connect to AWS and test it to make sure you have access to your S3 buckets.
If running on an EC2 instance I would suggest to use IAM Role.

## Running examples

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

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

