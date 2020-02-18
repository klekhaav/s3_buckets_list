import boto3
import csv

from botocore.exceptions import ClientError


""" Bucket list to csv file
Task: Create script that will collect data from AWS S3. 

Result of script saves as csv file with such number of columns for each bucket:
'Account ID', 'Account Aliases', 'S3 Bucket Name', 'S3 Bucket Region', 'Analytics'

Requirements:
boto3==1.12.0

AWS role requirement:
iam::listAccountAliases

Description: script collect data to output.csv file. Example:
Account ID,Account Aliases,S3 Bucket Name,S3 Bucket Region,Analytics
long account id,account aliases,bucket1,eu-central-1,Y
long account id,account aliases,bucket2,eu-central-1,N
long account id,account aliases,bucket3,eu-central-1,N

Parameters:
    -r, --aws_region - AWS region name
    -a, --aws_access_key - AWS access key id
    -s, --aws_secret_key - AWS secret key
    -t, --aws_security_token - AWS security token
    
Starting script command:
    python get_bucket_list.py -r region -a account_id -s secret_key


Tested on Python 3.6.9
@author klekhaav@gmail.com
"""


def get_client(client):
    """ AWS client interface. For different type of clients """
    try:
        return boto3.client(client,
                            region_name=opt.aws_region,
                            aws_access_key_id=opt.aws_access_key,
                            aws_secret_access_key=opt.aws_secret_key,
                            aws_session_token=opt.aws_security_token if 'aws_security_token' in opt else None)
    except ClientError as err:
        print("One of provided parameter for credentials is not valid: \n{}".format(err))


def get_iam_client():
    """ Getting IAM client """
    return get_client(u'iam')


def get_acc_aliases():
    """ For Accounts with available Account Aliases and
    with added role iam::listAccountAliases can return available aliases """
    aliases = []

    try:
        iam = get_iam_client()
        paginator = iam.get_paginator('list_account_aliases')
        for response in paginator.paginate():
            aliases.append(response['AccountAliases'])
        return ' '.join([str(alias) for alias in aliases])
    except ClientError:
        return "Not available"


def get_s3_client():
    """ Getting S3 client """
    return get_client(u's3')


def get_s3_bucket_region(bucket):
    """ Getting S3 Bucket Region.
    Might be different than account region """
    client = get_s3_client()
    return client.get_bucket_location(Bucket=bucket)['LocationConstraint']


def is_analytics_enabled(bucket):
    """ Checking for any Analytics are configured for bucket """
    client = get_s3_client()
    if u'AnalyticsConfigurationList' in client.list_bucket_analytics_configurations(Bucket=bucket):
        return True


def get_formated_buckets_list():
    """ Collecting available data and creating lists for each bucket name and requested resources """
    buckets_list = []
    try:
        buckets = boto3.resource(
            u's3',
            region_name=opt.aws_region,
            aws_access_key_id=opt.aws_access_key,
            aws_secret_access_key=opt.aws_secret_key,
            aws_session_token=opt.aws_security_token if 'aws_security_token' in opt else None).buckets.all()
        for bucket in buckets:
            buckets_list.append([bucket.Acl().owner['ID'],
                                 get_acc_aliases(),
                                 bucket.name,
                                 get_s3_bucket_region(bucket.name),
                                 'Y' if is_analytics_enabled(bucket.name) else 'N'])
        return buckets_list
    except ClientError as err:
        print("One of provided parameter for credentials is not valid: \n{}".format(err))


def main(opt):
    """ Main Function"""
    bucket_list = get_formated_buckets_list()
    if bucket_list:
        with open('output.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['Account ID', 'Account Aliases', 'S3 Bucket Name', 'S3 Bucket Region', 'Analytics'])
            for bucket_data in bucket_list:
                writer.writerow(bucket_data)


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('-r', '--aws_region', type=str, help="AWS region name", required=True)
    parser.add_argument('-a', '--aws_access_key', type=str, help="AWS access key id", required=True)
    parser.add_argument('-s', '--aws_secret_key', type=str, help="AWS secret key", required=True)
    parser.add_argument('-t', '--aws_security_token', type=str, help="AWS security token")

    opt = parser.parse_args()
    main(opt)