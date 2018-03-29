# -*- coding: utf-8 -*-

import logging
import boto3
# import xformch2grem

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    logger.info('processing event{}'.format(event))
    s3 = boto3.client('s3')

    # retrieve bucket name and file_key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    logger.info('Reading {} from {}'.format(file_key, bucket_name))
    # get the object
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    # get lines inside the csv here if needed

    # xformch2grem.main()
