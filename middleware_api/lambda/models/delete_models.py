import json
import logging
import os
from dataclasses import dataclass

import boto3

from common.response import ok

logger = logging.getLogger('delete_models')
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
models_table = dynamodb.Table(os.environ.get('MODELS_TABLE'))

s3_bucket_name = os.environ.get('S3_BUCKET_NAME')
s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_bucket_name)


@dataclass
class DeleteModelsEvent:
    model_id_list: [str]


def handler(event, ctx):
    logger.info(f'event: {event}')
    logger.info(f'ctx: {ctx}')

    body = DeleteModelsEvent(**json.loads(event['body']))

    # unique list for preventing duplicate delete
    model_id_list = list(set(body.model_id_list))

    for model_id in model_id_list:

        model = models_table.get_item(Key={'id': model_id})

        if 'Item' not in model:
            continue

        model = model['Item']

        logger.info(f'model: {model}')

        if 'output_s3_location' in model:
            output_s3_location = model['output_s3_location']
            prefix = output_s3_location.replace(f"s3://{s3_bucket_name}/", "")
            logger.info(f'delete prefix: {prefix}')

            response = bucket.objects.filter(Prefix=prefix).delete()
            logger.info(f'delete response: {response}')

        models_table.delete_item(Key={'id': model_id})

    return ok(message='models deleted')
