import json
import logging
import os
from dataclasses import dataclass

import boto3

from common.response import no_content

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get('LOG_LEVEL') or logging.ERROR)

dynamodb = boto3.resource('dynamodb')
checkpoints_table = dynamodb.Table(os.environ.get('CHECKPOINTS_TABLE'))

s3_bucket_name = os.environ.get('S3_BUCKET_NAME')
s3 = boto3.resource('s3')
bucket = s3.Bucket(s3_bucket_name)


@dataclass
class DeleteCheckpointsEvent:
    checkpoint_id_list: [str]


def handler(event, ctx):
    logger.info(f'event: {event}')
    logger.info(f'ctx: {ctx}')

    body = DeleteCheckpointsEvent(**json.loads(event['body']))

    # unique list for preventing duplicate delete
    checkpoint_id_list = list(set(body.checkpoint_id_list))

    for checkpoint_id in checkpoint_id_list:

        checkpoint = checkpoints_table.get_item(Key={'id': checkpoint_id})

        if 'Item' not in checkpoint:
            continue

        logger.info(f'checkpoint: {checkpoint}')

        prefix = checkpoint['Item']['s3_location'].replace(f"s3://{s3_bucket_name}/", "")
        logger.info(f'delete prefix: {prefix}')

        response = bucket.objects.filter(Prefix=prefix).delete()
        logger.info(f'delete response: {response}')

        checkpoints_table.delete_item(Key={'id': checkpoint_id})

    return no_content(message='checkpoints deleted')
