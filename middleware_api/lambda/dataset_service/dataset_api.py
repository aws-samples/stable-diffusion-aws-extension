import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List

from common.ddb_service.client import DynamoDbUtilsService
from _types import DatasetItem, DatasetInfo, DatasetStatus, DataStatus
from common.util import get_s3_presign_urls, generate_presign_url
from multi_users.utils import get_permissions_by_username, get_user_roles, check_user_permissions

dataset_item_table = os.environ.get('DATASET_ITEM_TABLE')
dataset_info_table = os.environ.get('DATASET_INFO_TABLE')
bucket_name = os.environ.get('S3_BUCKET')
user_table = os.environ.get('MULTI_USER_TABLE')

logger = logging.getLogger('boto3')
ddb_service = DynamoDbUtilsService(logger=logger)


@dataclass
class DataUploadEvent:
    filename: str
    name: str
    type: str
    params: dict[str, Any]


@dataclass
class DatasetCreateEvent:
    dataset_name: str
    content: List[DataUploadEvent]
    params: dict[str, Any]
    creator: str

    def get_filenames(self):
        return [f.filename for f in self.content]

    def __post_init__(self):
        parsed_arr = []
        for entry in self.content:
            parsed_arr.append(DataUploadEvent(**entry))

        self.content = parsed_arr


# POST /dataset
def create_dataset_api(raw_event, context):
    event = DatasetCreateEvent(**raw_event)

    try:
        creator_permissions = get_permissions_by_username(ddb_service, user_table, event.creator)
        if 'train' not in creator_permissions \
                or ('all' not in creator_permissions['train'] and 'create' not in creator_permissions['train']):
            return {
                'statusCode': 400,
                'errMsg': f'user {event.creator} has not permission to create a train job'
            }

        user_roles = get_user_roles(ddb_service, user_table, event.creator)
        timestamp = datetime.now().timestamp()
        new_dataset_info = DatasetInfo(
            dataset_name=event.dataset_name,
            timestamp=timestamp,
            dataset_status=DatasetStatus.Initialed,
            params=event.params,
            allowed_roles_or_users=user_roles,
        )

        presign_url_map = get_s3_presign_urls(
            bucket_name=bucket_name,
            base_key=new_dataset_info.get_s3_key(),
            filenames=event.get_filenames()
        )
        dataset = []
        for f in event.content:
            params = f.params
            if not params or len(params) == 0:
                params = {}

            params['original_file_name'] = f.filename
            dataset.append(DatasetItem(
                dataset_name=new_dataset_info.dataset_name,
                sort_key=f'{timestamp}_{f.name}',
                name=f.name,
                type=f.type,
                data_status=DataStatus.Initialed,
                params=params,
                allowed_roles_or_users=user_roles
            ).__dict__)

        ddb_service.batch_put_items({
            dataset_item_table: dataset,
            dataset_info_table: [new_dataset_info.__dict__]
        })
        return {
            'statusCode': 200,
            'datasetName': new_dataset_info.dataset_name,
            's3PresignUrl': presign_url_map
        }
    except Exception as e:
        logger.error(e)
        return {
            'statusCode': 500,
            'error': str(e)
        }


# GET /datasets
def list_datasets_api(event, context):
    _filter = {}
    if 'queryStringParameters' not in event:
        return {
            'statusCode': 500,
            'error': 'query parameter status and types are needed'
        }

    parameters = event['queryStringParameters']
    if 'dataset_status' in parameters and len(parameters['dataset_status']) > 0:
        _filter['dataset_status'] = parameters['dataset_status']

    if 'x-auth' not in event or not event['x-auth']['username']:
        return {
            'statusCode': 400,
            'error': 'no auth user provided'
        }

    requestor_name = event['x-auth']['username']
    requestor_permissions = get_permissions_by_username(ddb_service, user_table, requestor_name)
    requestor_roles = get_user_roles(ddb_service=ddb_service, user_table_name=user_table, username=requestor_name)
    if 'train' not in requestor_permissions or \
            ('all' not in requestor_permissions['train'] and 'list' not in requestor_permissions['train']):
        return {
            'statusCode': 400,
            'error': f'user has no permission to train'
        }

    resp = ddb_service.scan(table=dataset_info_table, filters=_filter)
    if not resp or len(resp) == 0:
        return {
            'statusCode': 200,
            'datasets': []
        }

    datasets = []
    for tr in resp:
        dataset_info = DatasetInfo(**(ddb_service.deserialize(tr)))
        dataset_info_dto = {
            'datasetName': dataset_info.dataset_name,
            's3': f's3://{bucket_name}/{dataset_info.get_s3_key()}',
            'status': dataset_info.dataset_status.value,
            'timestamp': dataset_info.timestamp,
            **dataset_info.params
        }

        if dataset_info.allowed_roles_or_users \
                and check_user_permissions(dataset_info.allowed_roles_or_users, requestor_roles, requestor_name):
            datasets.append(dataset_info_dto)
        elif not dataset_info.allowed_roles_or_users and \
                'user' in requestor_permissions and \
                'all' in requestor_permissions['user']:
            # superuser can view the legacy data
            datasets.append(dataset_info_dto)

    return {
        'statusCode': 200,
        'datasets': datasets
    }


# GET /dataset/{dataset_name}/data
def list_data_by_dataset(event, context):
    _filter = {}
    if 'pathStringParameters' not in event:
        return {
            'statusCode': 500,
            'error': 'path parameter /dataset/{dataset_name}/ are needed'
        }

    dataset_name = event['pathStringParameters']['dataset_name']
    if not dataset_name or len(dataset_name) == 0:
        return {
            'statusCode': 500,
            'error': 'path parameter /dataset/{dataset_name}/ are needed'
        }

    dataset_info_rows = ddb_service.get_item(table=dataset_info_table, key_values={
        'dataset_name': dataset_name
    })

    if not dataset_info_rows or len(dataset_info_rows) == 0:
        return {
            'statusCode': 500,
            'error': 'path parameter /dataset/{dataset_name}/ are not found'
        }

    dataset_info = DatasetInfo(**dataset_info_rows)

    if 'x-auth' not in event or not event['x-auth']['username']:
        return {
            'statusCode': 400,
            'error': 'no auth user provided'
        }

    requestor_name = event['x-auth']['username']
    requestor_permissions = get_permissions_by_username(ddb_service, user_table, requestor_name)
    requestor_roles = get_user_roles(ddb_service=ddb_service, user_table_name=user_table, username=requestor_name)

    if not (
            (dataset_info.allowed_roles_or_users and check_user_permissions(dataset_info.allowed_roles_or_users, requestor_roles, requestor_name)) or # permission in dataset
            (not dataset_info.allowed_roles_or_users and 'user' in requestor_permissions and 'all' in requestor_permissions['user']) # legacy data for super admin
    ):
        return {
            'statusCode': 400,
            'error': 'no permission to view dataset'
        }

    rows = ddb_service.query_items(table=dataset_item_table, key_values={
        'dataset_name': dataset_name
    })

    resp = []
    for row in rows:
        item = DatasetItem(**ddb_service.deserialize(row))
        resp.append({
            'key': item.sort_key,
            'name': item.name,
            'type': item.type,
            'preview_url': generate_presign_url(bucket_name, item.get_s3_key(), expires=3600 * 24, method='get_object'),
            'dataStatus': item.data_status.value,
            **item.params
        })

    return {
        'statusCode': 200,
        'dataset_name': dataset_name,
        'datasetName': dataset_info.dataset_name,
        's3': f's3://{bucket_name}/{dataset_info.get_s3_key()}',
        'status': dataset_info.dataset_status.value,
        'timestamp': dataset_info.timestamp,
        'data': resp,
        **dataset_info.params
    }


@dataclass
class UpdateDatasetStatusEvent:
    dataset_name: str
    status: str


# PUT /dataset
def update_dataset_status(raw_event, context):
    event = UpdateDatasetStatusEvent(**raw_event)
    try:
        raw_dataset_info = ddb_service.get_item(table=dataset_info_table, key_values={
            'dataset_name': event.dataset_name
        })
        if not raw_dataset_info or len(raw_dataset_info) == 0:
            return {
                'statusCode': 404,
                'errorMsg': f'dataset {event.dataset_name} is not found'
            }

        dataset_info = DatasetInfo(**raw_dataset_info)
        new_status = DatasetStatus[event.status]
        dataset_info.dataset_status = new_status
        ddb_service.update_item(table=dataset_info_table,
                                key={'dataset_name': dataset_info.dataset_name},
                                field_name='dataset_status', value=new_status.value
                                )
        dataset_items = ddb_service.query_items(table=dataset_item_table, key_values={
            'dataset_name': dataset_info.dataset_name,
        })

        updates_items = []
        for row in dataset_items:
            item = DatasetItem(**ddb_service.deserialize(row))
            item.data_status = DataStatus[event.status]
            updates_items.append(item.__dict__)

        ddb_service.batch_put_items(table_items={
            dataset_item_table: updates_items
        })
        return {
            'statusCode': 200,
            'datasetName': dataset_info.dataset_name,
            'status': dataset_info.dataset_status.value,
        }

    except Exception as e:
        logger.error(e)
        return {
            'statusCode': 500,
            'error': str(e)
        }
