import json
import logging
import os

import boto3
from aws_lambda_powertools import Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

from libs.common_tools import DecimalEncoder
from libs.utils import response_error

client = boto3.client('apigateway')

tracer = Tracer()

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get('LOG_LEVEL') or logging.ERROR)

esd_version = os.environ.get("ESD_VERSION")

tags = [
    {
        "name": "Service",
        "description": "Service API"
    },
    {
        "name": "Roles",
        "description": "Manage Roles"
    },
    {
        "name": "Users",
        "description": "Manage Users"
    },
    {
        "name": "Endpoints",
        "description": "Manage Endpoints"
    },
    {
        "name": "Checkpoints",
        "description": "Manage Checkpoints"
    },
    {
        "name": "Inferences",
        "description": "Manage Inferences"
    },
    {
        "name": "Executes",
        "description": "Manage Executes"
    },
    {
        "name": "Datasets",
        "description": "Manage Datasets"
    },
    {
        "name": "Trainings",
        "description": "Manage Trainings"
    },
    {
        "name": "Others",
        "description": "Others API"
    },
]

summaries = {
    "PingResponse": {
        "summary": "Ping API",
        "tags": ["Service"]
    },
    "ListRoles": {
        "summary": "List Roles",
        "tags": ["Roles"]
    },
    "GetInferenceJob": {
        "summary": "Get Inference Job",
        "tags": ["Inferences"]
    },
    "CreateRole": {
        "summary": "Create Role",
        "tags": ["Roles"]
    },
    "DeleteRoles": {
        "summary": "Delete Roles",
        "tags": ["Roles"]
    },
    "GetTraining": {
        "summary": "Get Training",
        "tags": ["Trainings"]
    },
    "ListCheckpoints": {
        "summary": "List Checkpoints",
        "tags": ["Checkpoints"]
    },
    "CreateCheckpoint": {
        "summary": "Create Checkpoint",
        "tags": ["Checkpoints"]
    },
    "DeleteCheckpoints": {
        "summary": "Delete Checkpoints",
        "tags": ["Checkpoints"]
    },
    "StartInferences": {
        "summary": "Start Inference Job",
        "tags": ["Inferences"]
    },
    "ListExecutes": {
        "summary": "List Executes",
        "tags": ["Executes"]
    },
    "CreateExecute": {
        "summary": "Create Execute",
        "tags": ["Executes"]
    },
    "DeleteExecutes": {
        "summary": "Delete Executes",
        "tags": ["Executes"]
    },
    "GetApiOAS": {
        "summary": "Get OAS",
        "tags": ["Service"]
    },
    "ListUsers": {
        "summary": "List Users",
        "tags": ["Users"]
    },
    "CreateUser": {
        "summary": "Create User",
        "tags": ["Users"]
    },
    "DeleteUsers": {
        "summary": "Delete Users",
        "tags": ["Users"]
    },

    "ListTrainings": {
        "summary": "List Trainings",
        "tags": ["Trainings"]
    },
    "CreateTraining": {
        "summary": "Create Training",
        "tags": ["Trainings"]
    },
    "DeleteTrainings": {
        "summary": "Delete Trainings",
        "tags": ["Trainings"]
    },
    "GetExecute": {
        "summary": "Get Execute",
        "tags": ["Executes"]
    },
    "ListDatasets": {
        "summary": "List Datasets",
        "tags": ["Datasets"]
    },
    "UpdateCheckpoint": {
        "summary": "Update Checkpoint",
        "tags": ["Checkpoints"]
    },
    "CreateDataset": {
        "summary": "Create Dataset",
        "tags": ["Datasets"]
    },
    "DeleteDatasets": {
        "summary": "Delete Datasets",
        "tags": ["Datasets"]
    },
    "GetDataset": {
        "summary": "Get Dataset",
        "tags": ["Datasets"]
    },
    "UpdateDataset": {
        "summary": "Update Dataset",
        "tags": ["Datasets"]
    },
    "ListInferences": {
        "summary": "List Inferences",
        "tags": ["Inferences"]
    },
    "CreateInferenceJob": {
        "summary": "Create Inference Job",
        "tags": ["Inferences"]
    },
    "DeleteInferenceJobs": {
        "summary": "Delete Inference Jobs",
        "tags": ["Inferences"]
    },
    "ListEndpoints": {
        "summary": "List Endpoints",
        "tags": ["Endpoints"]
    },
    "CreateEndpoint": {
        "summary": "Create Endpoint",
        "tags": ["Endpoints"]
    },
    "DeleteEndpoints": {
        "summary": "Delete Endpoints",
        "tags": ["Endpoints"]
    },
}


@tracer.capture_lambda_handler
def handler(event: dict, context: LambdaContext):
    logger.info(f'event: {event}')
    logger.info(f'ctx: {context}')

    try:
        response = client.get_export(
            restApiId=event['requestContext']['apiId'],
            stageName='prod',
            exportType='oas30',
        )

        oas = response['body'].read()
        json_schema = json.loads(oas)
        json_schema = replace_null(json_schema)
        json_schema['info']['version'] = esd_version.split('-')[0]

        json_schema['servers'] = [
            {
                "url": "{ApiGatewayUrl}",
                "variables": {
                    "ApiGatewayUrl": {
                        "default": "https://xxxxxx.execute-api.ap-northeast-1.amazonaws.com/prod/"
                    }
                }
            }
        ]

        json_schema['tags'] = tags

        for path in json_schema['paths']:
            for method in json_schema['paths'][path]:
                meta = summary(json_schema['paths'][path][method])
                json_schema['paths'][path][method]['summary'] = meta["summary"]
                json_schema['paths'][path][method]['tags'] = meta["tags"]

        json_schema['paths'] = dict(sorted(json_schema['paths'].items(), key=lambda x: x[0]))

        payload = {
            'isBase64Encoded': False,
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-v': True,
            },
            'body': json.dumps(json_schema, cls=DecimalEncoder, indent=2)
        }

        return payload
    except Exception as e:

        return response_error(e)


def replace_null(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if value is None:
                data[key] = {
                    "type": "null",
                    "description": "Last Key for Pagination"
                }
            else:
                data[key] = replace_null(value)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if item is None:
                data[i] = {
                    "type": "null",
                    "description": "Last Key for Pagination"
                }
            else:
                data[i] = replace_null(item)
    return data


def summary(method: any):
    if 'operationId' in method:
        if method['operationId'] in summaries:
            item = summaries[method['operationId']]
            item["summary"] = item["summary"] + f" ({method['operationId']})"
            return item

        return {
            "summary": method['operationId'],
            "tags": ["Others"],
        }

    return {
        "summary": "",
        "tags": ["Others"],
    }
