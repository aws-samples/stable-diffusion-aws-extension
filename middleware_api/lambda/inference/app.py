import time
import logging
import logging.config
import os
import traceback
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exception_handlers import http_exception_handler
from mangum import Mangum
from common.response_wrapper import resp_err
from common.enum import MessageEnum
from common.constant import const
from common.exception_handler import biz_exception
from parse.parameter_parser import json_convert_to_payload
from fastapi_pagination import add_pagination
from datetime import datetime
from typing import List

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
import json
import uuid

from sagemaker.predictor import Predictor
from sagemaker.predictor_async import AsyncPredictor
from sagemaker.serializers import JSONSerializer
from sagemaker.deserializers import JSONDeserializer
from boto3.dynamodb.conditions import Attr, Key

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(const.LOGGER_API)
STEP_FUNCTION_ARN = os.environ.get('STEP_FUNCTION_ARN')

DDB_INFERENCE_TABLE_NAME = os.environ.get('DDB_INFERENCE_TABLE_NAME')
DDB_TRAINING_TABLE_NAME = os.environ.get('DDB_TRAINING_TABLE_NAME')
DDB_ENDPOINT_DEPLOYMENT_TABLE_NAME = os.environ.get('DDB_ENDPOINT_DEPLOYMENT_TABLE_NAME')
REGION_NAME = os.environ['AWS_REGION']
S3_BUCKET_NAME = os.environ.get('S3_BUCKET')

ddb_client = boto3.resource('dynamodb')
s3 = boto3.client('s3', region_name=REGION_NAME)
sagemaker = boto3.client('sagemaker')
inference_table = ddb_client.Table(DDB_INFERENCE_TABLE_NAME)
endpoint_deployment_table = ddb_client.Table(DDB_ENDPOINT_DEPLOYMENT_TABLE_NAME)

async def custom_exception_handler(request: Request, exc: HTTPException):
    headers = {
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
    }
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=headers
    )

app = FastAPI(
    title="API List of SageMaker Inference",
    version="0.9",
)
app.exception_handler(HTTPException)(custom_exception_handler)

def get_uuid():
    uuid_str = str(uuid.uuid4())
    return uuid_str

def getInferenceJobList():
    response = inference_table.scan()
    logger.info(f"inference job list response is {str(response)}")
    return response['Items']

def build_filter_expression(end_time, endpoint, start_time, status, task_type):
    filter_expression = None
    if status:
        filter_expression = Attr('status').eq(status)
    if task_type:
        if filter_expression:
            filter_expression &= Attr('taskType').eq(task_type)
        else:
            filter_expression = Attr('taskType').eq(task_type)
    if start_time:
        if filter_expression:
            filter_expression &= Attr('startTime').gte(start_time)
        else:
            filter_expression = Attr('startTime').gte(start_time)
    if end_time:
        if filter_expression:
            filter_expression &= Attr('startTime').lte(end_time)
        else:
            filter_expression = Attr('startTime').lte(end_time)
    if endpoint:
        if filter_expression:
            filter_expression &= Attr('params.sagemaker_inference_endpoint_name').eq(endpoint)
        else:
            filter_expression = Attr('params.sagemaker_inference_endpoint_name').eq(endpoint)
    return filter_expression


def query_inference_job_list(status: str, task_type: str, start_time: str, end_time: str,
                             endpoint: str, checkpoint: str, limit: int):
    print(f"query_inference_job_list params are:{status},{task_type},{start_time},{end_time},{checkpoint},{endpoint}")
    try:
        response = None
        filter_expression = build_filter_expression(end_time, endpoint, start_time, status, task_type)
        if limit != const.PAGE_LIMIT_ALL and limit <= 0:
            logger.info(f"query inference job list error because of limit <0 {limit}")
            return ""
        if filter_expression:
            response = inference_table.scan(
                FilterExpression=filter_expression
            )
        else:
            response = inference_table.scan()
        logger.info(f"query inference job list response is {str(response)}")
        if response:
            return filter_checkpoint_items(limit, checkpoint, response['Items'])
        return response
    except Exception as e:
        logger.info(f"query inference job list error ")
        logger.info(e)
        return ""


def sort_by_start_time(item):
    return item.get("startTime", "")


def filter_checkpoint_items(limit, checkpoint, items):
    items = sorted(items, key=sort_by_start_time, reverse=True)
    if checkpoint:
        filtered_data = []
        for item in items:
            if "params" in item and "used_models" in item["params"]:
                used_models = item["params"]["used_models"].get("Stable-diffusion", [])
                for model in used_models:
                    if "model_name" in model and model["model_name"] == checkpoint:
                        filtered_data.append(item)
        if limit == const.PAGE_LIMIT_ALL:
            return filtered_data
        else:
            if len(filtered_data) >= limit:
                return filtered_data[0: limit]
            else:
                return filtered_data
    if limit == const.PAGE_LIMIT_ALL:
        return items
    else:
        if len(items) >= limit:
            return items[0: limit]
        else:
            return items


def getInferenceJob(inference_job_id):
    if not inference_job_id:
        logger.error("Invalid inference job id")
        raise ValueError("Inference job id must not be None or empty")

    try:
        resp = inference_table.query(
            KeyConditionExpression=Key('InferenceJobId').eq(inference_job_id)
        )
        # logger.info(resp)
        record_list = resp['Items']
        if len(record_list) == 0:
            logger.error(f"No inference job info item for id: {inference_job_id}")
            raise ValueError(f"There is no inference job info item for id: {inference_job_id}")
        return record_list[0]
    except Exception as e:
        logger.error(f"Exception occurred when trying to query inference job with id: {inference_job_id}, exception is {str(e)}")
        raise

def getEndpointDeploymentJobList():
    try:
        sagemaker = boto3.client('sagemaker')
        ddb = boto3.resource('dynamodb')
        endpoint_deployment_table = ddb.Table(DDB_ENDPOINT_DEPLOYMENT_TABLE_NAME)

        response = endpoint_deployment_table.scan()
        logger.info(f"endpoint deployment job list response is {str(response)}")

        # Get the list of SageMaker endpoints
        list_results = sagemaker.list_endpoints()
        sagemaker_endpoints = [ep_info['EndpointName'] for ep_info in list_results['Endpoints']]
        logger.info(str(sagemaker_endpoints))

        # Filter the endpoint job list
        filtered_endpoint_jobs = []
        for job in response['Items']:
            if 'endpoint_name' in job:
                endpoint_name = job['endpoint_name']
                deployment_job_id = job['EndpointDeploymentJobId']

                if endpoint_name in sagemaker_endpoints:
                    filtered_endpoint_jobs.append(job)
                else:
                    # Remove the job item from the DynamoDB table if the endpoint doesn't exist in SageMaker
                    endpoint_deployment_table.delete_item(Key={'EndpointDeploymentJobId': deployment_job_id})
            else:
                filtered_endpoint_jobs.append(job)

        return filtered_endpoint_jobs

    except ClientError as e:
        print(f"An error occurred: {e}")
        return []

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

def getEndpointDeployJob(endpoint_deploy_job_id):
    try:
        resp = endpoint_deployment_table.query(
            KeyConditionExpression=Key('EndpointDeploymentJobId').eq(endpoint_deploy_job_id)
        )
        logger.info(resp)
    except Exception as e:
        logger.error(e)
    record_list = resp['Items']
    if len(record_list) == 0:
        logger.error("There is no endpoint deployment job info item for id:" + endpoint_deploy_job_id)
        return {}
    return record_list[0]

def getEndpointDeployJob_with_endpoint_name(endpoint_name):
    try:
        resp = endpoint_deployment_table.scan(
            FilterExpression=Attr('endpoint_name').eq(endpoint_name)
        )
        logger.info(resp)
    except Exception as e:
        logger.error(e)

    record_list = resp['Items']
    if len(record_list) == 0:
        logger.error("There is no endpoint deployment job info item with endpoint name:" + endpoint_name)
        return {}

    return record_list[0]

def get_s3_objects(bucket_name, folder_name):
    # Ensure the folder name ends with a slash
    if not folder_name.endswith('/'):
        folder_name += '/'

    # List objects in the specified bucket and folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    # Extract object names from the response
    object_names = [obj['Key'][len(folder_name):] for obj in response.get('Contents', []) if obj['Key'] != folder_name]

    return object_names

def load_json_from_s3(bucket_name, key):

    # Get the JSON file from the specified bucket and key
    response = s3.get_object(Bucket=bucket_name, Key=key)
    json_file = response['Body'].read().decode('utf-8')

    # Load the JSON file into a dictionary
    data = json.loads(json_file)

    return data

# Global exception capture
stepf_client = boto3.client('stepfunctions')

@app.get("/inference")
def root():
    return {"message": const.SOLUTION_NAME}

# def get_curent_time():
#     # Get the current time
#     now = datetime.now()
#     formatted_time = now.strftime("%Y-%m-%d-%H-%M-%S")
#     return formatted_time

@app.post("/inference/run-sagemaker-inference")
@app.post("/inference-api/inference")
async def run_sagemaker_inference(request: Request):
    try:
        logger.info('entering the run_sage_maker_inference function!')

        inference_id = get_uuid()

        payload_checkpoint_info = await request.json()
        print(f"!!!!!!!!!!input in json format {payload_checkpoint_info}")
        task_type = payload_checkpoint_info.get('task_type')
        print(f"Task Type: {task_type}")
        path = request.url.path
        logger.info(f'Path: {path}')
        if path == '/inference-api/inference':
            # Invoke by API
            logger.info('invoked by api')
            params_dict = load_json_from_s3(S3_BUCKET_NAME, 'template/inferenceTemplate.json')
        else:
            # Invoke by UI
            params_dict = load_json_from_s3(S3_BUCKET_NAME, 'config/aigc.json')
        # logger.info(json.dumps(params_dict))
        payload = json_convert_to_payload(params_dict, payload_checkpoint_info, task_type)
        print(f"input in json format:")
        checkpoint_name = None
        if task_type == 'img2img':
            checkpoint_name = params_dict['img2img_sagemaker_stable_diffusion_checkpoint']
        elif task_type == 'txt2img':
            checkpoint_name = params_dict['txt2img_sagemaker_stable_diffusion_checkpoint']
        def show_slim_dict(payload):
            pay_type = type(payload)
            if pay_type is dict:
                for k, v in payload.items():
                    print(f"{k}")
                    show_slim_dict(v)
            elif pay_type is list:
                for v in payload:
                    print(f"list")
                    show_slim_dict(v)
            elif pay_type is str:
                if len(payload) > 100:
                    print(f" : {len(payload)} contents")
                else:
                    print(f" : {payload}")
            else:
                print(f" : {payload}")

        show_slim_dict(payload)

        endpoint_name = payload["endpoint_name"]

        predictor = Predictor(endpoint_name)

        # adjust time out time to 1 hour
        initial_args = {"InvocationTimeoutSeconds": 3600}

        predictor = AsyncPredictor(predictor, name=endpoint_name)
        predictor.serializer = JSONSerializer()
        predictor.deserializer = JSONDeserializer()
        prediction = predictor.predict_async(data=payload, initial_args=initial_args, inference_id=inference_id)
        output_path = prediction.output_path

        #put the item to inference DDB for later check status
        current_time = str(datetime.now())
        response = inference_table.put_item(
            Item={
                'InferenceJobId': inference_id,
                'startTime': current_time,
                'status': 'inprogress',
                'endpoint': endpoint_name,
                'checkpoint': checkpoint_name,
                'taskType': task_type
            })
        print(f"output_path is {output_path}")

        headers = {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        }

        response = JSONResponse(content={"inference_id": inference_id, "status": "inprogress", "endpoint_name": endpoint_name, "output_path": output_path}, headers=headers)
        return response

    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error occurred: {str(e)}")

        # raise HTTPException(status_code=500, detail=f"An error occurred during processing.{str(e)}")
        headers = {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        }

        current_time = str(datetime.now())
        response = inference_table.put_item(
            Item={
                'InferenceJobId': inference_id,
                'startTime': current_time,
                'completeTime': current_time,
                'status': 'failure',
                'endpoint': endpoint_name,
                'checkpoint': checkpoint_name,
                'taskType': task_type or "unknown",
                'error': f"error info {str(e)}"}
            )

        response = JSONResponse(content={"inference_id": inference_id, "status":"failure", "error": f"error info {str(e)}"}, headers=headers)
        return response


@app.post("/inference/deploy-sagemaker-endpoint")
async def deploy_sagemaker_endpoint(request: Request):
    logger.info("entering the deploy_sagemaker_endpoint function!")
    endpoint_deployment_id = get_uuid()
    try:
        payload = await request.json()
        logger.info(f"input in json format {payload}")
        payload['endpoint_deployment_id'] = endpoint_deployment_id

        # put the item to inference DDB for later check status
        # must insert item first
        current_time = str(datetime.now())
        response = endpoint_deployment_table.put_item(
            Item={
                'EndpointDeploymentJobId': endpoint_deployment_id,
                'startTime': current_time,
                'endpoint_status': 'Creating',
                'max_instance_number': payload['initial_instance_count'],
                'autoscaling': payload['autoscaling_enabled'],
                'owner_group_or_role': payload['assign_to_roles']
            })

        resp = stepf_client.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            input=json.dumps(payload)
        )

        logger.info("trigger step-function with following response")

        logger.info(f"finish trigger step function for deployment with output {resp}")
        return 0
    except Exception as e:
        logger.error(f"error calling run-sagemaker-inference with exception: {e}")
        #put the item to inference DDB for later check status
        current_time = str(datetime.now())
        response = endpoint_deployment_table.put_item(
        Item={
            'EndpointDeploymentJobId': endpoint_deployment_id,
            'startTime': current_time,
            'status': 'failed',
            'completeTime': current_time,
            'error': str(e)
        })
        return 0


@app.get("/inference/list-endpoint-deployment-jobs")
async def list_endpoint_deployment_jobs():
    logger.info(f"entering list_endpoint_deployment_jobs")
    return getEndpointDeploymentJobList()

@app.get("/inference/list-inference-jobs")
async def list_inference_jobs():
    logger.info(f"entering list_endpoint_deployment_jobs")
    return getInferenceJobList()


@app.post("/inference/query-inference-jobs")
async def query_inference_jobs(request: Request):
    logger.info(f"entering query-inference-jobs")
    query_params = await request.json()
    logger.info(query_params)
    status = query_params.get('status')
    task_type = query_params.get('task_type')
    start_time = query_params.get('start_time')
    end_time = query_params.get('end_time')
    endpoint = query_params.get('endpoint')
    checkpoint = query_params.get('checkpoint')
    limit = query_params.get("limit") if query_params.get("limit") else const.PAGE_LIMIT_ALL
    logger.info(f"entering query-inference-jobs {status},{task_type},{start_time},{end_time},{checkpoint},{endpoint},{limit}")
    return query_inference_job_list(status, task_type, start_time, end_time, endpoint, checkpoint, limit)


@app.get("/inference/get-endpoint-deployment-job")
async def get_endpoint_deployment_job(jobID: str = None):
    logger.info(f"entering get_endpoint_deployment_job function ")
    # endpoint_deployment_jobId = request.query_params
    endpoint_deployment_jobId = jobID
    logger.info(f"endpoint_deployment_jobId is {str(endpoint_deployment_jobId)}")
    return getEndpointDeployJob(endpoint_deployment_jobId)

@app.get("/inference/get-inference-job")
async def get_inference_job(jobID: str = None):
    inference_jobId = jobID
    # logger.info(f"entering get_inference_job function with jobId: {inference_jobId}")
    try:
        return getInferenceJob(inference_jobId)
    except Exception as e:
        # logger.error(f"Error getting inference job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/inference/get-inference-job-image-output")
async def get_inference_job_image_output(jobID: str = None) -> List[str]:
    inference_jobId = jobID

    if inference_jobId is None or inference_jobId.strip() == "":
        logger.info(f"jobId is empty string or None, just return empty string list")
        return []

    logger.info(f"Entering get_inference_job_image_output function with jobId: {inference_jobId}")

    try:
        job_record = getInferenceJob(inference_jobId)
    except Exception as e:
        logger.error(f"Error getting inference job: {str(e)}")
        return []

    # Assuming the job_record contains a list of image names
    image_names = job_record.get("image_names", [])

    presigned_urls = []

    for image_name in image_names:
        try:
            image_key = f"out/{inference_jobId}/result/{image_name}"
            presigned_url = generate_presigned_url(S3_BUCKET_NAME, image_key)
            presigned_urls.append(presigned_url)
        except Exception as e:
            logger.error(f"Error generating presigned URL for image {image_name}: {str(e)}")
            # Continue with the next image if this one fails
            continue

    return presigned_urls

@app.get("/inference/get-inference-job-param-output")
async def get_inference_job_param_output(jobID: str = None) -> List[str]:
    inference_jobId = jobID

    if inference_jobId is None or inference_jobId.strip() == "":
        logger.info(f"jobId is empty string or None, just return empty string list")
        return []

    logger.info(f"Entering get_inference_job_param_output function with jobId: {inference_jobId}")

    try:
        job_record = getInferenceJob(inference_jobId)
    except Exception as e:
        logger.error(f"Error getting inference job: {str(e)}")
        return []

    presigned_url = ""

    try:
        json_key = f"out/{inference_jobId}/result/{inference_jobId}_param.json"
        presigned_url = generate_presigned_url(S3_BUCKET_NAME, json_key)
    except Exception as e:
        logger.error(f"Error generating presigned URL: {str(e)}")
        return []

    return [presigned_url]

def generate_presigned_url(bucket_name: str, key: str, expiration=3600) -> str:
    try:
        response = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': key},
            ExpiresIn=expiration
        )
    except Exception as e:
        logger.error(f"Error generating presigned URL: {e}")
        raise

    return response


@app.get("/inference/generate-s3-presigned-url-for-uploading")
async def generate_s3_presigned_url_for_uploading(s3_bucket_name: str = None, key: str = None):
    if not s3_bucket_name:
        s3_bucket_name = S3_BUCKET_NAME

    if not key:
        raise HTTPException(status_code=400, detail="Key parameter is required")

    try:
        presigned_url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': s3_bucket_name,
                'Key': key,
                'ContentType': 'text/plain;charset=UTF-8'
            },
            ExpiresIn=3600,
            HttpMethod='PUT'
        )
    except Exception as e:
        headers = {
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET,PUT"
        }
        return JSONResponse(content=str(e), status_code=500, headers=headers)

    headers = {
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET,PUT"
    }

    response = JSONResponse(content=presigned_url, headers=headers)

    return response

@app.get("/inference/get-texual-inversion-list")
async def get_texual_inversion_list():
    logger.info(f"entering get_texual_inversion_list()")
    return get_s3_objects(S3_BUCKET_NAME,'texual_inversion')

@app.get("/inference/get-lora-list")
async def get_lora_list():
    return get_s3_objects(S3_BUCKET_NAME,'lora')

@app.get("/inference/get-hypernetwork-list")
async def get_hypernetwork_list():
    return get_s3_objects(S3_BUCKET_NAME,'hypernetwork')

@app.get("/inference/get-controlnet-model-list")
async def get_controlnet_model_list():
    return get_s3_objects(S3_BUCKET_NAME,'controlnet')

@app.post("/inference/run-model-merge")
async def run_model_merge(request: Request):
    try:
        logger.info('entering the run_model_merge function!')

        # TODO: add logic for inference id
        merge_id = get_uuid()

        payload_checkpoint_info = await request.json()
        print(f"!!!!!!!!!!input in json format {payload_checkpoint_info}")

        params_dict = load_json_from_s3(S3_BUCKET_NAME, 'config/aigc.json')

        logger.info(json.dumps(params_dict))
        payload = json_convert_to_payload(params_dict, payload_checkpoint_info)
        print(f"input in json format {payload}")
        task_type = payload_checkpoint_info.get('task_type')
        endpoint_name = payload["endpoint_name"]
        checkpoint_name = None
        if task_type == 'img2img':
            checkpoint_name = params_dict['img2img_sagemaker_stable_diffusion_checkpoint']
        elif task_type == 'txt2img':
            checkpoint_name = params_dict['txt2img_sagemaker_stable_diffusion_checkpoint']
        predictor = Predictor(endpoint_name)

        predictor = AsyncPredictor(predictor, name=endpoint_name)
        predictor.serializer = JSONSerializer()
        predictor.deserializer = JSONDeserializer()
        prediction = predictor.predict_async(data=payload, inference_id=inference_id)
        output_path = prediction.output_path

        #put the item to inference DDB for later check status
        current_time = str(datetime.now())
        response = inference_table.put_item(
            Item={
                'InferenceJobId': inference_id,
                'startTime': current_time,
                'status': 'inprogress',
                'endpoint': endpoint_name,
                'checkpoint': checkpoint_name,
            })
        print(f"output_path is {output_path}")

        headers = {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        }

        response = JSONResponse(content={"inference_id": inference_id, "status": "inprogress", "endpoint_name": endpoint_name, "output_path": output_path}, headers=headers)
        #response = JSONResponse(content={"inference_id": '6fa743f0-cb7a-496f-8205-dbd67df08be2', "status": "succeed", "output_path": ""}, headers=headers)
        return response

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")

        # raise HTTPException(status_code=500, detail=f"An error occurred during processing.{str(e)}")
        headers = {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        }

        response = JSONResponse(content={"inference_id": inference_id, "status":"failure", "error": f"error info {str(e)}"}, headers=headers)
        return response



#app.include_router(search) TODO: adding sub router for future

handler = Mangum(app)
add_pagination(app)
