import base64
import json
import logging
import os
import time

import sagemaker

from common.ddb_service.client import DynamoDbUtilsService
from common.response import ok, not_found, internal_server_error
from common.stepfunction_service.client import StepFunctionUtilsService
from libs.data_types import TrainJob, TrainJobStatus, Model, CheckPoint
from libs.common_tools import DecimalEncoder

train_table = os.environ.get('TRAIN_TABLE')
model_table = os.environ.get('MODEL_TABLE')
checkpoint_table = os.environ.get('CHECKPOINT_TABLE')
instance_type = os.environ.get('INSTANCE_TYPE')
sagemaker_role_arn = os.environ.get('TRAIN_JOB_ROLE')
# e.g. "648149843064.dkr.ecr.us-east-1.amazonaws.com/dreambooth-training-repo"
image_uri = os.environ.get('TRAIN_ECR_URL')
training_stepfunction_arn = os.environ.get('TRAINING_SAGEMAKER_ARN')
logger = logging.getLogger('boto3')
logger.setLevel(logging.INFO)
ddb_service = DynamoDbUtilsService(logger=logger)


# PUT /train used to kickoff a train job step function
def handler(event, context):
    logger.info(json.dumps(event))
    train_job_id = event['pathParameters']['id']
    body = json.loads(event['body'])
    if body['status'] == TrainJobStatus.Training.value:
        return _start_train_job(train_job_id)

    return ok(message=f'not implemented for train job status {body["status"]}')


def _start_train_job(train_job_id: str):
    raw_train_job = ddb_service.get_item(table=train_table, key_values={
        'id': train_job_id
    })
    if raw_train_job is None or len(raw_train_job) == 0:
        return not_found(message=f'no such train job with id({train_job_id})')

    train_job = TrainJob(**raw_train_job)

    model_raw = ddb_service.get_item(table=model_table, key_values={
        'id': train_job.model_id
    })
    if model_raw is None:
        return not_found(message=f'model with id {train_job.model_id} is not found')

    model = Model(**model_raw)

    raw_checkpoint = ddb_service.get_item(table=checkpoint_table, key_values={
        'id': train_job.checkpoint_id
    })
    if raw_checkpoint is None:
        return not_found(message=f'checkpoint with id {train_job.checkpoint_id} is not found')

    checkpoint = CheckPoint(**raw_checkpoint)

    try:
        # JSON encode hyperparameters
        def json_encode_hyperparameters(hyperparameters):
            new_params = {}
            for k, v in hyperparameters.items():
                json_v = json.dumps(v, cls=DecimalEncoder)
                v_bytes = json_v.encode('ascii')
                base64_bytes = base64.b64encode(v_bytes)
                base64_v = base64_bytes.decode('ascii')
                new_params[k] = base64_v
            return new_params

        hyperparameters = json_encode_hyperparameters({
            "sagemaker_program": "extensions/sd-webui-sagemaker/sagemaker_entrypoint_json.py",
            "params": train_job.params,
            "s3-input-path": train_job.input_s3_location,
            "s3-output-path": checkpoint.s3_location,
        })

        final_instance_type = instance_type
        if 'training_params' in train_job.params \
                and 'training_instance_type' in train_job.params['training_params'] and \
                train_job.params['training_params']['training_instance_type']:
            final_instance_type = train_job.params['training_params']['training_instance_type']

        est = sagemaker.estimator.Estimator(
            image_uri,
            sagemaker_role_arn,
            instance_count=1,
            instance_type=final_instance_type,
            volume_size=125,
            base_job_name=f'{model.name}',
            hyperparameters=hyperparameters,
            job_id=train_job.id,
        )
        est.fit(wait=False)

        while not est._current_job_name:
            time.sleep(1)

        train_job.sagemaker_train_name = est._current_job_name
        # trigger stepfunction
        stepfunctions_client = StepFunctionUtilsService(logger=logger)
        sfn_input = {
            'train_job_id': train_job.id,
            'train_job_name': train_job.sagemaker_train_name
        }
        sfn_arn = stepfunctions_client.invoke_step_function(training_stepfunction_arn, sfn_input)
        # todo: use batch update, this is ugly!!!
        search_key = {'id': train_job.id}
        ddb_service.update_item(
            table=train_table,
            key=search_key,
            field_name='sagemaker_train_name',
            value=est._current_job_name
        )
        train_job.job_status = TrainJobStatus.Training
        ddb_service.update_item(
            table=train_table,
            key=search_key,
            field_name='job_status',
            value=TrainJobStatus.Training.value
        )
        train_job.sagemaker_sfn_arn = sfn_arn
        ddb_service.update_item(
            table=train_table,
            key=search_key,
            field_name='sagemaker_sfn_arn',
            value=sfn_arn
        )

        data = {
            'job': {
                'id': train_job.id,
                'status': train_job.job_status.value,
                'created': train_job.timestamp,
                'trainType': train_job.train_type,
                'params': train_job.params,
                'input_location': train_job.input_s3_location
            },
        }

        return ok(data=data, decimal=True)
    except Exception as e:
        print(e)
        return internal_server_error(message=str(e))
