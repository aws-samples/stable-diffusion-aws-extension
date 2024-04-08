import logging

import boto3

import config

logger = logging.getLogger(__name__)
client = boto3.client('cloudformation')

template = "https://aws-gcr-solutions-us-east-1.s3.amazonaws.com/extension-for-stable-diffusion-on-aws/comfy.yaml"


class TestComfyClient:

    @classmethod
    def setup_class(self):
        pass

    @classmethod
    def teardown_class(self):
        pass

    def test_1_create_comfy_client_by_template(self):
        response = client.create_stack(
            StackName=config.comfy_stack,
            TemplateURL=template,
            Capabilities=['CAPABILITY_NAMED_IAM'],
            Parameters=[
                {
                    'ParameterKey': 'ApiGatewayUrl',
                    'ParameterValue': config.host_url
                },
                {
                    'ParameterKey': 'BucketName',
                    'ParameterValue': config.bucket
                },
                {
                    'ParameterKey': 'EndpointName',
                    'ParameterValue': "EndpointName"
                },
                {
                    'ParameterKey': 'ApiGatewayUrlToken',
                    'ParameterValue': config.api_key
                },
                {
                    'ParameterKey': 'InstanceType',
                    'ParameterValue': "g5.4xlarge"
                }
            ]
        )

        print(response)
