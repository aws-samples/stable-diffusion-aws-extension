import * as path from 'path';
import {
  StackProps,
  Duration,
  Aws,
  RemovalPolicy,
  aws_ecr,
  CustomResource,
  NestedStack,
} from 'aws-cdk-lib';
import * as apigw from 'aws-cdk-lib/aws-apigateway';

import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as eventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import { RetentionDays } from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as sns from 'aws-cdk-lib/aws-sns';
import { Construct } from 'constructs';
import { SagemakerInferenceStateMachine } from './sd-sagemaker-inference-state-machine';
import { DockerImageName, ECRDeployment } from '../cdk-ecr-deployment/lib';
import * as python from '@aws-cdk/aws-lambda-python-alpha';

/*
AWS CDK code to create API Gateway, Lambda and SageMaker inference endpoint for txt2img/img2img inference
based on Stable Diffusion. S3 is used to store large payloads and passed as object reference in the API Gateway
request and Lambda function to avoid request payload limitation
Note: Sync Inference is put here for reference, we use Async Inference now
*/
export interface SDAsyncInferenceStackProps extends StackProps {
  api_gate_way: apigw.RestApi;
  s3_bucket: s3.Bucket;
  training_table: dynamodb.Table;
  snsTopic: sns.Topic;
  default_endpoint_name: string;
}

export class SDAsyncInferenceStack extends NestedStack {
  constructor(
    scope: Construct,
    id: string,
    props: SDAsyncInferenceStackProps,
  ) {
    super(scope, id, props);
    const srcImg = 'public.ecr.aws/aws-gcr-solutions/stable-diffusion-aws-extension/aigc-webui-inference:v1.0.0';

    if (!props?.api_gate_way) {
      throw new Error('api_gate_way is required');
    }
    const restful_api = <apigw.RestApi>apigw.RestApi.fromRestApiAttributes(
      this,
      'ImportedRestApi',
      {
        restApiId: props.api_gate_way.restApiId,
        rootResourceId: props.api_gate_way.restApiRootResourceId,
      },
    );

    // create Dynamodb table to save the inference job data
    const sd_inference_job_table = new dynamodb.Table(
      this,
      'SD_Inference_job',
      {
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        removalPolicy: RemovalPolicy.DESTROY,
        partitionKey: {
          name: 'InferenceJobId',
          type: dynamodb.AttributeType.STRING,
        },
        pointInTimeRecovery: true,
      },
    );

    // create Dynamodb table to save the inference job data
    const sd_endpoint_deployment_job_table = new dynamodb.Table(
      this,
      'SD_endpoint_deployment_job',
      {
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        removalPolicy: RemovalPolicy.DESTROY,
        partitionKey: {
          name: 'EndpointDeploymentJobId',
          type: dynamodb.AttributeType.STRING,
        },
        pointInTimeRecovery: true,
      },
    );

    // Create an SNS topic to get async inference result
    const inference_result_topic = new sns.Topic(
      this,
      'SNS-Receive-SageMaker-inference-success',
    );

    const inference_result_error_topic = new sns.Topic(
      this,
      'SNS-Receive-SageMaker-inference-error',
    );

    const inferenceECR_url = this.createInferenceECR(srcImg);

    const stepFunctionStack = new SagemakerInferenceStateMachine(this, {
      snsTopic: inference_result_topic,
      snsErrorTopic: inference_result_error_topic,
      inferenceJobName: sd_inference_job_table.tableName,
      s3_bucket_name: props?.s3_bucket.bucketName ?? '',
      endpointDeploymentJobName:
                sd_endpoint_deployment_job_table.tableName,
      userNotifySNS:
                props?.snsTopic ??
                new sns.Topic(this, 'MyTopic', {
                  displayName: 'My SNS Topic',
                }),
      inference_ecr_url: inferenceECR_url,
    });

    const inferenceLambdaRole = new iam.Role(this, 'InferenceLambdaRole', {
      assumedBy: new iam.CompositePrincipal(
        new iam.ServicePrincipal('sagemaker.amazonaws.com'),
        new iam.ServicePrincipal('lambda.amazonaws.com')
      )
    });

    inferenceLambdaRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    // Create a Lambda function for inference
    const inferenceLambda = new lambda.DockerImageFunction(
      this,
      'InferenceLambda',
      {
        code: lambda.DockerImageCode.fromImageAsset(
          '../middleware_api/lambda/inference',
        ),
        timeout: Duration.minutes(15),
        memorySize: 3008,
        environment: {
          DDB_INFERENCE_TABLE_NAME: sd_inference_job_table.tableName,
          DDB_TRAINING_TABLE_NAME:
                        props?.training_table.tableName ?? '',
          DDB_ENDPOINT_DEPLOYMENT_TABLE_NAME:
                        sd_endpoint_deployment_job_table.tableName,
          S3_BUCKET: props?.s3_bucket.bucketName ?? '',
          ACCOUNT_ID: Aws.ACCOUNT_ID,
          REGION_NAME: Aws.REGION,
          SNS_INFERENCE_SUCCESS: inference_result_topic.topicName,
          SNS_INFERENCE_ERROR: inference_result_error_topic.topicName,
          STEP_FUNCTION_ARN: stepFunctionStack.stateMachineArn,
          NOTICE_SNS_TOPIC: props?.snsTopic.topicArn ?? '',
          INFERENCE_ECR_IMAGE_URL: inferenceECR_url,
          SAGEMAKER_ENDPOINT_NAME: props.default_endpoint_name,
        },
        role:inferenceLambdaRole,
        logRetention: RetentionDays.ONE_WEEK,
      },
    );

    // Grant Lambda permission to read/write from/to the S3 bucket
    props?.s3_bucket.grantReadWrite(inferenceLambda);

    // Grant Lambda permission to invoke SageMaker endpoint
    inferenceLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          'sagemaker:*',
          's3:Get*',
          's3:List*',
          's3:PutObject',
          's3:GetObject',
          's3:CreateBucket',
          'sns:*',
          'states:*',
          'dynamodb:*'
        ],
        resources: ['*'],
      }),
    );

    // Create a POST method for the API Gateway and connect it to the Lambda function
    const txt2imgIntegration = new apigw.LambdaIntegration(inferenceLambda);

    // Add a POST method with prefix inference
    const inference = restful_api?.root.addResource('inference');

    if (!restful_api) {
      throw new Error('resful_api is needed');
    }

    if (!inference) {
      throw new Error('inference is undefined');
    }
    inference.addMethod('POST', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    inference.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const run_sagemaker_inference = inference.addResource(
      'run-sagemaker-inference',
    );
    run_sagemaker_inference.addMethod('POST', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const deploy_sagemaker_endpoint = inference.addResource(
      'deploy-sagemaker-endpoint',
    );
    deploy_sagemaker_endpoint.addMethod('POST', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const list_endpoint_deployment_jobs = inference.addResource(
      'list-endpoint-deployment-jobs',
    );
    list_endpoint_deployment_jobs.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const delete_deployment_jobs = inference.addResource(
      'delete-sagemaker-endpoint');
      delete_deployment_jobs.addMethod('POST', txt2imgIntegration, {
        apiKeyRequired: true,
      }) 

    const list_inference_jobs = inference.addResource(
      'list-inference-jobs',
    );
    list_inference_jobs.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const get_endpoint_deployment_job = inference.addResource(
      'get-endpoint-deployment-job',
    );
    get_endpoint_deployment_job.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const get_inference_job = inference.addResource('get-inference-job');
    get_inference_job.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const get_texual_inversion_list = inference.addResource(
      'get-texual-inversion-list',
    );
    get_texual_inversion_list.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const get_lora_list = inference.addResource('get-lora-list');
    get_lora_list.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const get_hypernetwork_list = inference.addResource(
      'get-hypernetwork-list',
    );
    get_hypernetwork_list.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const get_controlnet_model_list = inference.addResource(
      'get-controlnet-model-list',
    );
    get_controlnet_model_list.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const get_inference_job_image_output = inference.addResource(
      'get-inference-job-image-output',
    );
    get_inference_job_image_output.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const get_inference_job_param_output = inference.addResource(
      'get-inference-job-param-output',
    );
    get_inference_job_param_output.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });

    const test_output = inference.addResource('generate-s3-presigned-url-for-uploading');
    test_output.addCorsPreflight({
      allowOrigins: apigw.Cors.ALL_ORIGINS,
      allowMethods: apigw.Cors.ALL_METHODS,
      allowHeaders: ['Content-Type', 'X-Amz-Date', 'Authorization', 'X-Api-Key', 'X-Amz-Security-Token', 'X-Amz-User-Agent'],
    });

    test_output.addMethod('GET', txt2imgIntegration, {
      apiKeyRequired: true,
    });
    const testResource = inference.addResource('test-connection');
    const mockIntegration = new apigw.MockIntegration({
      integrationResponses: [
        {
          statusCode: '200',
          responseTemplates: {
            'application/json': JSON.stringify({
              message: 'Success',
            }),
          },
        },
      ],
      passthroughBehavior: apigw.PassthroughBehavior.NEVER,
      requestTemplates: {
        'application/json': '{"statusCode": 200}',
      },
    });

    testResource.addMethod('GET', mockIntegration, {
      methodResponses: [
        {
          statusCode: '200',
          responseModels: {
            'application/json': apigw.Model.EMPTY_MODEL,
          },
        },
      ],
      apiKeyRequired: true,
    });

    const current_time = new Date().toISOString;

    const deployment = new apigw.Deployment(
      this,
      'rest-api-deployment' + current_time,
      {
        api: restful_api,
        retainDeployments: false,
      },
    );
    restful_api._attachDeployment(deployment);
    deployment.node.addDependency(test_output);

    deployment.addToLogicalId(new Date().toISOString());
    (deployment as any).resource.stageName = 'prod';

    const handler = new python.PythonFunction(
      this,
      "InferenceResultNotification",
      {
          entry: path.join(
              __dirname,
              "../../../middleware_api/lambda/inference_result_notification"
          ),
          runtime: lambda.Runtime.PYTHON_3_9,
          handler: "lambda_handler",
          index: 'app.py', // optional, defaults to 'index.py'
          memorySize: 256,
          timeout: Duration.seconds(900),
          environment: {
              DDB_INFERENCE_TABLE_NAME: sd_inference_job_table.tableName,
              DDB_TRAINING_TABLE_NAME: props?.training_table.tableName ?? '',
              DDB_ENDPOINT_DEPLOYMENT_TABLE_NAME: sd_endpoint_deployment_job_table.tableName,
              S3_BUCKET: props?.s3_bucket.bucketName ?? '',
              ACCOUNT_ID: Aws.ACCOUNT_ID,
              REGION_NAME: Aws.REGION,
              SNS_INFERENCE_SUCCESS: inference_result_topic.topicName,
              SNS_INFERENCE_ERROR: inference_result_error_topic.topicName,
              STEP_FUNCTION_ARN: stepFunctionStack.stateMachineArn,
              NOTICE_SNS_TOPIC: props?.snsTopic.topicArn ?? '',
              INFERENCE_ECR_IMAGE_URL: inferenceECR_url,
          },
          bundling: {
              buildArgs: {
                  PIP_INDEX_URL: "https://pypi.org/simple/",
                  PIP_EXTRA_INDEX_URL: "https://pypi.org/simple/",
              }
          },
          logRetention: RetentionDays.ONE_WEEK,
      }
  );
  
  // Grant Lambda permission to invoke SageMaker endpoint
  handler.addToRolePolicy(
      new iam.PolicyStatement({
          actions: [
              'sagemaker:*',
              's3:Get*',
              's3:List*',
              's3:PutObject',
              's3:GetObject',
              'sns:*',
              'states:*',
              'dynamodb:*',
          ],
          resources: ['*'],
      }),
  );

    //adding model to data directory of s3 bucket
    if (props?.s3_bucket != undefined) {
      this.uploadModelToS3(props?.s3_bucket);
    }

    // Add the SNS topic as an event source for the Lambda function
    handler.addEventSource(
      new eventSources.SnsEventSource(inference_result_topic),
    );
    handler.addEventSource(
      new eventSources.SnsEventSource(inference_result_error_topic),
    );
  }

  private createInferenceECR(srcImg: string) {
    const dockerRepo = new aws_ecr.Repository(
      this,
      'aigc-webui-inference-repo',
      {
        repositoryName: 'stable-diffusion-aws-extension/aigc-webui-inference',
        removalPolicy: RemovalPolicy.DESTROY,
      },
    );

    const ecrDeployment = new ECRDeployment(
      this,
      'aigc-webui-inference-ecr-deploy',
      {
        src: new DockerImageName(srcImg),
        dest: new DockerImageName(`${dockerRepo.repositoryUri}:latest`),
      },
    );

    // trigger the custom resource lambda
    const customJob = new CustomResource(
      this,
      'aigc-webui-inference-ecr-cr-image',
      {
        serviceToken: ecrDeployment.serviceToken,
        resourceType: 'Custom::AIGCSolutionECRLambda',
        properties: {
          SrcImage: `docker://${srcImg}`,
          DestImage: `docker://${dockerRepo.repositoryUri}:latest`,
          RepositoryName: `${dockerRepo.repositoryName}`,
        },
      },
    );

    customJob.node.addDependency(ecrDeployment);

    return dockerRepo.repositoryUri;
  }

  private uploadModelToS3(s3_bucket: s3.Bucket) {
    // Create a folder in the bucket
    const folderKey = 'data/';

    // Upload a local file to the created folder
    console.log(__dirname);
    const modelPath = path.resolve(__dirname, '../', '../', 'models', 'model.zip');
    new s3deploy.BucketDeployment(this, 'DeployLocalFile', {
      sources: [s3deploy.Source.asset(modelPath)],
      destinationBucket: s3_bucket,
      destinationKeyPrefix: folderKey,
      retainOnDelete: false,
    });
  }
}
