import { PythonFunction, PythonFunctionProps } from '@aws-cdk/aws-lambda-python-alpha';
import {
  Aws,
  aws_apigateway as apigw,
  aws_apigateway,
  aws_dynamodb,
  aws_ecr,
  aws_iam,
  aws_lambda,
  aws_s3,
  aws_sns,
  CfnParameter,
  CustomResource,
  Duration,
  RemovalPolicy,
} from 'aws-cdk-lib';

import { MethodOptions } from 'aws-cdk-lib/aws-apigateway/lib/method';
import { Effect } from 'aws-cdk-lib/aws-iam';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';
import { DockerImageName, ECRDeployment } from '../../cdk-ecr-deployment/lib';
import { AIGC_WEBUI_DREAMBOOTH_TRAINING } from '../../common/dockerImages';
import { ResourceProvider } from '../../shared/resource-provider';

export interface StartTrainingJobApiProps {
  router: aws_apigateway.Resource;
  httpMethod: string;
  modelTable: aws_dynamodb.Table;
  trainTable: aws_dynamodb.Table;
  srcRoot: string;
  s3Bucket: aws_s3.Bucket;
  commonLayer: aws_lambda.LayerVersion;
  checkpointTable: aws_dynamodb.Table;
  userTopic: aws_sns.Topic;
  ecr_image_tag: string;
  logLevel: CfnParameter;
  resourceProvider: ResourceProvider;
}

export class StartTrainingJobApi {

  private readonly id: string;
  private readonly scope: Construct;
  private readonly srcRoot: string;
  private readonly modelTable: aws_dynamodb.Table;
  private readonly layer: aws_lambda.LayerVersion;
  private readonly s3Bucket: aws_s3.Bucket;
  private readonly httpMethod: string;
  private readonly router: aws_apigateway.Resource;
  private readonly trainTable: aws_dynamodb.Table;
  private readonly checkpointTable: aws_dynamodb.Table;
  private readonly sagemakerTrainRole: aws_iam.Role;
  private readonly dockerRepo: aws_ecr.Repository;
  private readonly customJob: CustomResource;
  private readonly userSnsTopic: aws_sns.Topic;
  private readonly srcImg: string;
  private readonly instanceType: string = 'ml.g4dn.2xlarge';
  private readonly logLevel: CfnParameter;
  private readonly resourceProvider: ResourceProvider;

  constructor(scope: Construct, id: string, props: StartTrainingJobApiProps) {
    this.id = id;
    this.scope = scope;
    this.srcRoot = props.srcRoot;
    this.userSnsTopic = props.userTopic;
    this.modelTable = props.modelTable;
    this.checkpointTable = props.checkpointTable;
    this.layer = props.commonLayer;
    this.s3Bucket = props.s3Bucket;
    this.httpMethod = props.httpMethod;
    this.router = props.router;
    this.trainTable = props.trainTable;
    this.logLevel = props.logLevel;
    this.resourceProvider = props.resourceProvider;
    this.sagemakerTrainRole = this.sageMakerTrainRole();
    this.srcImg = AIGC_WEBUI_DREAMBOOTH_TRAINING + props.ecr_image_tag;
    [this.dockerRepo, this.customJob] = this.trainImageInPrivateRepo(this.srcImg);


    this.startTrainJobLambda();
  }

  private sageMakerTrainRole(): aws_iam.Role {
    const sagemakerRole = new aws_iam.Role(this.scope, `${this.id}-train-role`, {
      assumedBy: new aws_iam.ServicePrincipal('sagemaker.amazonaws.com'),
    });
    sagemakerRole.addManagedPolicy(aws_iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSageMakerFullAccess'));

    sagemakerRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        's3:GetObject',
        's3:PutObject',
      ],
      resources: [
        `${this.s3Bucket.bucketArn}/*`,
        `arn:${Aws.PARTITION}:s3:::*SageMaker*`,
        `arn:${Aws.PARTITION}:s3:::*Sagemaker*`,
        `arn:${Aws.PARTITION}:s3:::*sagemaker*`,
      ],
    }));

    sagemakerRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'kms:Decrypt',
      ],
      resources: ['*'],
    }));

    return sagemakerRole;
  }

  private getLambdaRole(): aws_iam.Role {
    const newRole = new aws_iam.Role(this.scope, `${this.id}-role`, {
      assumedBy: new aws_iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    newRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'dynamodb:BatchGetItem',
        'dynamodb:GetItem',
        'dynamodb:Scan',
        'dynamodb:Query',
        'dynamodb:BatchWriteItem',
        'dynamodb:PutItem',
        'dynamodb:UpdateItem',
        'dynamodb:DeleteItem',
      ],
      resources: [this.modelTable.tableArn, this.trainTable.tableArn, this.checkpointTable.tableArn],
    }));

    newRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'sagemaker:CreateTrainingJob',
      ],
      resources: [`arn:${Aws.PARTITION}:sagemaker:${Aws.REGION}:${Aws.ACCOUNT_ID}:training-job/*`],
    }));

    newRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'iam:PassRole',
      ],
      resources: [this.sagemakerTrainRole.roleArn],
    }));

    newRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        's3:GetObject',
        's3:PutObject',
        's3:DeleteObject',
        's3:ListBucket',
      ],
      resources: [
        `${this.s3Bucket.bucketArn}/*`,
        `arn:${Aws.PARTITION}:s3:::*SageMaker*`,
        `arn:${Aws.PARTITION}:s3:::*Sagemaker*`,
        `arn:${Aws.PARTITION}:s3:::*sagemaker*`,
      ],
    }));

    newRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
        'kms:Decrypt',
      ],
      resources: ['*'],
    }));

    return newRole;
  }


  private startTrainJobLambda(): aws_lambda.IFunction {
    const lambdaFunction = new PythonFunction(this.scope, `${this.id}-lambda`, <PythonFunctionProps>{
      entry: `${this.srcRoot}/trainings`,
      architecture: Architecture.X86_64,
      runtime: Runtime.PYTHON_3_9,
      index: 'start_training_job.py',
      handler: 'handler',
      timeout: Duration.seconds(900),
      role: this.getLambdaRole(),
      memorySize: 1024,
      environment: {
        S3_BUCKET: this.s3Bucket.bucketName,
        TRAIN_TABLE: this.trainTable.tableName,
        MODEL_TABLE: this.modelTable.tableName,
        CHECKPOINT_TABLE: this.checkpointTable.tableName,
        INSTANCE_TYPE: this.instanceType,
        TRAIN_JOB_ROLE: this.sagemakerTrainRole.roleArn,
        TRAIN_ECR_URL: `${this.dockerRepo.repositoryUri}:latest`,
        USER_EMAIL_TOPIC_ARN: this.userSnsTopic.topicArn,
        LOG_LEVEL: this.logLevel.valueAsString,
      },
      layers: [this.layer],
    });
    lambdaFunction.node.addDependency(this.customJob);


    const startTrainJobIntegration = new apigw.LambdaIntegration(
      lambdaFunction,
      {
        proxy: true,
      },
    );

    this.router.addResource('start')
      .addMethod(this.httpMethod, startTrainJobIntegration, <MethodOptions>{
        apiKeyRequired: true,
      });

    return lambdaFunction;
  }

  private trainImageInPrivateRepo(srcImage: string): [aws_ecr.Repository, CustomResource] {
    const dockerRepo = new aws_ecr.Repository(this.scope, `${this.id}-repo`, {
      repositoryName: 'stable-diffusion-aws-extension/aigc-webui-dreambooth-training',
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const ecrDeployment = new ECRDeployment(this.scope, `${this.id}-ecr-deploy`, {
      src: new DockerImageName(srcImage),
      dest: new DockerImageName(`${dockerRepo.repositoryUri}:latest`),
      environment: {
        BUCKET_NAME: this.resourceProvider.bucketName,
      },
    });

    // trigger the custom resource lambda
    const customJob = new CustomResource(this.scope, `${this.id}-cr-image`, {
      serviceToken: ecrDeployment.serviceToken,
      resourceType: 'Custom::AIGCSolutionECRLambda',
      properties: {
        SrcImage: `docker://${srcImage}`,
        DestImage: `docker://${dockerRepo.repositoryUri}:latest`,
        RepositoryName: `${dockerRepo.repositoryName}`,
      },
    });
    customJob.node.addDependency(ecrDeployment);
    return [dockerRepo, customJob];
  }


}
