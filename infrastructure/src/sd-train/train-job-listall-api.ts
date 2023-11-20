import { PythonFunction, PythonFunctionProps } from '@aws-cdk/aws-lambda-python-alpha';
import {
  aws_apigateway,
  aws_apigateway as apigw,
  aws_dynamodb,
  aws_iam,
  aws_lambda, aws_s3,
  Duration,
} from 'aws-cdk-lib';
import { MethodOptions } from 'aws-cdk-lib/aws-apigateway/lib/method';
import { Effect } from 'aws-cdk-lib/aws-iam';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';


export interface ListAllTrainJobsApiProps {
  router: aws_apigateway.Resource;
  httpMethod: string;
  trainTable: aws_dynamodb.Table;
  multiUserTable: aws_dynamodb.Table;
  srcRoot: string;
  commonLayer: aws_lambda.LayerVersion;
  s3Bucket: aws_s3.Bucket;
  authorizer: aws_apigateway.IAuthorizer;
}

export class ListAllTrainJobsApi {
  private readonly src;
  private readonly router: aws_apigateway.Resource;
  private readonly httpMethod: string;
  private readonly scope: Construct;
  private readonly trainTable: aws_dynamodb.Table;
  private readonly multiUserTable: aws_dynamodb.Table;
  private readonly layer: aws_lambda.LayerVersion;
  private readonly s3Bucket: aws_s3.Bucket;
  private readonly authorizer: aws_apigateway.IAuthorizer;

  private readonly baseId: string;

  constructor(scope: Construct, id: string, props: ListAllTrainJobsApiProps) {
    this.scope = scope;
    this.baseId = id;
    this.router = props.router;
    this.httpMethod = props.httpMethod;
    this.trainTable = props.trainTable;
    this.multiUserTable = props.multiUserTable;
    this.src = props.srcRoot;
    this.layer = props.commonLayer;
    this.s3Bucket = props.s3Bucket;
    this.authorizer = props.authorizer;

    this.listAllTrainJobsApi();
  }

  private iamRole(): aws_iam.Role {
    const newRole = new aws_iam.Role(this.scope, `${this.baseId}-role`, {
      assumedBy: new aws_iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    newRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'dynamodb:BatchGetItem',
        'dynamodb:GetItem',
        'dynamodb:Scan',
        'dynamodb:Query',
      ],
      resources: [
         this.trainTable.tableArn,
         this.multiUserTable.tableArn,
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

  private listAllTrainJobsApi() {
    const lambdaFunction = new PythonFunction(this.scope, `${this.baseId}-listall`, <PythonFunctionProps>{
      functionName: `${this.baseId}-listall`,
      entry: `${this.src}/model_and_train`,
      architecture: Architecture.X86_64,
      runtime: Runtime.PYTHON_3_9,
      index: 'train_api.py',
      handler: 'list_all_train_jobs_api',
      timeout: Duration.seconds(900),
      role: this.iamRole(),
      memorySize: 1024,
      environment: {
        TRAIN_TABLE: this.trainTable.tableName,
        S3_BUCKET: this.s3Bucket.bucketName,
        MULTI_USER_TABLE: this.multiUserTable.tableName,
      },
      layers: [this.layer],
    });

    const listTrainJobsIntegration = new apigw.LambdaIntegration(
      lambdaFunction,
      {
        proxy: false,
        requestParameters: {
          'integration.request.querystring.status': 'method.request.querystring.status',
          'integration.request.querystring.types': 'method.request.querystring.types',
        },
        requestTemplates: {
          'application/json': '{\n' +
              '    "queryStringParameters": {\n' +
              '      #foreach($key in $method.request.multivaluequerystring.keySet())\n' +
              '      "$key" : [\n' +
              '        #foreach($val in $method.request.multivaluequerystring.get($key))\n' +
              '       "$val"#if($foreach.hasNext),#end\n' +
              '        #end\n' +
              '        ]#if($foreach.hasNext),#end\n' +
              '      #end\n' +
              '    },\n' +
              '    "x-auth": {\n' +
              '        "username": "$context.authorizer.username",\n' +
              '        "role": "$context.authorizer.role"\n' +
              '    }\n' +
              '}',
        },
        integrationResponses: [{ statusCode: '200' }],
      },
    );
    this.router.addMethod(this.httpMethod, listTrainJobsIntegration, <MethodOptions>{
      apiKeyRequired: true,
      authorizer: this.authorizer,
      requestParameters: {
        'method.request.querystring.status': true,
        'method.request.querystring.types': true,
      },
      methodResponses: [{
        statusCode: '200',
      }, { statusCode: '500' }],
    });
  }
}

