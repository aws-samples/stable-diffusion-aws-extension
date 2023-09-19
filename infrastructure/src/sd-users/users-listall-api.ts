import { PythonFunction, PythonFunctionProps } from '@aws-cdk/aws-lambda-python-alpha';
import {
  aws_apigateway,
  aws_apigateway as apigw,
  aws_dynamodb,
  aws_iam, aws_kms,
  aws_lambda,
  Duration,
} from 'aws-cdk-lib';
import { MethodOptions } from 'aws-cdk-lib/aws-apigateway/lib/method';
import { Effect } from 'aws-cdk-lib/aws-iam';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';


export interface ListAllUsersApiProps {
  router: aws_apigateway.Resource;
  httpMethod: string;
  multiUserTable: aws_dynamodb.Table;
  srcRoot: string;
  commonLayer: aws_lambda.LayerVersion;
  passwordKey: aws_kms.IKey;
  authorizer: aws_apigateway.IAuthorizer;
}

export class ListAllUsersApi {
  private readonly src;
  private readonly router: aws_apigateway.Resource;
  private readonly httpMethod: string;
  private readonly scope: Construct;
  private readonly multiUserTable: aws_dynamodb.Table;
  private readonly layer: aws_lambda.LayerVersion;
  private readonly passwordKey: aws_kms.IKey;
  private readonly baseId: string;
  private readonly authorizer: aws_apigateway.IAuthorizer;

  constructor(scope: Construct, id: string, props: ListAllUsersApiProps) {
    this.scope = scope;
    this.baseId = id;
    this.router = props.router;
    this.passwordKey = props.passwordKey;
    this.httpMethod = props.httpMethod;
    this.multiUserTable = props.multiUserTable;
    this.src = props.srcRoot;
    this.layer = props.commonLayer;
    this.authorizer = props.authorizer;

    this.listAllUsersApi();
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
      resources: [this.multiUserTable.tableArn],
    }));

    newRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'kms:Decrypt',
      ],
      resources: ['*'],
      conditions: {
        StringEquals: {
          'kms:RequestAlias': `alias/${this.passwordKey.keyId}`,
        },
      },
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

  private listAllUsersApi() {
    const lambdaFunction = new PythonFunction(this.scope, `${this.baseId}-listall`, <PythonFunctionProps>{
      functionName: `${this.baseId}-listall`,
      entry: `${this.src}/multi_users`,
      architecture: Architecture.X86_64,
      runtime: Runtime.PYTHON_3_9,
      index: 'multi_users_api.py',
      handler: 'list_user',
      timeout: Duration.seconds(900),
      role: this.iamRole(),
      memorySize: 1024,
      environment: {
        MULTI_USER_TABLE: this.multiUserTable.tableName,
        KEY_ID: `alias/${this.passwordKey.keyId}`,
      },
      layers: [this.layer],
    });

    const listUsersIntegration = new apigw.LambdaIntegration(
      lambdaFunction,
      {
        proxy: false,
        requestParameters: {
          'integration.request.querystring.last_evaluated_key': 'method.request.querystring.last_evaluated_key',
          'integration.request.querystring.limit': 'method.request.querystring.limit',
          'integration.request.querystring.username': 'method.request.querystring.username',
          'integration.request.querystring.filter': 'method.request.querystring.filter',
          'integration.request.querystring.show_password': 'method.request.querystring.show_password',
        },
        requestTemplates: {
          'application/json': '{\n' +
                        '    "queryStringParameters": {\n' +
                        '        #foreach($queryParam in $input.params().querystring.keySet())\n' +
                        '        "$queryParam": "$util.escapeJavaScript($input.params().querystring.get($queryParam))"\n' +
                        '        #if($foreach.hasNext),#end\n' +
                        '        #end\n' +
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
    this.router.addMethod(this.httpMethod, listUsersIntegration, <MethodOptions>{
      apiKeyRequired: true,
      authorizer: this.authorizer,
      requestParameters: {
        'method.request.querystring.last_evaluated_key': false,
        'method.request.querystring.limit': false,
        'method.request.querystring.username': false,
        'method.request.querystring.filter': false,
        'method.request.querystring.show_password': false,
      },
      methodResponses: [{
        statusCode: '200',
      }, { statusCode: '500' }],
    });
  }
}

