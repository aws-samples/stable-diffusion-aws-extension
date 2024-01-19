import { PythonFunction, PythonFunctionProps } from '@aws-cdk/aws-lambda-python-alpha';
import { aws_iam, CfnParameter, Duration } from 'aws-cdk-lib';
import { Table } from 'aws-cdk-lib/aws-dynamodb';
import { Rule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { Effect, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Architecture, LayerVersion, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';

export interface SagemakerEndpointEventsProps {
  endpointDeploymentTable: Table;
  multiUserTable: Table;
  srcRoot: string;
  commonLayer: LayerVersion;
  logLevel: CfnParameter;
}

export class SagemakerEndpointEvents {
  private readonly src: string;
  private readonly scope: Construct;
  private readonly endpointDeploymentTable: Table;
  private readonly multiUserTable: Table;
  private readonly layer: LayerVersion;
  private readonly baseId: string;
  private readonly logLevel: CfnParameter;

  constructor(scope: Construct, id: string, props: SagemakerEndpointEventsProps) {
    this.scope = scope;
    this.baseId = id;
    this.endpointDeploymentTable = props.endpointDeploymentTable;
    this.multiUserTable = props.multiUserTable;
    this.src = props.srcRoot;
    this.layer = props.commonLayer;
    this.logLevel = props.logLevel;

    this.createEndpointEventBridge();
  }

  private iamRole(): Role {

    const newRole = new Role(this.scope, `${this.baseId}-role`, {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
    });

    newRole.addToPolicy(new aws_iam.PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'dynamodb:UpdateItem',
        'dynamodb:Scan',
        'dynamodb:GetItem',
      ],
      resources: [
        this.endpointDeploymentTable.tableArn,
        this.multiUserTable.tableArn,
      ],
    }));

    newRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'sagemaker:DeleteEndpoint',
        'sagemaker:DescribeEndpoint',
        'sagemaker:DescribeEndpointConfig',
        'sagemaker:UpdateEndpointWeightsAndCapacities',
        'cloudwatch:DeleteAlarms',
        'cloudwatch:DescribeAlarms',
        'cloudwatch:PutMetricAlarm',
        'application-autoscaling:PutScalingPolicy',
        'application-autoscaling:RegisterScalableTarget',
      ],
      resources: ['*'],
    }));

    newRole.addToPolicy(new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
      ],
      resources: ['*'],
    }));

    return newRole;
  }

  private createEndpointEventBridge() {

    const lambdaFunction = new PythonFunction(this.scope, `${this.baseId}-lambda`, <PythonFunctionProps>{
      entry: `${this.src}/endpoints`,
      architecture: Architecture.X86_64,
      runtime: Runtime.PYTHON_3_9,
      index: 'endpoint_event.py',
      handler: 'handler',
      timeout: Duration.seconds(900),
      role: this.iamRole(),
      memorySize: 1024,
      environment: {
        DDB_ENDPOINT_DEPLOYMENT_TABLE_NAME: this.endpointDeploymentTable.tableName,
        MULTI_USER_TABLE: this.multiUserTable.tableName,
        LOG_LEVEL: this.logLevel.valueAsString,
      },
      layers: [this.layer],
    });

    const rule = new Rule(this.scope, `${this.baseId}-rule`, {
      eventPattern: {
        source: ['aws.sagemaker'],
        detailType: ['SageMaker Endpoint State Change'],
      },
    });

    rule.addTarget(new LambdaFunction(lambdaFunction));

  }
}
