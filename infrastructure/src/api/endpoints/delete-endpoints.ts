import {PythonFunction} from '@aws-cdk/aws-lambda-python-alpha';
import {Aws, aws_lambda, Duration} from 'aws-cdk-lib';
import {JsonSchemaType, JsonSchemaVersion, LambdaIntegration, Model, Resource} from 'aws-cdk-lib/aws-apigateway';
import {Table} from 'aws-cdk-lib/aws-dynamodb';
import {Effect, PolicyStatement, Role, ServicePrincipal} from 'aws-cdk-lib/aws-iam';
import {Architecture, LayerVersion, Runtime} from 'aws-cdk-lib/aws-lambda';
import {Construct} from 'constructs';
import {ApiModels} from '../../shared/models';
import {SCHEMA_ENDPOINT_NAME} from '../../shared/schema';
import {ApiValidators} from '../../shared/validator';

export interface DeleteEndpointsApiProps {
    router: Resource;
    httpMethod: string;
    endpointDeploymentTable: Table;
    multiUserTable: Table;
    commonLayer: LayerVersion;
}

export class DeleteEndpointsApi {
    private readonly router: Resource;
    private readonly httpMethod: string;
    private readonly scope: Construct;
    private readonly endpointDeploymentTable: Table;
    private readonly multiUserTable: Table;
    private readonly layer: LayerVersion;
    private readonly baseId: string;

    constructor(scope: Construct, id: string, props: DeleteEndpointsApiProps) {
        this.scope = scope;
        this.baseId = id;
        this.router = props.router;
        this.httpMethod = props.httpMethod;
        this.endpointDeploymentTable = props.endpointDeploymentTable;
        this.multiUserTable = props.multiUserTable;
        this.layer = props.commonLayer;

        const lambdaFunction = this.apiLambda();

        const lambdaIntegration = new LambdaIntegration(
            lambdaFunction,
            {
                proxy: true,
            },
        );

        this.router.addMethod(this.httpMethod, lambdaIntegration, {
            apiKeyRequired: true,
            requestValidator: ApiValidators.bodyValidator,
            requestModels: {
                'application/json': this.createRequestBodyModel(),
            },
            operationName: 'DeleteEndpoints',
            methodResponses: [
                ApiModels.methodResponses204(),
                ApiModels.methodResponses400(),
                ApiModels.methodResponses401(),
                ApiModels.methodResponses403(),
            ],
        });
    }

    private iamRole(): Role {

        const newRole = new Role(this.scope, `${this.baseId}-role`, {
            assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
        });

        newRole.addToPolicy(new PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
                'sagemaker:DeleteModel',
                'sagemaker:DeleteEndpoint',
                'sagemaker:DeleteEndpointConfig',
                'sagemaker:DescribeEndpoint',
                'sagemaker:DescribeEndpointConfig',
            ],
            resources: [
                `arn:${Aws.PARTITION}:sagemaker:${Aws.REGION}:${Aws.ACCOUNT_ID}:model/*`,
                `arn:${Aws.PARTITION}:sagemaker:${Aws.REGION}:${Aws.ACCOUNT_ID}:endpoint/*`,
                `arn:${Aws.PARTITION}:sagemaker:${Aws.REGION}:${Aws.ACCOUNT_ID}:endpoint-config/*`,
            ],
        }));

        newRole.addToPolicy(new PolicyStatement({
            actions: [
                'dynamodb:Query',
                'dynamodb:GetItem',
                'dynamodb:PutItem',
                'dynamodb:DeleteItem',
                'dynamodb:UpdateItem',
                'dynamodb:Describe*',
                'dynamodb:List*',
            ],
            resources: [
                this.endpointDeploymentTable.tableArn,
                `${this.endpointDeploymentTable.tableArn}/*`,
                this.multiUserTable.tableArn,
            ],
        }));

        newRole.addToPolicy(new PolicyStatement({
            actions: [
                's3:Get*',
                's3:List*',
                's3:PutObject',
                's3:GetObject',
                's3:DeleteObject',
            ],
            resources: [
                '*',
            ],
        }));

        newRole.addToPolicy(new PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
                'application-autoscaling:DeregisterScalableTarget',
                'cloudwatch:DeleteAlarms',
                'cloudwatch:DescribeAlarms',
                'cloudwatch:DeleteDashboards',
            ],
            resources: [
                '*',
            ],
        }));

        newRole.addToPolicy(new PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
                'logs:CreateLogGroup',
                'logs:CreateLogStream',
                'logs:PutLogEvents',
                'logs:DeleteLogGroup',
            ],
            resources: [`arn:${Aws.PARTITION}:logs:${Aws.REGION}:${Aws.ACCOUNT_ID}:log-group:*:*`],
        }));

        return newRole;
    }

    private createRequestBodyModel(): Model {
        return new Model(this.scope, `${this.baseId}-model`, {
            restApi: this.router.api,
            modelName: this.baseId,
            description: `Request Model ${this.baseId}`,
            schema: {
                schema: JsonSchemaVersion.DRAFT7,
                title: this.baseId,
                type: JsonSchemaType.OBJECT,
                properties: {
                    endpoint_name_list: {
                        type: JsonSchemaType.ARRAY,
                        items: SCHEMA_ENDPOINT_NAME,
                        minItems: 1,
                        maxItems: 10,
                    },
                },
                required: [
                    'endpoint_name_list',
                ],
            },
            contentType: 'application/json',
        });
    }

    private apiLambda() {
        return new PythonFunction(this.scope, `${this.baseId}-lambda`, {
            entry: '../middleware_api/endpoints',
            architecture: Architecture.X86_64,
            runtime: Runtime.PYTHON_3_10,
            index: 'delete_endpoints.py',
            handler: 'handler',
            timeout: Duration.seconds(900),
            role: this.iamRole(),
            memorySize: 2048,
            tracing: aws_lambda.Tracing.ACTIVE,
            layers: [this.layer],
        });
    }


}
