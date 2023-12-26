import {PythonFunction, PythonFunctionProps} from '@aws-cdk/aws-lambda-python-alpha';
import {aws_apigateway, aws_apigateway as apigw, aws_dynamodb, aws_iam, aws_lambda, Duration} from 'aws-cdk-lib';
import {JsonSchemaType, JsonSchemaVersion, Model, RequestValidator} from 'aws-cdk-lib/aws-apigateway';
import {MethodOptions} from 'aws-cdk-lib/aws-apigateway/lib/method';
import {Effect} from 'aws-cdk-lib/aws-iam';
import {Architecture, Runtime} from 'aws-cdk-lib/aws-lambda';
import {Construct} from 'constructs';


export interface CreateRoleApiProps {
    router: aws_apigateway.Resource;
    httpMethod: string;
    multiUserTable: aws_dynamodb.Table;
    srcRoot: string;
    commonLayer: aws_lambda.LayerVersion;
}

export class CreateRoleApi {
    private readonly src;
    private readonly router: aws_apigateway.Resource;
    private readonly httpMethod: string;
    private readonly scope: Construct;
    private readonly layer: aws_lambda.LayerVersion;
    private readonly multiUserTable: aws_dynamodb.Table;

    private readonly baseId: string;

    constructor(scope: Construct, id: string, props: CreateRoleApiProps) {
        this.scope = scope;
        this.httpMethod = props.httpMethod;
        this.baseId = id;
        this.router = props.router;
        this.src = props.srcRoot;
        this.layer = props.commonLayer;
        this.multiUserTable = props.multiUserTable;

        this.createRoleApi();
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
                'dynamodb:BatchWriteItem',
                'dynamodb:PutItem',
                'dynamodb:UpdateItem',
                'dynamodb:DeleteItem',
            ],
            resources: [
                this.multiUserTable.tableArn,
            ],
        }));

        newRole.addToPolicy(new aws_iam.PolicyStatement({
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

    private createRoleApi() {
        const lambdaFunction = new PythonFunction(this.scope, `${this.baseId}-lambda`, <PythonFunctionProps>{
            entry: `${this.src}/roles`,
            architecture: Architecture.X86_64,
            runtime: Runtime.PYTHON_3_9,
            index: 'create_role.py',
            handler: 'handler',
            timeout: Duration.seconds(900),
            role: this.iamRole(),
            memorySize: 1024,
            environment: {
                MULTI_USER_TABLE: this.multiUserTable.tableName,
            },
            layers: [this.layer],
        });

        const requestModel = new Model(this.scope, `${this.baseId}-model`, {
            restApi: this.router.api,
            modelName: this.baseId,
            description: `${this.baseId} Request Model`,
            schema: {
                schema: JsonSchemaVersion.DRAFT4,
                title: this.baseId,
                type: JsonSchemaType.OBJECT,
                properties: {
                    role_name: {
                        type: JsonSchemaType.STRING,
                        minLength: 1,
                    },
                    creator: {
                        type: JsonSchemaType.STRING,
                        minLength: 1,
                    },
                    permissions: {
                        type: JsonSchemaType.ARRAY,
                        items: {
                            type: JsonSchemaType.STRING,
                            minLength: 1,
                        },
                        minItems: 1,
                        maxItems: 20,
                    },
                },
                required: [
                    'role_name',
                    'creator',
                    'permissions',
                ],
            },
            contentType: 'application/json',
        });

        const requestValidator = new RequestValidator(
            this.scope,
            `${this.baseId}-validator`,
            {
                restApi: this.router.api,
                requestValidatorName: this.baseId,
                validateRequestBody: true,
            });

        const upsertRoleIntegration = new apigw.LambdaIntegration(
            lambdaFunction,
            {
                proxy: true,
            },
        );
        this.router.addMethod(this.httpMethod, upsertRoleIntegration, <MethodOptions>{
            apiKeyRequired: true,
            requestValidator,
            requestModels: {
                'application/json': requestModel,
            },
        });
    }
}

