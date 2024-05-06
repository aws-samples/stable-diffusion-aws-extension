import { Table } from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';


export class Database {

  public trainingTable: Table;
  public checkpointTable: Table;
  public datasetInfoTable: Table;
  public datasetItemTable: Table;
  public sDInferenceJobTable: Table;
  public sDEndpointDeploymentJobTable: Table;
  public multiUserTable: Table;
  public logSubTable: Table;

  constructor(scope: Construct, baseId: string) {

    this.trainingTable = this.table(scope, baseId, 'TrainingTable');

    this.checkpointTable = this.table(scope, baseId, 'CheckpointTable');

    this.datasetInfoTable = this.table(scope, baseId, 'DatasetInfoTable');

    this.datasetItemTable = this.table(scope, baseId, 'DatasetItemTable');

    this.sDInferenceJobTable = this.table(scope, baseId, 'SDInferenceJobTable');

    this.sDEndpointDeploymentJobTable = this.table(scope, baseId, 'SDEndpointDeploymentJobTable');

    this.multiUserTable = this.table(scope, baseId, 'MultiUserTable');

    this.logSubTable = this.table(scope, baseId, 'LogSubTable');

  }

  private table(scope: Construct, baseId: string, tableName: string): Table {
    return <Table>Table.fromTableName(scope, `${baseId}-${tableName}`, tableName);
  }
}
