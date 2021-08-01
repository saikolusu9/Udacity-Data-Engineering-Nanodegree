from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
    This Class is used to Create Tables in Redshift Cluster

        Parameters:
            redshift_conn_id : Redshift Connection ID
'''

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        rsft = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Execution entered for {table}")
            records = rsft.get_records(f"SELECT COUNT(*) FROM {table}")
            self.log.info(records)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            if records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Execution completed for {table}")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")