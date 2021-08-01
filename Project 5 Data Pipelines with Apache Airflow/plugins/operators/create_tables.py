from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os

'''
    This Class is used to Create Tables in Redshift Cluster

        Parameters:
            redshift_conn_id : Redshift Connection ID
'''
    
class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'
    script_path = os.path.dirname(os.path.realpath(__file__))
    sql_statement_file='create_tables.sql'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Creating Redshift tables ")
        self.log.info("Script Path is " +CreateTablesOperator.script_path)
        fd = open(os.path.join(CreateTablesOperator.script_path,CreateTablesOperator.sql_statement_file), 'r')
        sql_file = fd.read()
        fd.close()

        sql_commands = sql_file.split(';')

        for command in sql_commands:
            if command.rstrip() != '':
                self.log.info(command)
                redshift.run(command)