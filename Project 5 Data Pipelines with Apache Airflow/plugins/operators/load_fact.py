from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
    This Class is used to Create Fact Table in Redshift Cluster

        Parameters:
            redshift_conn_id : Redshift Connection ID
            table_name : Dimension Table name that is being used to create
            query : Query used to create fact table in the Redshift cluster
'''


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 query="",
                 truncate_table = True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table_name=table_name
        self.redshift_conn_id = redshift_conn_id
        self.query=query
        self.truncate_table = truncate_table
        
    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Truncating Table {self.table_name}')
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table_name}')
            redshift.run("DELETE FROM {}".format(self.table_name))
        self.log.info(f'Running query {self.query}')
        #redshift.run(f"Insert into {self.table_name} {self.query}")
        redshift.run(self.query)
       
