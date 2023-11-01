from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.append_only == True:
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift_hook.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift_hook.run(sql_statement)
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift_hook.run(sql_statement)
        self.log.info('LoadFactOperator succeeded')
