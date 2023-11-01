from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            records = redshift_hook.get_records(sql)[0]
            errors = 0
            if exp_result != records[0]:
                errors += 1
            if errors > 0:
                self.log.info('Data quality check failed')
                raise ValueError('Data quality check failed')
            if errors == 0:
                self.log.info('Data quality check succeeded')
