import data_quality_queries
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 test_case_params,
                 aws_redshift_conn_id="redshift_default",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.test_case_params = test_case_params
        self.aws_redshift_conn_id = aws_redshift_conn_id

    def execute(self, context):
        self.log.info('Initialising DataQualityOperator')

        self.log.info(f'Initialising PostgresHook with connection {self.aws_redshift_conn_id}')
        hook = PostgresHook(self.aws_redshift_conn_id)

        self.log.info('Executing DataQualityOperator NULL checks')
        null_check_base_query = data_quality_queries.null_pct_check
        null_check_test_cases = self.test_case_params.get('null_checks')

        errors = []
        for null_check_test_case in null_check_test_cases:
            self.log.info('Checking null checks for {}'.format(null_check_test_case))
            table_name = null_check_test_case['table_name']
            column_name = null_check_test_case['column_name']
            threshold = null_check_test_case['threshold']
            null_check_test_case['test_case_type'] = 'null_check'

            executable_query = null_check_base_query.format(table_name=table_name, column_name=column_name,
                                                            threshold=threshold)
            output = hook.get_records(executable_query)
            if len(output) > 0:
                errors.append(null_check_test_case)

        self.log.info('Executing DataQualityOperator DUPLICATE checks')
        dup_check_base_query = data_quality_queries.duplicate_check
        dup_check_test_cases = self.test_case_params.get('duplicate_checks')

        for dup_check_test_case in dup_check_test_cases:
            self.log.info('Checking null checks for {}'.format(dup_check_test_case))
            table_name = dup_check_test_case['table_name']
            column_name = dup_check_test_case['column_name']
            dup_check_test_case['test_case_type'] = 'duplicate_check'

            executable_query = dup_check_base_query.format(table_name=table_name, column_name=column_name)
            output = hook.get_records(executable_query)
            if len(output) > 0:
                errors.append(dup_check_test_case)

        self.log.info('Executing DataQualityOperator ROW COUNT checks')
        row_count_base_query = data_quality_queries.row_count_check
        row_count_test_cases = self.test_case_params.get('row_count_check')

        for row_count_test_case in row_count_test_cases:
            self.log.info('Checking null checks for {}'.format(row_count_test_cases))
            table_name = row_count_test_case['table_name']
            row_count_test_case['test_case_type'] = 'row_count_check'

            executable_query = row_count_base_query.format(table_name=table_name)
            output = hook.get_records(executable_query)
            if len(output) > 0:
                errors.append(row_count_test_case)

        if errors:
            raise Exception(f"Data Quality Checks Failed : {errors}")

