from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    @apply_defaults
    def __init__(
            self,
            s3_path,
            target_table,
            json_path=None,
            aws_redshift_conn_id="redshift_default",
            aws_credentials_conn_id="aws_credentials",
            *args,
            **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(**kwargs)
        self.aws_redshift_conn_id = aws_redshift_conn_id
        self.aws_credentials_conn_id = aws_credentials_conn_id
        self.target_table = target_table
        self.s3_path = s3_path
        self.json_path = json_path
        self.execution_date = kwargs.get("execution_date")

    def execute(self, context):
        self.log.info(
            f"Data copy from S3 {self.s3_path} to Redshift target {self.target_table}"
        )

        self.log.info(f"Creating hooks")
        aws_hook = AwsHook(self.aws_credentials_conn_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.aws_redshift_conn_id)

        self.log.info(f"Truncating data from the table {self.target_table}")
        redshift.run("DELETE FROM {}".format(self.target_table))

        self.log.info("Copying data from S3 to Redshift")
        copy_sql = f""" COPY {self.target_table} 
        FROM '{self.s3_path}' 
        ACCESS_KEY_ID '{aws_credentials.access_key}' 
        SECRET_ACCESS_KEY '{aws_credentials.secret_key}' """

        if self.execution_date:
            self.log.info(
                f"Execution date set to {self.execution_date}. Executing back fill load"
            )
            copy_sql = f""" COPY {self.target_table} 
            FROM '{self.s3_path}/
            ACCESS_KEY_ID '{aws_credentials.access_key}' 
            SECRET_ACCESS_KEY '{aws_credentials.secret_key}'  """

        if self.json_path:
            copy_sql = f"{copy_sql} JSON '{self.json_path}' "

        redshift.run(copy_sql)
