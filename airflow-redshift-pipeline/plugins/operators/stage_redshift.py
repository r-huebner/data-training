from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sql.sql_statements import SqlQueries as sqt_stm

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                timestamped=False,
                json_path="auto",
                create_sql="",
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.timestamped = timestamped
        self.json_path = json_path
        self.create_sql=create_sql

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Create {self.table} table if not exists")
        redshift.run(self.create_sql)

        self.log.info(f"Truncate {self.table} table")
        redshift.run(f"TRUNCATE TABLE {self.table};")    

        execution_date = context.get("execution_date")
        if self.timestamped:
            s3_path = f"s3://{self.s3_bucket}/{self.s3_key}/{execution_date.year}/{execution_date.month}/"
        else:
            s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info(f"Copying data from {s3_path} to {self.table} table")
        formatted_sql = sqt_stm.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            json_path=self.json_path,
        )
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)






