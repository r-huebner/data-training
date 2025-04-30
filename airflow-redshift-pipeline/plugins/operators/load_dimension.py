from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                load_sql="",
                create_sql="",
                table="",
                truncate=True,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_sql=load_sql
        self.create_sql=create_sql
        self.table =table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Create {self.table} table if not exists")
        redshift.run(self.create_sql)

        if self.truncate:
            self.log.info(f"Truncate dimension table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table};")

        self.log.info(f"Loading dimension table {self.table}")
        redshift.run(self.load_sql)
