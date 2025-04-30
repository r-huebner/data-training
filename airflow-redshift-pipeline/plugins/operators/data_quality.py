from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks=checks or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Get check data
        for check in self.checks:
            test_sql = check.get("test_sql")
            expected_result = check.get("expected_result")
            comparison = check.get("comparison")
            check_name = check.get("check_name")

            records = redshift.get_records(test_sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check {check_name} failed. {test_sql} returned no records")
            actual  = records[0][0]
            if not self._compare(actual,expected_result,comparison):
                raise ValueError(f"""Data quality check {check_name} failed. 
                                    Result was {actual} instead of {expected_result}""")
            self.log.info(f"Data quality check {check_name} passed with the result {actual}")

    def _compare(self, actual, expected, comp):
                if comp == "==":
                    return actual == expected
                elif comp == "!=":
                    return actual != expected
                elif comp == ">":
                    return actual > expected
                elif comp == "<":
                    return actual < expected
                elif comp == ">=":
                    return actual >= expected
                elif comp == "<=":
                    return actual <= expected
                else:
                    raise ValueError(f"Unsupported comparison: {comp}")
