from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import pendulum
from random import choice

@dag(
    dag_id="dags_email_taskflow_example",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"]
)
def email_taskflow_dag():

    @task(task_id="something_task")
    def some_logic():
        return choice(["Success", "Fail"])

    result = some_logic()

    send_email = EmailOperator(
        task_id="send_email",
        to="skj1180@naver.com",
        subject="{{ data_interval_end.in_timezone('Asia/Seoul') | ds }} some_logic 처리결과",
        html_content="""
        {{ data_interval_end.in_timezone('Asia/Seoul') | ds }} 처리 결과는 <br>
        {{ ti.xcom_pull(task_ids='something_task') }} 였습니다 <br>
        """
    )

    result >> send_email


dag = email_taskflow_dag()
