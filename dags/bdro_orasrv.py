"""
The BRD ORASRV report is a daily process run at approximately 6:30 a.m. from Monday to Friday,
focusing on the generation and validation of transactional and settlement data for Visa and MasterCard.
This report is produced two days after the transaction settlement date.
Since it doesn’t run over the weekend, data from Thursday, Friday, and Saturday is processed together on the following Monday.
"""

from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def validate_results(**kwargs) -> str:
    results: List[List[float]] = kwargs["ti"].xcom_pull(
        task_ids="check_and_get_precision", key="return_value"
    )
    all_greater_than_99 = all(
        float(value) > 99.00 for item in results for value in item[1:]
    )
    return (
        "generate_and_send_report"
        if all_greater_than_99
        else "send_notification_if_data_is_not_greater_than_99"
    )


def default_dag_parameters() -> tuple[str, str]:
    today = datetime.today()
    if today.weekday() == 0:  # Monday
        default_start_date = (today - timedelta(days=4)).strftime("%Y-%m-%d")
        default_end_date = (today - timedelta(days=2)).strftime("%Y-%m-%d")
    elif 1 <= today.weekday() <= 4:  # Tuesday to Friday
        default_start_date = (today - timedelta(days=2)).strftime("%Y-%m-%d")
        default_end_date = (today - timedelta(days=2)).strftime("%Y-%m-%d")
    else:
        raise ValueError("Invalid day of the week for this report")

    return default_start_date, default_end_date


default_args = {
    "owner": "Max E.",
    "email": ["max.espejo@intelica.com"],
    "do_xcom_push": False,
    "email_on_failure": True,
    #'retries': 1,
}

default_start_date, default_end_date = default_dag_parameters()


with DAG(
    dag_id="BDRO_Orasrv",
    default_args=default_args,
    # schedule = "0 12 * * 1-5",
    schedule=None,
    catchup=False,
    params={"start_date": default_start_date, "end_date": default_end_date},
) as dag:
    check_and_get_precision = SQLExecuteQueryOperator(
        task_id="check_and_get_precision",
        conn_id="mssql_default",
        sql="""EXEC [TABRDRO_RPT].[dbo].[ETL_BRDRO_ORASRV] 
               @BGN_PRC_DT='2024-12-16', 
               @END_PRC_DT='2024-12-16', 
               @BGN_STP_ID=1, 
               @END_STP_ID=1"""
        ,
        parameters={
            "BGN_PRC_DT": "{{ params.start_date }}",
            "END_PRC_DT": "{{params.end_date}}",
        },
        do_xcom_push=True,
        hook_params={"autocommit": True},
        show_return_value_in_logs = True,
        
    )

    validate_if_precision_is_greater_than_99 = BranchPythonOperator(
        task_id="validate_if_precision_is_greater_than_99",
        python_callable=validate_results,
        provide_context=True,
    )

    send_notification_if_data_is_not_greater_than_99 = EmailOperator(
        task_id="send_notification_if_data_is_not_greater_than_99",
        to="max.espejo@intelica.com",
        subject="DAG {{ dag.dag_id }} has stopped because precision is not greater than 99 ",
        html_content="""
        <p>The task <strong>{{ task_instance.task_id }}</strong> in DAG <strong>{{ dag.dag_id }}</strong> failed.</p>
        <p><strong>Execution date:</strong> {{ execution_date }}</p>
        <p>Check the task log for more details.</p>
        """,
    )

    generate_and_send_report = SQLExecuteQueryOperator(
        task_id="generate_and_send_report",
        conn_id="mssql_default",
        sql="""EXEC [TABRDRO_RPT].[dbo].[ETL_BRDRO_ORASRV] 
               @BGN_PRC_DT='2024-12-16', 
               @END_PRC_DT='2024-12-16', 
               @BGN_STP_ID=1, 
               @END_STP_ID=2"""
        ,
        parameters={
            "BGN_PRC_DT": "{{ params.start_date }}",
            "END_PRC_DT": "{{params.end_date}}",
        },
        hook_params={"autocommit": True},
        show_return_value_in_logs = True,
    )

    send_notification_if_dag_success = EmailOperator(
        task_id="send_notification_if_dag_success",
        to="max.espejo@intelica.com",
        subject="The DAG {{ dag.dag_id }} completed successfully",
        html_content="""
        <p><strong>Execution date:</strong> {{ execution_date }}</p>
        <p>Check the task log for more details.</p>
        """,
    )

    (
        check_and_get_precision
        >> validate_if_precision_is_greater_than_99
        >> [generate_and_send_report, send_notification_if_data_is_not_greater_than_99]
    )
    generate_and_send_report >> send_notification_if_dag_success
