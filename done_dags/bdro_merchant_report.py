"""
###############################################################################
DAG Name: bdro_merchant_report.py

Summary:
    This DAG automates the generation and validation of merchant reports for Visa and MasterCard transactions.
    This report is run on a monthly basis and is expected to be received by the 10th of each month. 
    In addition, data accuracy must be guaranteed to exceed 99% before finalizing and submitting the report. 
    Notifications are sent in case of both success and failure.

Steps:
    1. Generate temporary tables for Visa and MasterCard transactions.
    2. Execute stored procedures to process and validate transaction data.
    3. Branch based on validation results:
       - If precision > 99%, generate and send the merchant report.
       - Otherwise, send a failure notification via email.
    4. Notify stakeholders upon successful DAG completion.

Metadata:
    Author: Max Espejo, Julio Cardenas
    Created On: [2024/12/10]
    Last Updated: [2024/12/10]
    Version: 1.0

Changelog:
    - Version 1.0: Initial implementation of the DAG for merchant report generation.

###############################################################################
"""


from datetime import datetime
from typing import List

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dateutil.relativedelta import relativedelta


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
    default_start_date = (
        (today - relativedelta(months=1)).replace(day=1).strftime("%Y-%m-%d")
    )
    default_end_date = (today.replace(day=1) - relativedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

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
    dag_id="BDRO_Merchant_Report",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={"@BGN_DT": default_start_date, "@END_DT": default_end_date},
) as dag:
    generate_temporal_visa_and_mastercard_tables = SQLExecuteQueryOperator(
        task_id="generate_visa_and_mastercard_temporal_tables",
        conn_id="mssql_default",
        sql="""
        SELECT 1 AS SUCCESS 
            /*
                EXEC [TABRDRO_RPT].[dbo].[ETL_RPT_MERCHANT_GENERATE_MTH]
                @BGN_DT = %(BGN_DT)s,
                @END_DT = %(END_DT)s
            */
        """,
        parameters={
            "BGN_DT": "{{params['@BGN_DT']}}",
            "END_DT": "{{params['@END_DT']}}",
        },
    )

    check_and_get_precision = SQLExecuteQueryOperator(
        task_id="check_and_get_precision",
        conn_id="mssql_default",
        sql="""EXEC [TABRDRO_RPT].[dbo].[ETL_RPT_MERCHANT_MTHLY_RPT]
               @BGN_PRC_DT=%(BGN_PRC_DT)s, 
               @END_PRC_DT=%(END_PRC_DT)s, 
               @BGN_STP_ID=1, 
               @END_STP_ID=1""",
        parameters={
            "BGN_PRC_DT": "{{ params.start_date }}",
            "END_PRC_DT": "{{params.end_date}}",
        },
        do_xcom_push=True,
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

    generate_and_send_merchant_report = SQLExecuteQueryOperator(
        task_id="generate_and_send_merchant_report",
        conn_id="mssql_default",
        sql="""
            EXEC [TABRDRO_RPT].[dbo].[ETL_RPT_MERCHANT_MTHLY_RPT] 
               @BGN_PRC_DT=%(BGN_PRC_DT)s, 
               @END_PRC_DT=%(END_PRC_DT)s, 
               @BGN_STP_ID=1, 
               @END_STP_ID=1
        """,
        parameters={
            "BGN_PRC_DT": "{{ params.start_date }}",
            "END_PRC_DT": "{{params.end_date}}",
        },
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
        generate_temporal_visa_and_mastercard_tables
        >> check_and_get_precision
        >> validate_if_precision_is_greater_than_99
        >> [
            send_notification_if_data_is_not_greater_than_99,
            generate_and_send_merchant_report,
        ]
    )
    generate_and_send_merchant_report >> send_notification_if_dag_success
