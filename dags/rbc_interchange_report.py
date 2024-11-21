"""
Here there is a brief description of what this report is about
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLExecuteQueryOperator,
)
from airflow.utils.task_group import TaskGroup
from dateutil.relativedelta import relativedelta
from utils.branch_email_util import send_dynamic_error_email
from utils.hello_world import hello_world

# from utils.generate_rbc_reports import main


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
    dag_id="RBC_Interchange_Report",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={"@BGN_DT": default_start_date, "@END_DT": default_end_date},
) as dag:
    with TaskGroup("SP_1") as SP_1:
        get_settlement_credit_data = SQLExecuteQueryOperator(
            task_id="get_settlement_credit_data",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                EXEC [ITLRPT].[dbo].[ETL_RPT_REPORT_MTH_VI_RBC_ISS_CREDIT]
                @BGN_DT = %(BGN_DT)s,
                @END_DT = %(END_DT)s
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "END_DT": "{{params['@END_DT']}}",
            },
        )
        validate_generated_table_sp1 = BranchSQLOperator(
            task_id="validate_generated_table_sp1",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                SELECT 
                    CASE 
                        WHEN OBJECT_ID('[ITLRPT].[dbo].[RPT_ITX_RBC_ISS_CREDIT]', 'U') IS NOT NULL 
                            AND MIN(SettlementDate) >= %(BGN_DT)s
                            AND MAX(SettlementDate) <= %(END_DT)s
                        THEN 'true'
                        ELSE 'false'
                    END 
                        AS TableStatus
                FROM [ITLRPT].[dbo].[RPT_ITX_RBC_ISS_CREDIT]
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "END_DT": "{{params['@END_DT']}}",
            },
            follow_task_ids_if_false=["send_error_email_sp1"],
            follow_task_ids_if_true=["SP_2.get_settlement_debit_data"],
        )
        (get_settlement_credit_data >> validate_generated_table_sp1)

    send_error_email_sp1 = PythonOperator(
        task_id="send_error_email_sp1",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )

    with TaskGroup("SP_2") as SP_2:
        get_settlement_debit_data = SQLExecuteQueryOperator(
            task_id="get_settlement_debit_data",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                EXEC [ITLRPT].[dbo].[ETL_RPT_REPORT_MTH_VI_RBC_ISS_DEBIT]
                @BGN_DT = %(BGN_DT)s,
                @END_DT = %(END_DT)s
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "END_DT": "{{params['@END_DT']}}",
            },
        )

        validate_generated_table_sp2 = BranchSQLOperator(
            task_id="validate_generated_table_sp2",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                SELECT 
                    CASE 
                        WHEN OBJECT_ID('[ITLRPT].[dbo].[RPT_ITX_RBC_ISS_DEBIT]', 'U') IS NOT NULL 
                            AND MIN(SettlementDate) >= %(BGN_DT)s
                            AND MAX(SettlementDate) <= %(END_DT)s
                        THEN 'true'
                        ELSE 'false'
                    END 
                        AS TableStatus
                FROM [ITLRPT].[dbo].[RPT_ITX_RBC_ISS_DEBIT]
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "END_DT": "{{params['@END_DT']}}",
            },
            follow_task_ids_if_false=["send_error_email_sp2"],
            follow_task_ids_if_true=[
                "SP_2.generate_settlement_debit_data_to_transactions_a"
            ],
        )

        (get_settlement_debit_data >> validate_generated_table_sp2)

    send_error_email_sp2 = PythonOperator(
        task_id="send_error_email_sp2",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )
    run_generate_rbc_report_script = PythonOperator(
        task_id="run_generate_rbc_report_script",
        python_callable=hello_world,
        # python_callable = main
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

    SP_1 >> SP_2 >> run_generate_rbc_report_script >> send_notification_if_dag_success
    validate_generated_table_sp2 >> send_error_email_sp2
    SP_1 >> send_error_email_sp1
    SP_2 >> send_error_email_sp2
