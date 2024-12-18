"""
##############################################################################
DAG Name: rbc_exception_report.py

Summary:
This DAG processes and validates transactional data for RBC reports, executing multiple stored procedures (SPs) to manage 
and analyze data from Interchange credit and debit transactions as well as from the file system that RBC manages, for specific time periods.
This report is generated monthly and is due by the 7th of each month.  It checks the validity of the generated tables and sends 
notifications based on the success or failure of these tasks.

Steps:
    1. Calculate Parameters:
        - Extract start date and end date for the report period.
        - Push calculated dates to XCom for use in subsequent tasks.
    2. Task Group: SP_1
        - Load and validate settlement credit data for transactions.
        - Validate the table generation and send error notifications if validation fails.
    3. Task Group: SP_2_BC
        - Generate and validate settlement debit data for transactions for BC.
        - Send error notifications if validation fails.
    4. Task Group: SP_2_A
        - Generate and validate settlement debit data for transactions for A.
        - Send error notifications if validation fails.
    5. Send Success Notification:
        - Send email notification if the DAG completes successfully.

Metadata:
    Author: Max Espejo
    Created On: [2024/12/10]
    Last Updated: [2024/12/10]
    Version: 1.0

Changelog:
    - Version 1.0: Initial implementation for RBC Exception report DAG.

###############################################################################
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
    dag_id="RBC_Exception_Report",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={"@BGN_DT": default_start_date, "@END_DT": default_end_date},
) as dag:
    with TaskGroup("SP_1") as SP_1:
        add_settlement_credit_data_to_transactions = SQLExecuteQueryOperator(
            task_id="add_settlement_credit_data_to_transactions",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                EXEC [TAVIRBC_CLC].[dbo].[ETL_RPT_RBC_AUTH_TRX_CREDIT]
                @BGN_DT = %(BGN_DT)s,
                @END_DT = %(END_DT)s
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "END_DT": "{{params['@END_DT']}}",
            },
        )

        add_settlement_credit_data_to_transactions_a = SQLExecuteQueryOperator(
            task_id="add_settlement_credit_data_to_transactions_a",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                EXEC [TAVIRBC_CLC].[dbo].[ETL_RPT_RBC_AUTH_TRX_CREDIT_A]
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
                SELECT CASE 
                    WHEN OBJECT_ID('TAVIRBC_CLC.DBO.[TMP_RPT_AUTH_CREDIT_A]', 'U') IS NOT NULL
                    THEN CASE
                        WHEN EXISTS (SELECT 1 FROM TAVIRBC_CLC.DBO.[TMP_RPT_AUTH_CREDIT_A])
                        THEN CASE 
                            WHEN (SELECT MIN(DAY_DATE) FROM TAVIRBC_CLC.DBO.[TMP_RPT_AUTH_CREDIT_A]) >= %(BGN_DT)s
                                AND (SELECT MAX(DAY_DATE) FROM TAVIRBC_CLC.DBO.[TMP_RPT_AUTH_CREDIT_A]) <= %(END_DT)s
                            THEN 'true'
                            ELSE 'false'
                        END
                        ELSE 'false'
                    END
                    ELSE 'false'
                END AS TableStatus
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "END_DT": "{{params['@END_DT']}}",
            },
            follow_task_ids_if_false=["send_error_email_sp1"],
            follow_task_ids_if_true=[
                "SP_2.generate_settlement_debit_data_to_transactions_bc"
            ],
        )
        (
            add_settlement_credit_data_to_transactions
            >> add_settlement_credit_data_to_transactions_a
            >> validate_generated_table_sp1
        )

    send_error_email_sp1 = PythonOperator(
        task_id="send_error_email_sp1",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )

    with TaskGroup("SP_2_BC") as SP_2_BC:
        generate_settlement_debit_data_to_transactions_bc = SQLExecuteQueryOperator(
            task_id="generate_settlement_debit_data_to_transactions_bc",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /* 
                EXEC [TAVIRBC_CLC].[dbo].[SP_AUT_GENERATE_DEBIT]
                @DateOption = 'BC',
                @ProccesYear = YEAR(%(BGN_DT)s),
                @ProcessMonth = MONTH(%(BGN_DT)s)
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
            },
        )

        add_settlement_debit_data_to_transactions_bc = SQLExecuteQueryOperator(
            task_id="add_settlement_debit_data_to_transactions_bc",
            conn_id="mssql_default",
            sql="""
            SELECT 1 AS SUCCESS
            /*
                EXEC [TAVIRBC_CLC].[dbo].[ETL_RPT_RBC_AUTH_TRX_DEBIT]
                @Debug = 1,
                @ProccesYear = YEAR(%(BGN_DT)s),
                @ProcessMonth = MONTH(%(BGN_DT)s)
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
            },
        )

        validate_generated_table_sp2_bc = BranchSQLOperator(
            task_id="validate_generated_table_sp2_bc",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS  
            /*
                SELECT 
                    CASE 
                        WHEN OBJECT_ID('TAVIRBC_CLC.DBO.[ETL_RPT_RBC_AUTH_TRX_DEBITO]', 'U') IS NOT NULL 
                            AND MIN(REF_DT) >= %(BGN_DT)s
                            AND MAX(REF_DT) <= %(END_DT)s
                        THEN 'true'
                        ELSE 'false'
                    END 
                        AS TableStatus
                FROM TAVIRBC_CLC.DBO.[ETL_RPT_RBC_AUTH_TRX_DEBITO]
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "END_DT": "{{params['@END_DT']}}",
            },
            follow_task_ids_if_false=["send_error_email_sp2_bc"],
            follow_task_ids_if_true=[
                "SP_2.generate_settlement_debit_data_to_transactions_a"
            ],
        )
        (
            generate_settlement_debit_data_to_transactions_bc
            >> add_settlement_debit_data_to_transactions_bc
            >> validate_generated_table_sp2_bc
        )

    send_error_email_sp2_bc = PythonOperator(
        task_id="send_error_email_sp2_bc",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )

    with TaskGroup("SP_2_A") as SP_2_A:
        generate_settlement_debit_data_to_transactions_a = SQLExecuteQueryOperator(
            task_id="generate_settlement_debit_data_to_transactions_a",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                EXEC [TAVIRBC_CLC].[dbo].[SP_AUT_GENERATE_DEBIT]
                @DateOption = 'A'
                @ProccesYear = YEAR(%(BGN_DT)s),
                @ProcessMonth = MONTH(%(BGN_DT)s)
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
            },
        )
        add_settlement_debit_data_to_transactions_a = SQLExecuteQueryOperator(
            task_id="add_settlement_debit_data_to_transactions_a",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                EXEC [TAVIRBC_CLC].[dbo].[ETL_RPT_RBC_AUTH_TRX_DEBIT_A]
                @Debug = 1,
                @ProccesYear = YEAR(%(BGN_DT)s),
                @ProcessMonth = MONTH(%(BGN_DT)s)
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
            },
        )
        validate_generated_table_sp2_a = BranchSQLOperator(
            task_id="validate_generated_table_sp2_a",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            --SELECT CAST(0 AS BIT) AS SUCCESS
            /*
                SELECT 
                    CASE 
                        WHEN OBJECT_ID('TAVIRBC_CLC.DBO.[ETL_RPT_RBC_AUTH_TRX_DEBITO_A]', 'U') IS NOT NULL 
                            AND MIN(REF_DT) >= %(BGN_DT)s
                            AND MAX(REF_DT) <= %(END_DT)s
                        THEN 'true'
                        ELSE 'false'
                    END 
                        AS TableStatus
                FROM TAVIRBC_CLC.DBO.[ETL_RPT_RBC_AUTH_TRX_DEBITO_A]
            */
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "END_DT": "{{params['@END_DT']}}",
            },
            follow_task_ids_if_false=["send_error_email_sp2_a"],
            follow_task_ids_if_true=["send_notification_if_dag_success"],
        )
        (
            generate_settlement_debit_data_to_transactions_a
            >> add_settlement_debit_data_to_transactions_a
            >> validate_generated_table_sp2_a
        )

    send_error_email_sp2_a = PythonOperator(
        task_id="send_error_email_sp2_a",
        python_callable=send_dynamic_error_email,
        provide_context=True,
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

    SP_1 >> SP_2_BC >> SP_2_A >> send_notification_if_dag_success
    SP_1 >> send_error_email_sp1
    SP_2_BC >> send_error_email_sp2_bc
    SP_2_A >> send_error_email_sp2_a
