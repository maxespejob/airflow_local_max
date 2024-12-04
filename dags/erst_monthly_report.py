from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLExecuteQueryOperator,
)
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
    dag_id="ERSTE_Monthly_Report",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={"start_date": default_start_date, "end_date": default_end_date},
) as dag:
    update_visa_transactions_table = SQLExecuteQueryOperator(
        task_id="update_visa_transactions_table",
        conn_id="mssql_default",
        sql="""
        SELECT 1 AS SUCCESS 
        /*
           EXEC TAERSTHU_RPT.dbo.ETL_VI_FM_TXN 
           @MONTH = SUBSTRING(CONVERT(VARCHAR,%(BGN_DT)s,112),1,6),
           @BGN_DT = %(BGN_DT)s,
           @END_DT = %(END_DT)s 
        */
        """,
        do_xcom_push=True,
        parameters={
            "BGN_DT": "{{params.start_date}}",
            "END_DT": "{{params.end_date}}",
        },
    )

    update_mastercard_transactions_table = SQLExecuteQueryOperator(
        task_id="update_mastercard_transactions_table",
        conn_id="mssql_default",
        sql="""
        SELECT 1 AS SUCCESS
        /*
           EXEC TAERSTHU_RPT.dbo.ETL_MC_FM_TXN 
           @MONTH = SUBSTRING(CONVERT(VARCHAR,%(BGN_DT)s,112),1,6),
           @BGN_DT = %(BGN_DT)s,
           @END_DT = %(END_DT)s 
        */
        """,
        do_xcom_push=True,
        parameters={
            "BGN_DT": "{{params.start_date}}",
            "END_DT": "{{params.end_date}}",
        },
    )

    validate_visa_and_mastercard_tables = BranchSQLOperator(
        task_id="validate_visa_and_mastercard_tables",
        conn_id="mssql_default",
        sql="""
                SELECT CASE
                    WHEN 
                        -- Validación de la tabla Visa
                        (
                            OBJECT_ID('[TAERSTHU_RPT].[dbo].[FM_VI_TXN_FRAC]', 'U') IS NOT NULL
                            AND EXISTS (
							SELECT 1 
							FROM [TAERSTHU_RPT].[dbo].[FM_VI_TXN_FRAC]
							WHERE [MONTH_ID]= SUBSTRING(CONVERT(VARCHAR,CAST( (%(BGN_DT)s) AS smalldatetime),112),1,6)
							)
						)
                        AND 
                        -- Validación de la tabla Mastercard
                        (
                            OBJECT_ID('[TAERSTHU_RPT].[dbo].[FM_MC_TXN_FRAC]', 'U') IS NOT NULL
                            AND EXISTS (
							SELECT 1 
							FROM [TAERSTHU_RPT].[dbo].[FM_MC_TXN_FRAC] 
							WHERE [MONTH_ID] = SUBSTRING(CONVERT(VARCHAR,CAST( (%(BGN_DT)s) AS smalldatetime),112),1,6)
							)
                        )
                    THEN 'true'
                    ELSE 'false'
                END AS TableStatus;            
        """,
        parameters={
            "BGN_DT": "{{params['start_date']}}",
        },
        follow_task_ids_if_false="send_error_email",
        follow_task_ids_if_true="send_success_email",
    )

    send_error_email = PythonOperator(
        task_id="send_error_email", python_callable=send_dynamic_error_email
    )

    send_success_email = EmptyOperator(task_id="send_success_email")

    (
        [update_visa_transactions_table, update_mastercard_transactions_table]
        >> validate_visa_and_mastercard_tables
        >> [send_error_email, send_success_email]
    )
