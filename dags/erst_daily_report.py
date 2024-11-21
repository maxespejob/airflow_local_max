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

def validate_mastercard_adt_data():
    return True

def validate_visa_adt_data():
    return True

default_args = {
    "owner": "Max E.",
    "email": ["max.espejo@intelica.com"],
    "do_xcom_push": False,
    "email_on_failure": True,
    #'retries': 1,
}


with DAG(
    dag_id="ERSTE_Daily_Report",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={"current_date": datetime.today().strftime("%Y-%m-%d")},
) as dag:
    with TaskGroup("mastercard_validation") as MV:
        get_mastercard_outgoing_adt_table = SQLExecuteQueryOperator(
            task_id="get_mastercard_outgoing_adt_table",
            conn_id="mssql_default",
            sql="""
                SELECT
                    (G85_SET_AMT - TXL_SET_AMT) AS [DIFF_SET_AMT],
                    (G85_ITX_DB_FEE - TXL_ITX_DB_FEE) AS [DIFF_ITX_DEBIT],
                    (G85_ITX_CR_FEE - TXL_ITX_CR_FEE) AS [DIFF_ITX_CREDIT],
                    ((G85_ITX_CR_FEE - TXL_ITX_CR_FEE)+(G85_ITX_DB_FEE - TXL_ITX_DB_FEE)) AS [DIFF_ITX_CD_DB],
                    (G85_ITX_FEE - TXL_ITX_FEE) AS [DIFF_ITX_NET],
                    (G85_TRX_CNT - TXL_TRX_CNT) AS [DIFF_COUNT]
                FROM [TAERSTHU_RPT].[dbo].[RPT_MC_ADT_DETAIL] WITH (NOLOCK)
                WHERE 1=1
                AND STT_DT = %(DT)s
                AND BMOD_DESC = 'ACQ'
                GO            
            """,
            parameters={"DT": "{{params['current_date']}}"},
            do_xcom_push = True,
        )
        get_mastercard_incoming_adt_table = SQLExecuteQueryOperator(
            task_id="get_mastercard_incoming_adt_table",
            conn_id="mssql_default",
            sql="""
                SELECT
                    --G85_SET_AMT,TXL_SET_AMT,
                    --G85_ITX_DB_FEE,TXL_ITX_DB_FEE,
                    --G85_ITX_CR_FEE,TXL_ITX_CR_FEE,
                    --G85_ITX_FEE,TXL_ITX_FEE,
                    --G85_TRX_CNT,TXL_TRX_CNT,
                    (G85_SET_AMT - TXL_SET_AMT) AS [DIFF_SET_AMT],
                    (G85_ITX_DB_FEE - TXL_ITX_DB_FEE) AS [DIFF_ITX_DEBIT],
                    (G85_ITX_CR_FEE - TXL_ITX_CR_FEE) AS [DIFF_ITX_CREDIT],
                    ((G85_ITX_CR_FEE - TXL_ITX_CR_FEE)+(G85_ITX_DB_FEE - TXL_ITX_DB_FEE)) AS [DIFF_ITX_CD_DB],
                    (G85_ITX_FEE - TXL_ITX_FEE) AS [DIFF_ITX_NET],
                    (G85_TRX_CNT - TXL_TRX_CNT) AS [DIFF_COUNT]
                FROM [TAERSTHU_RPT].[dbo].[RPT_MC_ADT_DETAIL] WITH (NOLOCK)
                WHERE 1=1
                AND STT_DT = %(DT)s
                AND BMOD_DESC = 'ISS'
                GO
            """,
            parameters={"DT": "{{params['current_date']}}"},
            do_xcom_push = True,
        )

    with TaskGroup("visa_validation") as VV:
        get_visa_outgoing_adt_table = SQLExecuteQueryOperator(
            task_id="get_visa_outgoing_adt_table",
            conn_id="mssql_default",
            sql="""
                --Outgoing VISA
                SELECT
                    (TRX_CR_AMT - VSS_CR_AMT) AS [DIFF_CR_AMT],
                    (TRX_DB_AMT - VSS_DB_AMT) AS [DIFF_DB_AMT],
                    (TRX_CR_FEE - VSS_CR_FEE) AS [DIFF_CR_FEE],
                    (TRX_DB_FEE - VSS_DB_FEE) AS [DIFF_DB_FEE],
                    ((TRX_DB_FEE - VSS_DB_FEE)+(TRX_CR_FEE - VSS_CR_FEE))AS [DIFF_CD_DB]
                FROM [TAERSTHU_RPT].[dbo].[RPT_VI_ADT_DETAIL] WITH (NOLOCK)
                WHERE 1=1
                AND STT_DT = %(DT)s
                AND BMOD_DESC = 'ACQ'
                GO            
            """,
            parameters={"DT": "{{params['current_date']}}"},
            do_xcom_push = True,
        )
        get_visa_incoming_adt_table = SQLExecuteQueryOperator(
            task_id="get_visa_incoming_adt_table",
            conn_id="mssql_default",
            sql="""
                SELECT
                    (TRX_CR_AMT - VSS_CR_AMT) AS [DIFF_CR_AMT],
                    (TRX_DB_AMT - VSS_DB_AMT) AS [DIFF_DB_AMT],
                    (TRX_CR_FEE - VSS_CR_FEE) AS [DIFF_CR_FEE],
                    (TRX_DB_FEE - VSS_DB_FEE) AS [DIFF_DB_FEE],
                    ((TRX_DB_FEE - VSS_DB_FEE)+(TRX_CR_FEE - VSS_CR_FEE))AS [DIFF_CD_DB]
                FROM [TAERSTHU_RPT].[dbo].[RPT_VI_ADT_DETAIL] WITH (NOLOCK)
                WHERE 1=1
                AND STT_DT = %()s
                AND BMOD_DESC = 'ISS'
                GO       
            """,
            parameters={"DT": "{{params['current_date']}}"},
            do_xcom_push = True,
        )
    