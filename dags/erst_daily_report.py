from datetime import datetime

import pandas as pd
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


def _validate_mastercard_adt_data(**kwargs):
    mastercard_outgoing_data = kwargs["ti"].xcom_pull(
        task_ids="mastercard_validation.get_mastercard_outgoing_adt_table"
    )

    mastercard_incoming_data = kwargs["ti"].xcom_pull(
        task_ids="mastercard_validation.get_mastercard_incoming_adt_table"
    )

    column_headers = [
        "[Diff. Settlement Amount]",
        "[Diff. Interchange Debit]",
        "[Diff. Interchange Credit]",
        "[Diff. Interchange Credit - Debit]",
        "[Diff. Interchange Net]",
        "[Diff. Count]",
    ]

    df_mc_out = pd.DataFrame(mastercard_outgoing_data, columns=column_headers)
    print(df_mc_out)
    df_mc_out_validation = df_mc_out[
        [
            "[Diff. Settlement Amount]",
            "[Diff. Interchange Credit - Debit]",
            "[Diff. Interchange Net]",
            "[Diff. Count]",
        ]
    ]

    df_mc_in = pd.DataFrame(mastercard_incoming_data, columns=column_headers)
    print(df_mc_in)
    df_mc_in_validation = df_mc_in[
        [
            "[Diff. Settlement Amount]",
            "[Diff. Interchange Credit - Debit]",
            "[Diff. Interchange Net]",
            "[Diff. Count]",
        ]
    ]

    return (0 == df_mc_out_validation.to_numpy()).all() and (
        0 == df_mc_in_validation.to_numpy()
    ).all()

    # kwargs["ti"].xcom_push(key="validation_result", value=result)


def _validate_visa_adt_data(**kwargs):
    visa_outgoing_data = kwargs["ti"].xcom_pull(
        task_ids="visa_validation.get_visa_outgoing_adt_table"
    )

    visa_incoming_data = kwargs["ti"].xcom_pull(
        task_ids="visa_validation.get_visa_incoming_adt_table"
    )
    column_headers = [
        "[Diff. Interchange Amount Credit]",
        "[Diff. Interchange Amount Debit]",
        "[Diff. Interchange Fee Credit]",
        "[Diff. Interchange Fee Debit]",
        "[Diff. Interchange Fee Debit - Interchange Fee Credit]",
        "[Diff. Count]",
    ]

    df_vi_out = pd.DataFrame(visa_outgoing_data, columns=column_headers)
    df_vi_out_validation = df_vi_out[
        [
            "[Diff. Interchange Amount Credit]",
            "[Diff. Interchange Amount Debit]",
            "[Diff. Interchange Fee Debit - Interchange Fee Credit]",
            "[Diff. Count]",
        ]
    ]
    df_vi_in = pd.DataFrame(visa_incoming_data, columns=column_headers)
    df_vi_in_validation = df_vi_in[
        [
            "[Diff. Interchange Amount Credit]",
            "[Diff. Interchange Amount Debit]",
            "[Diff. Interchange Fee Debit - Interchange Fee Credit]",
            "[Diff. Count]",
        ]
    ]
    print(df_vi_in_validation)
    print(df_vi_out_validation)
    return (0.0 == df_vi_out_validation.to_numpy()).all() and (
        0.0 == df_vi_in_validation.to_numpy()
    ).all()

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
                    CAST((G85_SET_AMT - TXL_SET_AMT) AS FLOAT)  AS [DIFF_SET_AMT],
                    CAST((G85_ITX_DB_FEE - TXL_ITX_DB_FEE) AS FLOAT) AS [DIFF_ITX_DEBIT],
                    CAST((G85_ITX_CR_FEE - TXL_ITX_CR_FEE) AS FLOAT) AS [DIFF_ITX_CREDIT],
                    CAST(((G85_ITX_CR_FEE - TXL_ITX_CR_FEE)+(G85_ITX_DB_FEE - TXL_ITX_DB_FEE)) AS FLOAT) AS [DIFF_ITX_CD_DB],
                    CAST((G85_ITX_FEE - TXL_ITX_FEE) AS FLOAT) AS [DIFF_ITX_NET],
                    CAST((G85_TRX_CNT - TXL_TRX_CNT) AS FLOAT) AS [DIFF_COUNT]
                FROM [TAERSTHU_RPT].[dbo].[RPT_MC_ADT_DETAIL] WITH (NOLOCK)
                WHERE 1=1
                AND STT_DT = %(DT)s
                AND BMOD_DESC = 'ACQ'         
            """,
            parameters={"DT": "{{params['current_date']}}"},
            do_xcom_push=True,
        )
        get_mastercard_incoming_adt_table = SQLExecuteQueryOperator(
            task_id="get_mastercard_incoming_adt_table",
            conn_id="mssql_default",
            sql="""
                SELECT
                    CAST((G85_SET_AMT - TXL_SET_AMT) AS FLOAT) AS [DIFF_SET_AMT],
                    CAST((G85_ITX_DB_FEE - TXL_ITX_DB_FEE) AS FLOAT) AS [DIFF_ITX_DEBIT],
                    CAST((G85_ITX_CR_FEE - TXL_ITX_CR_FEE) AS FLOAT) AS [DIFF_ITX_CREDIT],
                    CAST(((G85_ITX_CR_FEE - TXL_ITX_CR_FEE)+(G85_ITX_DB_FEE - TXL_ITX_DB_FEE)) AS FLOAT) AS [DIFF_ITX_CD_DB],
                    CAST((G85_ITX_FEE - TXL_ITX_FEE) AS FLOAT) AS [DIFF_ITX_NET],
                    CAST((G85_TRX_CNT - TXL_TRX_CNT) AS FLOAT) AS [DIFF_COUNT]
                FROM [TAERSTHU_RPT].[dbo].[RPT_MC_ADT_DETAIL] WITH (NOLOCK)
                WHERE 1=1
                AND STT_DT = %(DT)s
                AND BMOD_DESC = 'ISS'
            """,
            parameters={"DT": "{{params['current_date']}}"},
            do_xcom_push=True,
        )
        validate_mastercard_adt_data = PythonOperator(
            task_id="validate_mastercard_adt_data",
            python_callable=_validate_mastercard_adt_data,
            do_xcom_push=True,
        )
        [
            get_mastercard_incoming_adt_table,
            get_mastercard_outgoing_adt_table,
        ] >> validate_mastercard_adt_data
    
    with TaskGroup("visa_validation") as VV:
        get_visa_outgoing_adt_table = SQLExecuteQueryOperator(
            task_id="get_visa_outgoing_adt_table",
            conn_id="mssql_default",
            sql="""
                SELECT
                    COALESCE(CAST((TRX_CR_AMT - VSS_CR_AMT) AS FLOAT),0) AS [DIFF_CR_AMT], -- OK
                    COALESCE(CAST((TRX_DB_AMT - VSS_DB_AMT) AS FLOAT),0) AS [DIFF_DB_AMT], --OK
                    COALESCE(CAST((TRX_CR_FEE - VSS_CR_FEE) AS FLOAT),0) AS [DIFF_CR_FEE],
                    COALESCE(CAST((TRX_DB_FEE - VSS_DB_FEE) AS FLOAT),0) AS [DIFF_DB_FEE],
                    COALESCE(CAST(((TRX_DB_FEE - VSS_DB_FEE)+(TRX_CR_FEE - VSS_CR_FEE)) AS FLOAT),0) AS [DIFF_CD_DB], --OK
                    COALESCE(CAST((TRX_TRX_CNT - VSS_TRX_CNT) AS FLOAT),0) AS [DIFF_COUNT] -- OK
                FROM [TAERSTHU_RPT].[dbo].[RPT_VI_ADT_DETAIL] WITH (NOLOCK)
                WHERE 1=1
                AND STT_DT = %(DT)s
                AND BMOD_DESC = 'ACQ'    
            """,
            parameters={"DT": "{{params['current_date']}}"},
            do_xcom_push=True,
        )
        get_visa_incoming_adt_table = SQLExecuteQueryOperator(
            task_id="get_visa_incoming_adt_table",
            conn_id="mssql_default",
            sql="""
                SELECT
                    COALESCE(CAST((TRX_CR_AMT - VSS_CR_AMT) AS FLOAT),0) AS [DIFF_CR_AMT], --OK
                    COALESCE(CAST((TRX_DB_AMT - VSS_DB_AMT) AS FLOAT),0) AS [DIFF_DB_AMT], --OK
                    COALESCE(CAST((TRX_CR_FEE - VSS_CR_FEE) AS FLOAT),0) AS [DIFF_CR_FEE],
                    COALESCE(CAST((TRX_DB_FEE - VSS_DB_FEE) AS FLOAT),0) AS [DIFF_DB_FEE],
                    COALESCE(CAST(((TRX_DB_FEE - VSS_DB_FEE)+(TRX_CR_FEE - VSS_CR_FEE)) AS FLOAT),0) AS [DIFF_CD_DB], --OK
                    COALESCE(CAST((TRX_TRX_CNT - VSS_TRX_CNT) AS FLOAT),0) AS [DIFF_COUNT] --OK
                FROM [TAERSTHU_RPT].[dbo].[RPT_VI_ADT_DETAIL] WITH (NOLOCK)
                WHERE 1=1
                AND STT_DT = %(DT)s
                AND BMOD_DESC = 'ISS'
            """,
            parameters={"DT": "{{params['current_date']}}"},
            do_xcom_push=True,
        )
        validate_visa_adt_data = PythonOperator(
            task_id="validate_visa_adt_data",
            python_callable=_validate_visa_adt_data,
            do_xcom_push=True,
        )

        [
            get_visa_incoming_adt_table,
            get_visa_outgoing_adt_table,
        ] >> validate_visa_adt_data
