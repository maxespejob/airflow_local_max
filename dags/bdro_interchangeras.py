"""
Doing
"""

# Pendiente
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    BranchSQLOperator,
)
from airflow.utils.task_group import TaskGroup
from dateutil.relativedelta import relativedelta
from utils.branch_email_util import send_dynamic_error_email
from pruebas.web_scraping_v2 import web_scraping_bnr

import pandas as pd

def default_dag_parameters() -> tuple[str, str]:
    today = datetime.today()
    default_start_date = (
        (today - relativedelta(months=1)).replace(day=1).strftime("%Y-%m-%d")
    )
    default_end_date = (today.replace(day=1) - relativedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    return default_start_date, default_end_date


def _verify_correct_web_scraping(**kwargs):
    # Extraer XComs
    scraped_exchange_rate = kwargs['ti'].xcom_pull(task_ids='accounting_reconciliation_report.scrape_current_exchange_rate_data_from_bnr')
    db_exchange_rate = kwargs['ti'].xcom_pull(task_ids='accounting_reconciliation_report.get_past_month_exchange_rate_from_db')

    # Procesar el primer XCom (web scraping)
    columns_scraped = scraped_exchange_rate[0]
    data_scraped = scraped_exchange_rate[1:]
    df_ws = pd.DataFrame(data_scraped, columns=columns_scraped)
    df_ws_filtered = df_ws[["Month", "EUR", "USD"]].copy()  # Filtrar columnas necesarias

    # Procesar el segundo XCom (base de datos)
    df_db_unpivoted = pd.DataFrame(db_exchange_rate, columns=["Month", "Currency", "Value"])
    df_db = df_db_unpivoted.pivot(index="Month", columns="Currency", values="Value").reset_index()

    # Convertir fechas a formato consistente
    df_ws_filtered["Month"] = pd.to_datetime(df_ws_filtered["Month"]).dt.to_period('M').astype(str)
    df_db["Month"] = pd.to_datetime(df_db["Month"]).dt.to_period('M').astype(str)

    # Filtrar el mes de interés en df_ws_filtered
    db_month = df_db["Month"].iloc[0]  # Tomar el único mes en df_db_filtered
    df_ws_month = df_ws_filtered[df_ws_filtered["Month"] == db_month]

    if df_ws_month.empty:
        print(f"Advertencia: No se encontraron datos para el mes {db_month} en df_ws_filtered.")
        return False

    # Comparar valores de EUR y USD
    eur_db = float(df_db.loc[df_db["Month"] == db_month, "EUR"].values[0])
    usd_db = float(df_db.loc[df_db["Month"] == db_month, "USD"].values[0])

    eur_ws = df_ws_month["EUR"].astype(float).mean()  # Promedio en caso de múltiples valores
    usd_ws = df_ws_month["USD"].astype(float).mean()  # Promedio en caso de múltiples valores

    # Comparación
    tol = 1e-4
    if abs(eur_ws - eur_db) > tol or abs(usd_ws - usd_db) > tol:
        print(f"Discrepancia encontrada para el mes {db_month}:")
        print(f"EUR: Web scraping={eur_ws}, Base de datos={eur_db}")
        print(f"USD: Web scraping={usd_ws}, Base de datos={usd_db}")
        return 'send_error_email_sp3'
    
    current_usd = df_ws_filtered["USD"].head(1).values
    current_eur = df_ws_filtered["EUR"].head(1).values
    print(f"Los valores para el mes {db_month} coinciden correctamente.")
    kwargs["ti"].xcom_push(key='USD',value = current_usd)
    kwargs["ti"].xcom_push(key='EUR',value = current_eur)
    return 'accounting_reconciliation_report.update_exchange_rate_table'


def _is_quarter_data(**kwargs) -> str:
    date_param_str = kwargs["params"].get("start_date", default_start_date)
    date_param_object = datetime.strptime(date_param_str, "%Y-%m-%d")
    current_month = date_param_object.month
    return (
        ["extract_data_for_visa", "extract_data_for_mastercard"]
        if current_month % 3 == 0
        else ""
    )


def _export_data_to_excel(**kwargs):
    return True


default_args = {
    "owner": "Max E.",
    "email": ["max.espejo@intelica.com"],
    "do_xcom_push": False,
    "email_on_failure": True,
    #'retries': 1,
}

default_start_date, default_end_date = default_dag_parameters()

with DAG(
    dag_id="BDRO_InterchangeRas",
    default_args=default_args,
    # schedule = "0 12 * * 1-5",
    schedule=None,
    catchup=False,
    params={"start_date": default_start_date, "end_date": default_end_date},
) as dag:
    with TaskGroup("SP_1") as SP_1:
        consolidate_transactional_information = SQLExecuteQueryOperator(
            task_id="consolidate_transactional_information",
            conn_id="mssql_default",
            sql=""" 
            SELECT 1 AS SUCCESS
            /*
                EXEC [ITLRPT].[dbo].[ETL_RPT_GENERATE_MTH]
                @BGN_PRC_DT = %(BGN_DT)s,
                @END_PRC_DT = %(END_DT)s,
                @PRJ_CD = 'BRDRO'
            */
            """,
            parameters={
                "BGN_DT": "{{params.start_date}}",
                "END_DT": "{{params.end_date}}",
            },
        )

        validate_temporal_tables = BranchSQLOperator(
            task_id="validate_temporal_tables",
            conn_id="mssql_default",
            sql="""
            SELECT 1 AS SUCCESS;
                /*
                SELECT CASE 
                    WHEN 
                        -- Validación de la tabla Visa
                        (
                            OBJECT_ID('[TAVIBRDRO_CLC].[dbo].[TMP_TRX_MTH_INTERCHRAS]', 'U') IS NOT NULL
                            AND EXISTS (SELECT 1 FROM [TAVIBRDRO_CLC].[dbo].[TMP_TRX_MTH_INTERCHRAS])
                            AND (SELECT MIN(REF_DT) FROM [TAVIBRDRO_CLC].[dbo].[TMP_TRX_MTH_INTERCHRAS]) >= DATEADD(DAY,-3,%(BGN_DT)s)
                            AND (SELECT MAX(REF_DT) FROM [TAVIBRDRO_CLC].[dbo].[TMP_TRX_MTH_INTERCHRAS]) <= %(END_DT)s
                        )
                        AND 
                        -- Validación de la tabla Mastercard
                        (
                            OBJECT_ID('[TAMCBRDRO_CLC].[dbo].[TMP_TRX_MTH_INTERCHRAS]', 'U') IS NOT NULL
                            AND EXISTS (SELECT 1 FROM [TAMCBRDRO_CLC].[dbo].[TMP_TRX_MTH_INTERCHRAS])
                            AND (SELECT MIN(CAST(IFP_DT AS DATE)) FROM [TAMCBRDRO_CLC].[dbo].[TMP_TRX_MTH_INTERCHRAS]) >= DATEADD(DAY,-3,%(BGN_DT)s)
                            AND (SELECT MAX(CAST(IFP_DT AS DATE)) FROM [TAMCBRDRO_CLC].[dbo].[TMP_TRX_MTH_INTERCHRAS]) <= %(END_DT)s
                        )
                    THEN 'true'
                    ELSE 'false'
                END AS TableStatus;
                */
            """,
            parameters={
                "BGN_DT": "{{params['start_date']}}",
                "END_DT": "{{params['end_date']}}",
            },
            follow_task_ids_if_false=["send_error_email_sp1"],
            follow_task_ids_if_true=["SP_1.update_parameters_table"],
        )

        update_parameters_table = SQLExecuteQueryOperator(
            task_id="update_parameters_table",
            conn_id="mssql_default",
            sql="""
            SELECT 1 AS SUCCESS
            /*
            UPDATE [ITLCTRL].[dbo].[BRDRO_REPORT_PARAM]
            SET BGN_DT = %(BGN_DT)s, END_DT = %(END_DT)s
            WHERE RPT_ID = 16
            */
            """,
            parameters={
                "BGN_DT": "{{params['start_date']}}",
                "END_DT": "{{params['end_date']}}",
            },
        )

        (
            consolidate_transactional_information
            >> validate_temporal_tables
            >> update_parameters_table
        )

    send_error_email_sp1 = PythonOperator(
        task_id="send_error_email_sp1",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )

    generate_report_tables = SQLExecuteQueryOperator(
        task_id="generate_report_tables",
        conn_id="mssql_default",
        sql="""

        SELECT 1 AS SUCCESS
        --EXEC [ITLCTRL].[dbo].[SP_GETDATACSV4] 2
        
        """,
    )

    with TaskGroup(
        "accounting_reconciliation_report"
    ) as accounting_reconciliation_report:
        validate_interchange_recurrent_table = SQLExecuteQueryOperator(
            task_id="validate_interchange_recurrent_table",
            conn_id="mssql_default",
            sql="""
            --SELECT 1 AS SUCCESS
            
                SELECT CASE
                    WHEN 
                        OBJECT_ID('[TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_RECURRENT]', 'U') IS NOT NULL

                        AND -- Verify that exists only data in the current month period for Visa
                        ( 
                        SELECT MIN([AGTX_POST_DATE]) 
                        FROM [TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_RECURRENT]
                        )>= %(BGN_DT)s

                        AND (
                        SELECT MAX([AGTX_POST_DATE]) 
                        FROM [TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_RECURRENT]
                        )<= %(END_DT)s

                    THEN 'True'
                    ELSE 'False'
                END 
                AS Result;
                
            """,
            parameters={
                "BGN_DT": "{{params['start_date']}}",
                "END_DT": "{{params['end_date']}}",
            },
        )

        scrape_current_exchange_rate_data_from_bnr = PythonOperator(
            task_id="scrape_current_exchange_rate_data_from_bnr",
            python_callable=web_scraping_bnr,
            do_xcom_push=True,
            retries=4,
            retry_delay=timedelta(seconds=10),
        )

        get_past_month_exchange_rate_from_db = SQLExecuteQueryOperator(
            task_id="get_past_month_exchange_rate_from_db",
            conn_id="mssql_default",
            sql="""
                SELECT CONCAT(T1.YEAR_MONTH,'-01'), T2.CRNCY_CD3, T1.XCH_RAT 
                FROM TABRDRO_RPT.[dbo].[LU_EXCH_RATE_MONTH_BNR] T1
                LEFT JOIN TABRDRO_RPT.[dbo].[LU_CURRENCY] T2 ON T1.SRC_CRNCY_ID = T2.CRNCY_NUM
                WHERE CONCAT(T1.YEAR_MONTH,'-01') = DATEADD(MONTH,-1,%(BGN_DT)s)
            """,
            do_xcom_push=True,
            parameters={
                "BGN_DT": "{{params['start_date']}}",
                "END_DT": "{{params['end_date']}}",
            },
        )
    

        verify_correct_web_scraping = BranchPythonOperator(
            task_id = "verify_correct_web_scraping",
            python_callable = _verify_correct_web_scraping,

        )
        update_exchange_rate_table = SQLExecuteQueryOperator(
            task_id="update_exchange_rate_table",
            conn_id="mssql_default",
            sql="""
            ----EUR
                INSERT INTO TABRDRO_RPT.[dbo].[LU_EXCH_RATE_MONTH_BNR] (YEAR_MONTH, SRC_CRNCY_ID, TGT_CRNCY_ID, XCH_RAT)
                VALUES(
                FORMAT(cast(%(BGN_DT)s as date),'yyyy-MM'),
                978,
                946,
                '{{ti.xcom_pull(key="EUR",task_ids="accounting_reconciliation_report.verify_correct_web_scraping")[0]}}',
                )
            ----USD
                INSERT INTO TABRDRO_RPT.[dbo].[LU_EXCH_RATE_MONTH_BNR] (YEAR_MONTH, SRC_CRNCY_ID, TGT_CRNCY_ID, XCH_RAT)
                VALUES(
                FORMAT(cast(%(BGN_DT)s as date),'yyyy-MM'),
                840,
                946,
                '{{ti.xcom_pull(key="USD",task_ids="accounting_reconciliation_report.verify_correct_web_scraping")[0]}}',
                )            
            """,
            parameters={
                "BGN_DT": "{{params['start_date']}}",
                "END_DT": "{{params['end_date']}}",
            },
            do_xcom_push = True
        )
        (
            validate_interchange_recurrent_table
            >> [scrape_current_exchange_rate_data_from_bnr, get_past_month_exchange_rate_from_db]
            >>verify_correct_web_scraping
            >> update_exchange_rate_table
        )

    send_error_email_sp3 = PythonOperator(
        task_id="send_error_email_sp3",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )

    verify_correct_web_scraping >> send_error_email_sp3
    
    with TaskGroup(
        "scheme_fee_reconciliation_report"
    ) as scheme_fee_reconciliation_report:
        validate_interchange_fee_reconciliation_table = BranchSQLOperator(
            task_id="validate_interchange_fee_reconciliation_table",
            conn_id="mssql_default",
            sql="""
            SELECT 1 AS SUCCESS
            /*
                SELECT CASE
                    WHEN 
                        OBJECT_ID('[TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_FEE_RECONCILIATION]', 'U') IS NOT NULL
                        AND -- Verify that exists data in the current month
                        (
                            SELECT MAX(CAST(CONCAT(YEAR_MONTH,'-1') AS DATE)) 
                            FROM [TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_FEE_RECONCILIATION]
                        ) = %(BGN_DT)s

                        AND -- Verify that exists two rows in the current month (Visa and Mastercard)
                        (
                            SELECT COUNT(*)
                            FROM [TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_FEE_RECONCILIATION]
                            WHERE 1=1
                                AND CAST(CONCAT(YEAR_MONTH,'-1') AS DATE) = %(BGN_DT)s
                        ) = 2

                        AND EXISTS -- Verify that exists a row for Mastecard in the current month
                        (
                            SELECT IDENTIFIER 
                            FROM [TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_FEE_RECONCILIATION]
                            WHERE 'MC' IN (
                                            SELECT IDENTIFIER
                                            FROM [TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_FEE_RECONCILIATION]
                                            WHERE 1=1
                                                AND CAST(CONCAT(YEAR_MONTH,'-1') AS DATE) = %(BGN_DT)s
                                            )
                        )

                        AND EXISTS  --Verify that exists the another row for Visa in the current month
                        (
                            SELECT IDENTIFIER 
                            FROM [TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_FEE_RECONCILIATION]
                            WHERE 'VI' IN (
                                            SELECT IDENTIFIER
                                            FROM [TABRDRO_RPT].[dbo].[RPT_INTERCHANGE_FEE_RECONCILIATION]
                                            WHERE 1=1
                                                AND CAST(CONCAT(YEAR_MONTH,'-1') AS DATE) = %(BGN_DT)s
                                            )
                        )
                    THEN 'True'
                    ELSE 'False'
                END 
                AS Result;
            */
            """,
            parameters={
                "BGN_DT": "{{params['start_date']}}",
            },
            follow_task_ids_if_false=[""],
            follow_task_ids_if_true=["is_quarter_data", ""],
        )

        is_quarter_data = BranchPythonOperator(
            task_id="is_quarter_data",
            python_callable=_is_quarter_data,
        )

        with TaskGroup("generate_excel_report") as generate_excel_report:
            get_fee_reconciliation_table = SQLExecuteQueryOperator(
                task_id="get_fee_reconciliation_table",
                conn_id="mssql_default",
                sql="""
                """,
                do_xcom_push=True,
            )
            export_data_to_excel = PythonOperator(
                task_id="export_data_to_excel",
                python_callable=_export_data_to_excel,
            )
            get_fee_reconciliation_table >> export_data_to_excel

        extract_data_for_visa = SQLExecuteQueryOperator(
            task_id="extract_data_for_visa",
            conn_id="mssql_default",
            sql="""
            SELECT 1 AS SUCCESS
            /*
            SET NOCOUNT ON
 
            SELECT      TA628DA1.[MONTH_ID] [MONTH_ID], T1.[MONTH_LDESC], T.[RPT_ID] [RPT_ID], T2.[RPT_DESC], T.[RPT_BNK_ID] [RPT_BNK_ID], T3.[RPT_BNK_DESC], TA628DA1.[DAY_DATE], TA628DA1.[DAY_DESC], TA92C0E1.[BRAND_ID] [BRAND_ID], T5.[BRAND_DESC], Sum(ISNULL(T.[FEE_TGT_AMT],0)*1.000000) [A85427F8F9F3]
            FROM      [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[FACT_ASSOC_FEE] T
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[DT_DAY] TA628DA1 ON
                TA628DA1.[DAY_DATE] = T.[RPT_RUN_DT]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[DT_MONTH] T1 ON
                T1.[MONTH_ID] = TA628DA1.[MONTH_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[LU_REPORT] T2 ON
                T2.[RPT_ID] = T.[RPT_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[LU_BANK] T3 ON
                T3.[RPT_BNK_ID] = T.[RPT_BNK_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[VW_LU_FEE_SUB] TA92C0E1 ON
                TA92C0E1.[FEE_SUB_ID] = T.[FEE_SUB_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[LU_BRAND] T5 ON
                T5.[BRAND_ID] = TA92C0E1.[BRAND_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[LU_FEE_SERVICE_TYPE] TAA67292 ON
                TAA67292.[FEE_STYP_ID] = TA92C0E1.[FEE_STYP_ID]
            WHERE      ((TAA67292.[FEE_STYP_ID] NOT IN ('73', '74', '110'))) AND ((TA628DA1.[MONTH_ID] = CONCAT(YEAR(%(BGN_DT)s),SUBSTRING(%(BGN_DT)s,6,2)))) AND (((TAA67292.[FEE_STYP_ID] NOT IN ('73', '74', '110')))) AND ((TA92C0E1.[BRAND_ID] IN ('2')))
            GROUP BY
                TA628DA1.[MONTH_ID], T1.[MONTH_LDESC], T.[RPT_ID], T2.[RPT_DESC], T.[RPT_BNK_ID], T3.[RPT_BNK_DESC], TA628DA1.[DAY_DATE], TA628DA1.[DAY_DESC], TA92C0E1.[BRAND_ID], T5.[BRAND_DESC]            
            */
            """,
            parameters={
                "BGN_DT": "{{params['start_date']}}",
            },
            do_xcom_push=True,
        )
        extract_data_for_mastercard = SQLExecuteQueryOperator(
            task_id="extract_data_for_mastercard",
            conn_id="mssql_default",
            sql="""
            SELECT 1 AS SUCCESS
            /*
            SET NOCOUNT ON
            
            SELECT      TA5D0A81.[MONTH_ID] [MONTH_ID], T1.[MONTH_LDESC], T.[FEE_GRP_ID] [FEE_GRP_ID], T2.[FEE_GRP_COD], T.[RPT_BNK_ID] [RPT_BNK_ID], T3.[RPT_BNK_DESC], TA5D0A81.[DAY_DATE], TA5D0A81.[DAY_DESC], TA874331.[BRAND_ID] [BRAND_ID], T5.[BRAND_DESC], Sum(ISNULL(T.[FEE_TGT_AMT],0)*1.000000) [A3ACB7D699A9]
            FROM      [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[FACT_ASSOC_FEE] T
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[DT_DAY] TA5D0A81 ON
                TA5D0A81.[DAY_DATE] = T.[RPT_RUN_DT]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[DT_MONTH] T1 ON
                T1.[MONTH_ID] = TA5D0A81.[MONTH_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[LU_FEE_GROUP] T2 ON
                T2.[FEE_GRP_ID] = T.[FEE_GRP_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[LU_BANK] T3 ON
                T3.[RPT_BNK_ID] = T.[RPT_BNK_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[VW_LU_FEE_SUB] TA874331 ON
                TA874331.[FEE_SUB_ID] = T.[FEE_SUB_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[LU_BRAND] T5 ON
                T5.[BRAND_ID] = TA874331.[BRAND_ID]
                INNER JOIN [AMZ-DENVER].[MSTRAFBBRDRO].dbo.[LU_FEE_SERVICE_TYPE] TA6A59C2 ON
                TA6A59C2.[FEE_STYP_ID] = TA874331.[FEE_STYP_ID]
            WHERE      ((TA6A59C2.[FEE_STYP_ID] NOT IN ('73', '74', '110'))) AND ((TA5D0A81.[MONTH_ID] = CONCAT(YEAR(%(BGN_DT)s),SUBSTRING(%(BGN_DT)s,6,2)))) AND (((TA6A59C2.[FEE_STYP_ID] NOT IN ('73', '74', '110')))) AND ((TA874331.[BRAND_ID] IN ('1'))) AND ((T.[FEE_GRP_ID] IN ('4', '27', '778')))
            GROUP BY
                TA5D0A81.[MONTH_ID], T1.[MONTH_LDESC], T.[FEE_GRP_ID], T2.[FEE_GRP_COD], T.[RPT_BNK_ID], T3.[RPT_BNK_DESC], TA5D0A81.[DAY_DATE], TA5D0A81.[DAY_DESC], TA874331.[BRAND_ID], T5.[BRAND_DESC]
            */
            """,
            parameters={
                "BGN_DT": "{{params['start_date']}}",
            },
            do_xcom_push=True,
        )

        validate_interchange_fee_reconciliation_table >> [
            is_quarter_data,
            generate_excel_report,
        ]

        is_quarter_data >> [extract_data_for_visa, extract_data_for_mastercard]

    send_error_email_sp2 = PythonOperator(
        task_id="send_error_email_sp2",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )

    validate_temporal_tables >> send_error_email_sp1

    (
        SP_1
        >> generate_report_tables
        >> [accounting_reconciliation_report, scheme_fee_reconciliation_report]
    )

    validate_interchange_fee_reconciliation_table >> send_error_email_sp2
