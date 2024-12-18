"""
###############################################################################
DAG Name: btro_tableau.py

Summary:
This DAG processes the previous month's consolidated transactional information for banks, ensuring data consistency and data accuracy for Tableau reporting.
In this case, for client Banca Transilvania, they are due by the 15th of each month. Also, this process involves multiple tasks to load and validate data from various 
stored procedures (SP) and sends notifications based on success or failure.

Steps:
    1. Calculate Parameters:
        - Extract start date and the number of months to process.
        - Push calculated years and months to XCom.
    2. Task Group: SP_1
        - Load historical analysis data into a table.
        - Validate the table generation using conditions.
        - Send error email if validation fails.
    3. Task Group: SP_2
        - Load ATM historical analysis data into a table.
        - Validate the table generation using conditions.
        - Send error email if validation fails.
    4. Update Tableau Report:
        - Trigger a refresh of Tableau dashboards with the new data.
    5. Notify Key Account Manager:
        - Send an email to the KAM informing about the update.
    6. Send Success Notification:
        - Notify the DAG owner when the workflow completes successfully.

Metadata:
    Author: Max Espejo, Julio Cardenas
    Created On: [2024/12/10]
    Last Updated: [2024/12/10]
    Version: 1.0

Changelog:
    - Version 1.0: Initial implementation of the DAG for the Banca Transilvania Tableau report.

###############################################################################
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLExecuteQueryOperator,
)

# from airflow.providers.tableau.operators.tableau import TableauOperator
from airflow.utils.task_group import TaskGroup
from dateutil.relativedelta import relativedelta
from utils.branch_email_util import send_dynamic_error_email


def default_validation_parameters(**kwargs) -> None:
    start_date_str: str = kwargs["params"].get("@BGN_DT")
    num_date: int = kwargs["params"].get("@NUM_DT")
    # current_years = datetime.now().strftime("%Y")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    months_list = []
    years_set = set()
    for i in range(num_date):
        new_date = start_date + relativedelta(months=i)
        month = new_date.strftime("%m")
        months_list.append(month)
        year = new_date.strftime("%Y")
        years_set.add(year)
    kwargs["ti"].xcom_push(key="current_years", value=list(years_set))
    kwargs["ti"].xcom_push(key="months", value=months_list)


def default_begin_date() -> tuple[str, int]:
    current_date = datetime.now()
    num_date = 3
    # Get the first day of the month three months ago
    first_day_three_months_ago = (
        current_date.replace(day=1) - relativedelta(months=4)
    ).strftime("%Y-%m-%d")
    # Get the corresponding parameters.
    return (first_day_three_months_ago, num_date)


default_args = {
    "owner": "Max E.",
    "email": ["max.espejo@intelica.com"],
    "do_xcom_push": False,
    "email_on_failure": True,
    #'retries': 1,
}

default_start_date, default_num_date = default_begin_date()


with DAG(
    dag_id="BTRO_Tableau",
    default_args=default_args,
    # schedule = "0 12 * * 1-5",
    schedule=None,
    catchup=False,
    params={"@BGN_DT": default_start_date, "@NUM_DT": default_num_date},
) as dag:
    calculate_parameters_sp = PythonOperator(
        task_id="calculate_parameters_sp",
        python_callable=default_validation_parameters,
        provide_context=True,
        do_xcom_push=True,
    )
    with TaskGroup("SP_1") as SP_1:
        load_historical_analysis_data_table = SQLExecuteQueryOperator(
            task_id="load_historical_analysis_data_table",
            conn_id="mssql_default",
            sql=""" 
                EXEC [TABTRLRO].[dbo].[ETL_RPT_HISTORICAL_ANALYSIS]
                @BGN_DT = %(BGN_DT)s,
                @NUM_DT = %(NUM_DT)s
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "NUM_DT": "{{params['@NUM_DT']}}",
            },
        )
        validate_generated_table_sp1 = BranchSQLOperator(
            task_id="validate_generated_table_sp1",
            sql=""" 
                SELECT 
                    CASE 
                        WHEN OBJECT_ID('[TABTRLRO].dbo.[HISTORICAL_ANALYSIS_REPORT_NEW]', 'U') IS NOT NULL 
                            AND (SELECT COUNT(DISTINCT MONTH_DT) 
                                    FROM [TABTRLRO].dbo.HISTORICAL_ANALYSIS_REPORT_NEW 
                                    WHERE YEAR_DT IN ({{ task_instance.xcom_pull(task_ids="calculate_parameters_sp", key="current_years") | join(", ") }})
                                    AND MONTH_DT IN ({{ task_instance.xcom_pull(task_ids="calculate_parameters_sp", key="months") | join(", ") }})) 
                            = {{ params['@NUM_DT'] }}
                        THEN 'true'
                        ELSE 'false'
                    END 
                        AS TableStatus                
            """,
            follow_task_ids_if_false=["send_error_email_sp1"],
            follow_task_ids_if_true=["SP_2.load_atm_historical_analysis"],
        )

        load_historical_analysis_data_table >> validate_generated_table_sp1

    send_error_email_sp1 = PythonOperator(
        task_id="send_error_email_sp1",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )
    with TaskGroup("SP_2") as SP_2:
        load_atm_historical_analysis = SQLExecuteQueryOperator(
            task_id="load_atm_historical_analysis",
            conn_id="mssql_default",
            sql="""
                EXEC [TABTRLRO].[dbo].[ETL_RPT_ATM_DCC]
                @BGN_DT = %(BGN_DT)s,
                @NUM_DT = %(NUM_DT)s
            """,
            parameters={
                "BGN_DT": "{{params['@BGN_DT']}}",
                "NUM_DT": "{{params['@NUM_DT']}}",
            },
        )
        validate_generated_table_sp2 = BranchSQLOperator(
            task_id="validate_generated_table_sp2",
            sql=""" 
                #SELECT 
                    CASE 
                        WHEN OBJECT_ID('[TABTRLRO].dbo.[ATM_DCC_TRX]', 'U') IS NOT NULL 
                            AND (SELECT COUNT(DISTINCT RIGHT(SET_MTH,2))
                                    FROM [TABTRLRO].dbo.ATM_DCC_TRX
                                    WHERE LEFT(SET_MTH,4) IN ({{ task_instance.xcom_pull(task_ids="calculate_parameters_sp", key="current_years") | join(", ")}})
                                    AND RIGHT(SET_MTH,2) IN ({{ task_instance.xcom_pull(task_ids="calculate_parameters_sp", key="months") | join(", ") }})) 
                            = {{ params['@NUM_DT'] }}
                        THEN 'true'
                        ELSE 'false'
                    END 
                        AS TableStatus          
            """,
            follow_task_ids_if_false=["send_error_email_sp3"],
            follow_task_ids_if_true=["update_tableau_report"],
        )
        load_atm_historical_analysis >> validate_generated_table_sp2

    send_error_email_sp2 = PythonOperator(
        task_id="send_error_email_sp2",
        python_callable=send_dynamic_error_email,
        provide_context=True,
    )
    update_tableau_report = EmptyOperator(task_id="update_tableau_report")
    # update_tableau_report = TableauOperator(
    #     task_id="update_tableau_report",
    #     resource="workbooks",
    #     method="refresh",
    #     find="MyWorkbook",
    #     match_with="name",
    #     blocking_refresh=True,
    # )

    send_notification_to_kam = EmailOperator(
        task_id="send_notification_to_kam",
        to="no.one@intelica.com",
        subject="BTRLRO Report",
        html_content="""
        <p>The Tableau report has been updated.</p>
        """,
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
        calculate_parameters_sp
        >> SP_1
        >> SP_2
        >> update_tableau_report
        >> send_notification_to_kam
        >> send_notification_if_dag_success
    )
    SP_1 >> send_error_email_sp1
    SP_2 >> send_error_email_sp2
