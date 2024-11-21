"""
Here there is a brief description of what this report is about
"""

from datetime import datetime
from datetime import timedelta
import calendar


from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.task_group import TaskGroup


def execute_first_stored_procedure(**kwargs) -> None:
    # Si estamos en el mes X, el reporte se genera para el mes X-1
    first_day: str = kwargs["params"].get("SP1: first_day_month (@BGN_DT)")
    last_day: str = kwargs["params"].get("SP1: last_day_month (@END_DT)")

    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        sql_query = f""" 
        EXEC [TAEURBGR_RPT].dbo.[ETL_INS_MTH_ACQ_TXN] 
        @PRJ_CD = 'EURBGR', 
        @BGN_DT = '{first_day}', 
        @END_DT = '{last_day}'
        """
        # cursor.execute(sql_query)

    except Exception as e:
        # conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()
        kwargs["ti"].xcom_push(key="XD", value=sql_query)


def count_TXN_SCHEME_FEE_data(**kwargs) -> None:
    first_day: str = kwargs["params"].get("SP1: first_day_month (@BGN_DT)")
    last_day: str = kwargs["params"].get("SP1: last_day_month (@END_DT)")

    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        sql_query = f""" 
            SELECT 
                CASE 
                    WHEN OBJECT_ID('[TAEURBGR_RPT].dbo.TXN_SCHEME_FEE', 'U') IS NOT NULL 
                        AND EXISTS (SELECT 1 FROM [TAEURBGR_RPT].dbo.TXN_SCHEME_FEE)
                    THEN (
                    SELECT COUNT(*) FROM TAEURBGR_RPT.dbo.TXN_SCHEME_FEE with (nolock)
                    WHERE 1 = 1
                    AND SET_DT BETWEEN '{first_day}' AND '{last_day}'
                    )
                    ELSE 0 
                END 
                    AS TableStatus
         """

        cursor.execute(sql_query)

        result = cursor.fetchone()

        kwargs["ti"].xcom_push(key="TXN_SCHEME_FEE_data", value=result[0])

    except Exception as e:
        # conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()


def count_MTH_ACQ_TXN_data_1(**kwargs) -> None:
    year_month: str = kwargs["params"].get("SP2: year_month (@SET_MTH_ID)")

    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        sql_query = f""" 
            SELECT 
                CASE 
                    WHEN OBJECT_ID('[TAEURBGR_RPT].dbo.MTH_ACQ_TXN', 'U') IS NOT NULL 
                        AND EXISTS (SELECT 1 FROM [TAEURBGR_RPT].dbo.MTH_ACQ_TXN)
                    THEN (
                    SELECT SUM(TXN_CNT) FROM TAEURBGR_RPT.dbo.MTH_ACQ_TXN with (nolock)
                    WHERE 1 = 1
                    AND SET_MTH = '{year_month}'
                    )
                    ELSE 0 
                END 
                    AS TableStatus
         """

        cursor.execute(sql_query)

        result = cursor.fetchone()

        kwargs["ti"].xcom_push(key="MTH_ACQ_TXN_data", value=result[0])

    except Exception as e:
        # conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()


def validate_counted_rows_first_validation(**kwargs) -> None:
    counted_rows_TXN_SCHEME_FEE: int = kwargs["ti"].xcom_pull(
        task_ids="count_TXN_SCHEME_FEE_data", key="TXN_SCHEME_FEE_data"
    )
    counted_rows_MTH_ACQ_TXN: int = kwargs["ti"].xcom_pull(
        task_ids="count_MTH_ACQ_TXN_data", key="MTH_ACQ_TXN_data"
    )

    if counted_rows_MTH_ACQ_TXN == counted_rows_TXN_SCHEME_FEE:
        return "execute_second_store_procedure"
    else:
        return "send_notification_if_data_is_not_right_1"


def execute_second_stored_procedure(**kwargs) -> None:
    year_month: str = kwargs["params"].get("SP2: year_month (@SET_MTH_ID)")
    flag = kwargs["params"].get("SP2: update_flag (@UPD_FLG)")

    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        sql_query = f""" 
        EXEC [TAEURBGR_RPT].dbo.[ETL_RPT_MCT_OUT] 
        @PRJ_CD = 'EURBGR', 
        @SET_MTH_ID = '{year_month}',
        @UPD_FLG = '{flag}'
        """
        cursor.execute(sql_query)

    except Exception as e:
        raise e

    finally:
        cursor.close()

        conn.close()


def count_MTH_ACQ_TXN_data_2(**kwargs) -> None:
    year_month: str = kwargs["params"].get("SP2: year_month (@SET_MTH_ID)")

    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        sql_query = f""" 
            SELECT 
                CASE 
                    WHEN OBJECT_ID('[TAEURBGR_RPT].dbo.MTH_ACQ_TXN', 'U') IS NOT NULL 
                        AND EXISTS (SELECT 1 FROM [TAEURBGR_RPT].dbo.MTH_ACQ_TXN)
                    THEN (
                    SELECT SUM(TXN_CNT) FROM TAEURBGR_RPT.dbo.MTH_ACQ_TXN with (nolock)
                    WHERE 1 = 1
                    AND SET_MTH = '{year_month}' AND TXN_TYP_ID = 1
                    )
                    ELSE 0 
                END 
                    AS TableStatus
         """

        cursor.execute(sql_query)

        result = cursor.fetchone()

        kwargs["ti"].xcom_push(key="MTH_ACQ_TXN_data", value=result[0])

    except Exception as e:
        # conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()


def count_RPT_MCT_OUT_data(**kwargs) -> None:
    year_month: str = kwargs["params"].get("SP2: year_month (@SET_MTH_ID)")

    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        sql_query = f""" 
            SELECT 
                CASE 
                    WHEN OBJECT_ID('[TAEURBGR_RPT].dbo.RPT_MCT_OUT', 'U') IS NOT NULL 
                        AND EXISTS (SELECT 1 FROM [TAEURBGR_RPT].dbo.RPT_MCT_OUT)
                    THEN (
                    SELECT SUM(TXN_CNT) FROM TAEURBGR_RPT.dbo.RPT_MCT_OUT with (nolock)
                    WHERE 1 = 1
                    AND SET_MTH = '{year_month}'
                    )
                    ELSE 0
                END 
                    AS TableStatus
         """

        cursor.execute(sql_query)

        result = cursor.fetchone()

        kwargs["ti"].xcom_push(key="RPT_MCT_OUT_data", value=result[0])

    except Exception as e:
        # conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()


def validate_counted_rows_second_validation(**kwargs):
    counted_rows_MTH_ACQ_TXN = kwargs["ti"].xcom_pull(
        task_ids="count_MTH_ACQ_TXN_data_2", key="MTH_ACQ_TXN_data"
    )
    counted_rows_RPT_MCT_OUT = kwargs["ti"].xcom_pull(
        task_ids="count_RPT_MCT_OUT_data", key="RPT_MCT_OUT_data"
    )
    if counted_rows_RPT_MCT_OUT is None or counted_rows_MTH_ACQ_TXN is None:
        return "send_notification_if_data_is_not_right_2"
    elif int(counted_rows_MTH_ACQ_TXN) == (counted_rows_RPT_MCT_OUT):
        return "upload_files_to_sftp"
    else:
        raise ValueError("Uno de los valores es None. Revisar las tareas de origen.")


default_args = {
    "owner": "Max E.",
    "email": ["max.espejo@intelica.com"],
    "do_xcom_push": False,
    "email_on_failure": True,
    #'retries': 1,
}


def default_dag_parameters() -> tuple[str, str]:
    first_day: datetime = datetime.today().replace(day=1)
    last_day: datetime = first_day + timedelta(
        days=calendar.monthrange(first_day.year, first_day.month)[1] - 1
    )
    year_month: str = first_day.strftime("%Y%m")
    first_day_str: str = first_day.strftime("%Y-%m-%d")
    last_day_str: str = last_day.strftime("%Y-%m-%d")
    return first_day_str, last_day_str, year_month


default_first_date, default_end_date, year_month = default_dag_parameters()


with DAG(
    dag_id="EURBGR_scheme_fee",
    default_args=default_args,
    schedule=None,  # Para ejecución manual
    # start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,  # Define si el DAG debe “ponerse al día” y ejecutar todas las ejecuciones programadas en el pasado desde la start_date
    params={
        "SP1: first_day_month (@BGN_DT)": default_first_date,
        "SP1: last_day_month (@END_DT)": default_end_date,
        "SP2: year_month (@SET_MTH_ID)": year_month,
        "SP2: update_flag (@UPD_FLG)": 1,
    },
) as dag_eurbgr_scheme_fee:
    execute_first_stored_procedure = PythonOperator(
        task_id="execute_first_store_procedure",
        python_callable=execute_first_stored_procedure,
        provide_context=True,
        # do_xcom_push = True
    )

    with TaskGroup("first_validation") as first_validation_group:
        count_TXN_SCHEME_FEE_data = PythonOperator(
            task_id="count_TXN_SCHEME_FEE_data",
            python_callable=count_TXN_SCHEME_FEE_data,
            provide_context=True,
            do_xcom_push=True,
        )

        count_MTH_ACQ_TXN_data_1 = PythonOperator(
            task_id="count_MTH_ACQ_TXN_data_1",
            python_callable=count_MTH_ACQ_TXN_data_1,
            provide_context=True,
            do_xcom_push=True,
        )

        validate_counted_rows_first_validation = BranchPythonOperator(
            task_id="validate_counted_rows_first_validation",
            python_callable=validate_counted_rows_first_validation,
        )

        [
            count_TXN_SCHEME_FEE_data,
            count_MTH_ACQ_TXN_data_1,
        ] >> validate_counted_rows_first_validation

    send_notification_if_data_is_not_right_1 = EmailOperator(
        task_id="send_notification_if_data_is_not_right_1",
        to="max.espejo@intelica.com",  # Dirección de correo de notificación
        subject="Fallo en la tarea {{ task_instance.task_id }} del DAG {{ dag.dag_id }}",
        html_content="""
        <p>La tarea <strong>{{ task_instance.task_id }}</strong> en el DAG <strong>{{ dag.dag_id }}</strong> falló.</p>
        <p><strong>Fecha de ejecución:</strong> {{ execution_date }}</p>
        <p>Revisa el log de la tarea para más detalles.</p>
        """,
    )

    execute_second_stored_procedure = PythonOperator(
        task_id="execute_second_store_procedure",
        python_callable=execute_second_stored_procedure,
        # do_xcom_push = True
    )

    with TaskGroup("second_validation") as second_validation_group:
        count_MTH_ACQ_TXN_data_2 = PythonOperator(
            task_id="count_MTH_ACQ_TXN_data_2",
            python_callable=count_MTH_ACQ_TXN_data_2,
            do_xcom_push=True,
            provide_context=True,
        )
        count_RPT_MCT_OUT_data = PythonOperator(
            task_id="count_RPT_MCT_OUT_data",
            python_callable=count_RPT_MCT_OUT_data,
            do_xcom_push=True,
            provide_context=True,
        )
        validate_file_path = EmptyOperator(task_id="validate_file_path")
        validate_counted_rows_second_validation = BranchPythonOperator(
            task_id="validate_counted_rows_second_validation",
            python_callable=validate_counted_rows_second_validation,
        )
        [
            count_MTH_ACQ_TXN_data_2,
            count_RPT_MCT_OUT_data,
            validate_file_path,
        ] >> validate_counted_rows_second_validation

    send_notification_if_data_is_not_right_2 = EmailOperator(
        task_id="send_notification_if_data_is_not_right_2",
        to="max.espejo@intelica.com",  # Dirección de correo de notificación
        subject="Fallo en la tarea {{ task_instance.task_id }} del DAG {{ dag.dag_id }}",
        html_content="""
        <p>La tarea <strong>{{ task_instance.task_id }}</strong> en el DAG <strong>{{ dag.dag_id }}</strong> falló.</p>
        <p><strong>Fecha de ejecución:</strong> {{ execution_date }}</p>
        <p>Revisa el log de la tarea para más detalles.</p>
        """,
    )

    upload_files_to_sftp = EmptyOperator(task_id="upload_files_to_sftp")

    send_notification_to_Anastasios = EmptyOperator(
        task_id="send_notification_to_Anastasios"
    )

    send_notification_if_dag_success = EmailOperator(
        task_id="send_success_email",
        to="max.espejo@intelica.com",
        subject="El DAG {{ dag.dag_id }} terminó exitosamente",
        html_content="""
        <p><strong>Fecha de ejecución:</strong> {{ execution_date }}</p>
        <p>Revisa el log de la tarea para más detalles.</p>
        """,
    )

    (
        execute_first_stored_procedure
        >> first_validation_group
        >> execute_second_stored_procedure
        >> second_validation_group
        >> upload_files_to_sftp
        >> send_notification_to_Anastasios
        >> send_notification_if_dag_success
    )
    first_validation_group >> send_notification_if_data_is_not_right_1
    second_validation_group >> send_notification_if_data_is_not_right_2
