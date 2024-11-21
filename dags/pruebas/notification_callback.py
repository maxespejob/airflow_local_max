from airflow.operators.email import EmailOperator

# Callback de notificación en caso de fallo
def notify_failure(context):
    # Extraer información de contexto
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']

    # Enviar correo electrónico de notificación

    email.execute(context)
