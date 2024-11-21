from airflow.operators.email import EmailOperator
def send_dynamic_error_email(**kwargs):
    # Get the Airflow execution context
    task = kwargs['task']
    dag_id = kwargs['dag'].dag_id
    execution_date = kwargs['execution_date']
    
    # Get the upstream (previous) task
    upstream_task_id = list(task.upstream_task_ids)[0] if task.upstream_task_ids else "unknown_task"
    
    # Dynamic email content
    email_content = f"""
    <p>The task <strong>{upstream_task_id}</strong> in the DAG <strong>{dag_id}</strong> failed.</p>
    <p><strong>Execution date:</strong> {execution_date}</p>
    <p>Check the task log for more details.</p>
    """
    
    # Definition and execution of the email operator within the function
    email_operator = EmailOperator(
        task_id=f"send_error_email_{upstream_task_id}",
        to="max.espejo@intelica.com",
        subject=f"DAG {dag_id} stopped due to an issue in {upstream_task_id}",
        html_content=email_content
    )
    email_operator.execute(context=kwargs)

