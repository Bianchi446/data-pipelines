from ariflow import DAG
import airflow.operators.bash BashOperator

dag = DAG(
    dag_id = "Bash command with message demostration"
        ),

bash_task = BashOperator(
    task_id = "bash_task",
    bash_command = "echo "\here is the message : '$message'\"",
    env={"message" : '{{dag_run.conf["message" if dag_run else " "]}}'},
)


