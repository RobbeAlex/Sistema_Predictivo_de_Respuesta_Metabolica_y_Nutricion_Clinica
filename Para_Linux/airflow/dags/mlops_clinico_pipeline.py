from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Configuración de parámetros por defecto
default_args = {
    'owner': 'postgres',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 1),
    'email': ['tu_correo@ejemplo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# 2. Instanciación del DAG
dag = DAG(
    'pipeline_predictivo_metabolico',
    default_args=default_args,
    description='ETL en PostgreSQL y Reentrenamiento de Random Forest',
    schedule='0 3 * * *',
    catchup=False,
    tags=['clinico', 'machine_learning', 'sql']
)

# 3. Definición de las Tareas
tarea_etl_sql = BashOperator(
    task_id='ejecutar_etl_postgresql',
    bash_command='python /opt/airflow/dags/etl_clinico.py',
    dag=dag,
)

tarea_entrenamiento_ml = BashOperator(
    task_id='reentrenar_random_forest',
    bash_command='python /opt/airflow/dags/entrenamiento_ml.py',
    dag=dag,
)

tarea_recargar_api = BashOperator(
    task_id='recargar_api_fastapi',
    # Usamos host.docker.internal para que Airflow le avise a tu FastAPI en Windows que recargue el modelo
    bash_command='curl -X POST http://host.docker.internal:8000/recargar_modelo || true',
    dag=dag,
)

# 4. Orquestación y Dependencias
tarea_etl_sql >> tarea_entrenamiento_ml >> tarea_recargar_api
