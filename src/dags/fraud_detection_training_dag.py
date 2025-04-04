from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException


logger = logging.getLogger(__name__)

default_args = {
    'owner': 'datamasterylab.com',
    'depends_on_past': False,
    'start_date': datetime(2025,3,12),
    # 'execution_timeout': timedelta(minutes=120),
    'max_active_runs': 1,
}

def _train_model(**kwargs):
    """Airflow wrapper for training task"""
    from fraud_detection_training_dag import FraudDetectionTraining
    context = kwargs
    try:
        logger.info('Initializing fraud detection training')
        trainer = FraudDetectionTraining()
        
        return { 'status': 'success' }
    except Exception as e:
        logger.error('Training failed: %s', str(e),exec_info=True)
        raise AirflowException(f'Model training failed: {str(e)}')

with DAG(
    'fraud_detection_training',
    default_args=default_args,
    description='Fraud detection model training pipeline',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['fraud','ML']
) as dag:
    validate_environment = BashOperator(
        task_id='validate_environment',
        bash_command='''
        echo "Validating environment..."
        test -f /app/config.yaml &&
        test -f /app/.env &&
        echo "Environment is valid"
        '''
    )

    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model,
        provide_context=True,
    )

    cleanup_task = BashOperator(
        task_id='cleanup_resources',
        bash_command='rm -f /app/tmp/*.pkl',
        trigger_rule='all_done'
    )

    validate_environment >> training_task >> cleanup_task

    # Documentation
    dag.doc_md = """
    ## Fraud Detection Training Pipeline

    Fairly training of fraud detection model using: 
    - Transaction data from Kafka
    - XGBoost classifier with precision optimization
    - MLFlow for experiment tracking
    """