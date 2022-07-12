from datetime import timedelta
#Common arguments for DAGs.

default_args = {
    'owner': 'lgramu97',
    'depdends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_rety': False,
    'retires': 3,
    'retry_delay': timedelta(minutes=5),
}