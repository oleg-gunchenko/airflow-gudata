#!/bin/env python

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago
import uuid

part_value = datetime.now().strftime('%Y-%m-%d')

landing_dir = "data/landing/fns/zags/in"
target_dir = "data/RAW/fns/zags/export_date='%s'" % part_value
result_csv_dir = "data/landing/fns/zags/out"

hadoop_username = "niips0"

ds_connection_string = 'jdbc:postgresql://10.3.48.112/gutest'
ds_username = 'gutest'
ds_password = 'gutest'

hdfs_command_move = """
HADOOP_USER_NAME={{params.hadoop_username}} hdfs dfs -mv {{params.source_data_dir}} {{params.target_data_dir}} 
"""

hdfs_command_remove = """
HADOOP_USER_NAME={{params.hadoop_username}} hdfs dfs -rm -r -f -skipTrash {{params.data_dir}}
"""

sqoop_command = """HADOOP_USER_NAME={{params.hadoop_username}} /bin/sqoop export --connect '{{params.connection_url}}' --username '{{params.username}}'  \
  -table '{{params.table}}' --export-dir '{{params.source_dir}}' -password {{params.password}} 
"""

spark_command = """HADOOP_USER_NAME={{params.hadoop_username}} /bin/spark2-submit \
                    --master local \
                    --packages org.apache.spark:spark-avro_2.11:2.4.0 \
                    /opt/airflow/pyspark/gudata.py
"""

sqoop_params = {
    'source_dir': '/tmp/gudata/fns/zags/result.avro',
    'connection_url': ds_connection_string,
    'username': ds_username,
    'table': 'fnszags',
    'password': ds_password,
    'hadoop_username': hadoop_username
}

dag = DAG('export_data', start_date=datetime(2019, 6, 6),
          schedule_interval=None,
          dagrun_timeout=timedelta(minutes=60))

start = DummyOperator(dag=dag, task_id='start')

end = DummyOperator(dag=dag, task_id='end')

remove_temp_dir = BashOperator(dag=dag, task_id='remove_temp_dir', depends_on_past=True,
                               bash_command=hdfs_command_remove, params={
        'hadoop_username': hadoop_username,
        'data_dir': '/tmp/gudata/fns'
    })

remove_result_csv = BashOperator(dag=dag, task_id='remove_result_csv', depends_on_past=True,
                                 bash_command=hdfs_command_remove, params={
        'hadoop_username': hadoop_username,
        'data_dir': '/tmp/gudata/result.csv'
    })

execute_spark_job = BashOperator(dag=dag, task_id='execute_spark_job',
                                 bash_command=spark_command, params={'hadoop_username': hadoop_username})

export_data_sqoop = BashOperator(dag=dag, task_id='export_data_sqoop', depends_on_past=True,
                                 bash_command=sqoop_command, params=sqoop_params)

start >> [remove_temp_dir, remove_result_csv] >> execute_spark_job >> export_data_sqoop >> end
