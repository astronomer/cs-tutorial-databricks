from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from datetime import datetime
from include.databricks_tools import DatabricksUtil
from airflow.providers.databricks.hooks.databricks import DatabricksHook

'''
Notes
- This example dag is using all known operators and hook methods for Databricks as of 11/1/21
- DatabricksSubmitRunOperator = DatabricksHook.submit_run & DatabricksRunNowOperator = DatabricksHook.run_now 
  (essentially) see next bullet point for a key difference
- The DatabricksSubmitRunOperator gets marked as success once the actual job on Databricks succeeds while the hook 
  method submit_run gets marked as success once the job is kicked off (not finished in Databricks)
- To use this demo, you'll need to start a trial Databricks on Azure Portal. And create a hello world notebook that has
  one simple cell: print("Hello World") (you could also add time.sleep(60) if you wanted to more easily demonstrate the
  cancel_run method on the hook)
- The Operators and Hook default to the databricks_default conn_id in the Airflow UI, so you'll need to add that as well
  params are as follows:
    -  conn_id: databricks_default
    -  connection_type: Databricks
    -  extra: {"host": "https://adb-xxxx.azuredatabricks.net/", "token": "<databricks_token>"}
    (on extra replace the xxxx in host with the url in your databricks instance and generate a token through the 
    databricks UI under Settings >> User Settings
'''


#these typically would go into Airflow Variables, but setting them here for Demonstration purposes.
job_id = 9 #grabbed this from the databricks UI. Represents a job that is using a notebook that has a hello world application
run_id = 131 #represents a run of job 9.
cluster_id = "1101-180739-3e8n7nkv" #created a "default_cluster" as an All-Purpose Cluster

new_cluster = {
    'spark_version': '8.3.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2',
    'azure_attributes': {'availability': 'ON_DEMAND_AZURE'},
    'num_workers': 8,
}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('databricks_example_dag',
         start_date=datetime(2019, 1, 1),
         max_active_runs=3,
         schedule_interval=None,
         catchup=False
         ) as dag:

    start = DummyOperator(
        task_id='start'
    )
    finish = DummyOperator(
        task_id='finish'
    )

    with TaskGroup(group_id="operators") as g1:
        #runs a job that is already assigned to an existing all purpose cluster
        opr_run_now = DatabricksRunNowOperator(
            task_id="databricks_run_now",
            databricks_conn_id="databricks_default",
            job_id=str(job_id)
        )

        #spins up a new cluster and runs a job - to see the cluster look at the "Job Clusters" tab in Databricks UI
        opr_submit_run = DatabricksSubmitRunOperator(
            task_id='databricks_submit_run',
            json={
                'new_cluster': new_cluster,
                'notebook_task': {
                    'notebook_path': '/Users/chronek@astronomer.io/hello_world_2'
                }
            }
        )

    with TaskGroup(group_id="hook_methods") as g2:
        with TaskGroup(group_id="job_interaction") as g3:
            #runs a job that is already assigned to an existing all purpose cluster
            python_run_now = PythonOperator(
                task_id="python_run_now",
                python_callable=DatabricksUtil().run_now,
                op_kwargs={
                    "job_id": str(job_id)
                }
            )

            # spins up a new cluster and runs a job - to see the cluster look at the "Job Clusters" tab in Databricks UI
            python_submit_run = PythonOperator(
                task_id="python_submit_run",
                python_callable=DatabricksUtil().submit_run,
                op_kwargs={
                    "json": {
                        'new_cluster': new_cluster,
                        'notebook_task': {
                            'notebook_path': '/Users/chronek@astronomer.io/hello_world_2'
                        }
                    }
                }
            )

            #returns url for the specified run given the run_id (see xcom)
            python_get_run_page_url = PythonOperator(
                task_id="python_get_run_page_url",
                python_callable=DatabricksUtil().get_run_page_url,
                op_kwargs={
                    "run_id": str(run_id)
                }
            )

            #returns job_id given a run_id (see xcom)
            python_get_job_id = PythonOperator(
                task_id="python_get_job_id",
                python_callable=DatabricksUtil().get_job_id,
                op_kwargs={
                    "run_id": str(run_id)
                }
            )

            #I think this is broken opened a bug here https://github.com/apache/airflow/issues/19357
            python_get_run_state = PythonOperator(
                task_id="python_get_run_state",
                python_callable=DatabricksUtil().get_run_state,
                op_kwargs={
                    "run_id": str(run_id)
                }
            )

            #cancels a specified run for a job given a run id
            python_cancel_run = PythonOperator(
                task_id="python_cancel_run",
                python_callable=DatabricksUtil().cancel_run,
                op_kwargs={
                    "run_id": str(run_id)
                }
            )

        with TaskGroup("clusters") as g4:
            #starts a terminated cluster
            python_start_cluster = PythonOperator(
                task_id="python_start_cluster",
                python_callable=DatabricksUtil().start_cluster,
                op_kwargs={
                    "cluster_id": cluster_id
                }
            )

            #restarts a cluster
            python_restart_cluster = PythonOperator(
                task_id="python_restart_cluster",
                python_callable=DatabricksUtil().restart_cluster,
                op_kwargs={
                    "cluster_id": cluster_id
                }
            )

            #terminates a cluster
            python_terminate_cluster = PythonOperator(
                task_id="python_terminate_cluster",
                python_callable=DatabricksUtil().terminate_cluster,
                op_kwargs={
                    "cluster_id": cluster_id
                }
            )

            #install package dependencies on a cluster
            python_install = PythonOperator(
                task_id="python_install",
                python_callable=DatabricksUtil().install,
                op_kwargs={
                    "json": {
                        "cluster_id": cluster_id,
                        "libraries": [{
                            "pypi": {
                                "package": "requests"
                            }
                        }]
                    }
                }
            )

            #uninstall package dependencies on a cluster
            python_uninstall = PythonOperator(
                task_id="python_uninstall",
                python_callable=DatabricksUtil().uninstall,
                op_kwargs={
                    "json": {
                        "cluster_id": cluster_id,
                        "libraries": [{
                                "pypi": {
                                    "package": "requests"
                                }
                            }]
                    }
                }
            )


    start >> g1 >> finish
    start >> g2 >> finish
