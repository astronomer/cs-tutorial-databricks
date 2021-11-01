# Customer Success Databricks Tutorial
This DAG demonstrates how to use the `DatabricksRunNowOperator`, `DatabricksSubmitRunOperator` and methods contained within the `DatabricksHook`

# Prerequisites:
- Astro CLI
- Databricks Instance
- Airflow Instance (If you plan on deploying)

# Steps to Use:
### Run the following in your terminal:
1. `git clone git@github.com:astronomer/cs-tutorial-databricks.git`
2. `cd cs-tutorial-databricks`
3. `astro d start`

### Add **databricks_default** connection to your sandbox
1. Go to your sandbox http://localhost:8080/home
2. Navigate to connections (i.e. Admin >> Connections)
3. Add a new connection with the following parameters
    - Connection Id: databricks_default
    - Connection Type: Databricks
    - Extra: {"host": "https://adb-your_databricks_deployment_id.azuredatabricks.net", "token": "your_databricks_token"}

Be sure to replace *your_databricks_deployment_id* and *your_databricks_token* with your actual Databricks instance URL and a token generated from Databricks. 

**FYI**: You can generate a Databricks token in the Databricks UI under Settings >> User Settings
  
### Replace variables with values from your Databricks Instance
In the `databricks_example_dag.py` you'll need to replace the variables like `job_id`, `run_id`, and `cluster_id` with corresponding values that actually exist in your Databricks deployment

___
After following these steps, you should be able to run the tasks in the `databricks_example_dag`. Enjoy!
