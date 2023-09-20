Steps:

1. Converted pure python ingestion script(previous step) into prefect ETL script
2. Refactored the code into multiple functions (extraction, transformation and ingestion/load) and wrapped the functions with prefect tasks
3. Main fucntion is wrapped in prefect flow component. (we can add subflows if needed)
4. Created an sqlAlchemy block component(connection module) in orion UI, using which our script can access postgre db
5. We can use ORION UI to view the flow runs/invidual steps and other metrics
