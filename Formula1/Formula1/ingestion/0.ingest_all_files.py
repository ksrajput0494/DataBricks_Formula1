# Databricks notebook source
V_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

V_result

# COMMAND ----------

V_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

V_result

# COMMAND ----------

V_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

V_result

# COMMAND ----------

V_result = dbutils.notebook.run("4. Ingest_Drivers_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

V_result

# COMMAND ----------

V_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

V_result

# COMMAND ----------

V_result = dbutils.notebook.run("6.ingest_pit_stop_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------



# COMMAND ----------

V_result = dbutils.notebook.run("7.ingest_laptimes_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

V_result

# COMMAND ----------

V_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

V_result