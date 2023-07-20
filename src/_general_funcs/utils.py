def get_dbutils():
  from pyspark.sql import SparkSession
  spark = SparkSession.getActiveSession()
  if spark.conf.get("spark.databricks.service.client.enabled") == "true":
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)
  else:
    import IPython
    return IPython.get_ipython().user_ns["dbutils"]

def get_notebook_name(name_only=True):
    """
    function get_notebook_name to use dbutils to return the current notebook name (and path if requested)
    params:
        name_only bool: optional param to specify whether to return only the notebook name, default=true
            if false, will return full file path

    returns:
        string with name/path if requested

    """

    # Get the current notebook name
    name = get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

    if name_only:
        name = name.split('/')[-1]

    return name