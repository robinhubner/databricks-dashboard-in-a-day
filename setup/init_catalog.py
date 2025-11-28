from pyspark.sql import SparkSession
from IPython.display import display, HTML


def setup_catalog(catalog: str = "sunny_bay_roastery"):
    try:
        spark = SparkSession.builder.getOrCreate()
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        display(HTML(f"<div style='color:green'>Catalog `{catalog}` created or already exists.</div>"))
    except Exception as e:
        display(HTML(f"<div style='color:red'>Failed to create catalog: {e}</div>"))
