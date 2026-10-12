# Databricks notebook source
# MAGIC %md
# MAGIC # Inference on the Silver Table
# MAGIC
# MAGIC The purpose of this notebook is to perform inference on the streaming table stored in the `dev` catalog. The model being used here was created during the classroom setup script for the associated lab. 
# MAGIC
# MAGIC ## Steps:
# MAGIC 1. Read in the streaming table and perform some data transformations to prepare it to be used as an input for our model. 
# MAGIC 1. Load a pre-trained model from Unity Catalog. This is located in the staging catalog. 
# MAGIC 1. Make a prediction on the transformed streaming table on the first 2 rows to validate our silver layer for the ML team.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameterize the notebook for our workflow and passing variables

# COMMAND ----------

base_model_name = dbutils.widgets.get("base_model_name") # Set to "diabetes_model_dev" in the parameterization of the notebook in the workflow 
silver_table_name = dbutils.widgets.get('silver_table_name')# Set to "<username>_1_dev.default.health_silver"
catalog_name = dbutils.widgets.get('catalog_name')# Set to "<username>_1_dev.default.diabetes_model_dev"
print(base_model_name)
print(silver_table_name)
print(catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read and transform the silver-layer streaming table.

# COMMAND ----------

from mlflow.tracking import MlflowClient
import mlflow 

from pyspark.sql.functions import col, log, pow
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql import DataFrame

# Create a function to transform columns to be used for inference on streaming data
def create_streaming_features(silver_table: str) -> DataFrame:
    # Read streaming data
    stream_df = (
        spark.read
        .table(silver_table)  # Assumes a streaming table is registered in Unity Catalog
    )

    # Transform the data to include required computed features
    transformed_stream_df = (
        stream_df
        .withColumn("log_BMI", log(col("BMI") + 1))
        .withColumn("log_Age", log(col("Age") + 1))
        .withColumn("BMI_squared", pow(col("BMI"), 2))
        .drop("PII", "date")
        .na.drop()
    )

    # Ensure features are transformed as done during training
    assembler = VectorAssembler(
        inputCols=["HighCholest", "HighBP", "BMI", "Age", "Education", "income", "log_BMI", "log_Age", "BMI_squared"], 
        outputCol="features"
    )
    stream_features = assembler.transform(transformed_stream_df)
    print("Silver table successfully transformed!")
    return stream_features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load the pre-trained model.

# COMMAND ----------

from pyspark.ml import PipelineModel

def load_ml_model(env: str) -> PipelineModel:
    model_base_name = base_model_name
    full_model_name = f"{catalog_name}.default.{model_base_name}"

    # Retrieve the latest version of the model
    client = MlflowClient()
    model_version_infos = client.search_model_versions(f"name = '{full_model_name}'")

    if model_version_infos:
        latest_version = max([int(info.version) for info in model_version_infos])
        model_uri = f"models:/{full_model_name}/{latest_version}"
        print(f"Found model {full_model_name} in {env}")
        print(f"Loading model version {latest_version} from MLflow...")
        return mlflow.spark.load_model(model_uri)  # Ensure correct model loading
    else:
        raise ValueError(f"No registered versions of {full_model_name} found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Make a prediction on the transformed streaming table and output the results.

# COMMAND ----------

from pyspark.ml import PipelineModel



def make_prediction(sample_df: DataFrame, loaded_model: PipelineModel):
    """
    Applies feature transformation and runs inference using the loaded model.
    """
    feature_cols = [
        "HighCholest",  
        "HighBP",
        "BMI",
        "Age",
        "Education",
        "income",
        "log_BMI",
        "log_Age",
        "BMI_squared"
    ]

    # Ensure all required columns exist
    missing_cols = [col for col in feature_cols if col not in sample_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in input DataFrame: {missing_cols}")

    # Check if 'features' column already exists
    if "features" not in sample_df.columns:
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        sample_df = assembler.transform(sample_df)

    print("Inferenceing sample silver table data...")
    # Perform inference
    predictions = loaded_model.transform(sample_df)
    
    return predictions.select("prediction")  # Return only predictions

# COMMAND ----------

silver_df = create_streaming_features(silver_table_name)
loaded_model = load_ml_model('dev')
sample_prediction = make_prediction(silver_df, loaded_model)
display(sample_prediction)