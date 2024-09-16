import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import pickle
import os
import uuid
import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BoundStatement
import mlflow
import mlflow.sklearn

# Cassandra connection details
cassandra_host = 'localhost'
cassandra_port = 9042
cassandra_username = 'cassandra'
cassandra_password = 'cassandra'
keyspace = 'model_repo'
table = 'models'

# Debugging: Log or print to confirm execution and path
print("Training model...")

# Load the data from the mounted directory inside the container 
df = pd.read_csv('/Users/nguyennam/Desktop/MLOpsl1/components/house_prices.csv')

# Prepare the data
X = df[['feature1', 'feature2']]  # Replace with your actual feature names
y = df['price']

# Split the data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train the model
model = LinearRegression()
model.fit(X_train, y_train)

# Serialize the model using pickle
model_data = pickle.dumps(model)

# Create metadata for the model
model_id = uuid.uuid4()
model_name = 'house_price_prediction'
model_version = '1.0'
created_at = datetime.datetime.now()

# Set up MLflow
mlflow.set_experiment("House Price Prediction Experiment")

# Log the model with MLflow
with mlflow.start_run() as run:
    mlflow.log_param("model_name", model_name)
    mlflow.log_param("model_version", model_version)
    mlflow.log_param("created_at", created_at)
    mlflow.sklearn.log_model(model, "model")

    # Get the model URI
    model_uri = mlflow.get_artifact_uri("model")
    # Train the model


    # Save model metadata to Cassandra
    auth_provider = PlainTextAuthProvider(username=cassandra_username, password=cassandra_password)
    cluster = Cluster([cassandra_host], port=cassandra_port, auth_provider=auth_provider)
    session = cluster.connect(keyspace)

    # Prepare the query with placeholders
    insert_query = f"""
    INSERT INTO {table} (model_id, model_name, model_version, model_file, created_at)
    VALUES (?, ?, ?, ?, ?)
    """
    # Prepare and execute the query with the model data
    prepared = session.prepare(insert_query)
    bound_statement = prepared.bind((model_id, model_name, model_version, model_data, created_at))
    session.execute(bound_statement)

print(f"Model {model_name} (ID: {model_id}) saved successfully in Cassandra and MLflow.")
