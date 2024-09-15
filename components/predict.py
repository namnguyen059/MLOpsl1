import pandas as pd
import pickle
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid

# Cassandra connection details
cassandra_host = 'cassandra' 
cassandra_port = 9042
cassandra_username = 'cassandra'
cassandra_password = 'cassandra'
keyspace = 'model_repo'
table = 'models'

# Model metadata
model_name = 'house_price_prediction'
model_version = '1.0'  

# Debugging: Log or print to confirm execution and path
print("Making predictions...")

# Load the data
df = pd.read_csv('/opt/airflow/components/house_prices.csv')
X = df[['feature1', 'feature2']]  # Replace with your actual feature names

# Cassandra connection setup
auth_provider = PlainTextAuthProvider(username=cassandra_username, password=cassandra_password)
cluster = Cluster([cassandra_host], port=cassandra_port, auth_provider=auth_provider)
session = cluster.connect(keyspace)

# Query to retrieve models based on model name
query = f"""
SELECT model_id, model_file, created_at FROM {table} 
WHERE model_name = %s ALLOW FILTERING
"""

# Execute the query to get all models for the given name
rows = session.execute(query, [model_name])

# Sort the models by 'created_at' in descending order to get the latest one
sorted_rows = sorted(rows, key=lambda row: row.created_at, reverse=True)

if sorted_rows:
    latest_model_row = sorted_rows[0]
    print(f"Loading model '{model_name}' from Cassandra (created at {latest_model_row.created_at})...")
    # Deserialize the model
    model = pickle.loads(latest_model_row.model_file)
else:
    raise ValueError(f"Model '{model_name}' not found in Cassandra.")


# Make predictions
predictions = model.predict(X)

# Save the predictions
pred_path = '/opt/airflow/output/house_price_predictions.csv'
print(f"Saving predictions to {pred_path}")
df['predictions'] = predictions
df.to_csv(pred_path, index=False)

print("Predictions saved successfully.")
