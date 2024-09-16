import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.metrics import mean_squared_error
import pickle
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Cassandra connection details
cassandra_host = 'cassandra'
cassandra_port = 9042
cassandra_username = 'cassandra'
cassandra_password = 'cassandra'
keyspace = 'model_repo'
table = 'models'
model_name = 'house_price_prediction'

# Load your test data
test_df = pd.read_csv('/opt/airflow/components/house_prices.csv')
X_test = test_df[['feature1', 'feature2']]
y_test = test_df['price']

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

if not rows:
    raise ValueError(f"No models found for '{model_name}' in Cassandra.")

# Start MLflow experiment tracking
mlflow.set_experiment("House Price Prediction Evaluation")

for row in rows:
    # Sort the models by 'created_at' in descending order
    sorted_rows = sorted(rows, key=lambda r: r.created_at, reverse=True)
    
    for model_row in sorted_rows:
        print(f"Evaluating model '{model_name}' from Cassandra (created at {model_row.created_at})...")
        # Deserialize the model from Cassandra
        model = pickle.loads(model_row.model_file)
        
        with mlflow.start_run():
            # Log model version info (if needed)
            mlflow.log_param("model_version", model_row.created_at)

            # Predict and evaluate
            predictions = model.predict(X_test)
            mse = mean_squared_error(y_test, predictions)
    
            # Log the MSE metric in MLflow
            mlflow.log_metric("mse", mse)
    
            print(f"Evaluation completed for model created at {model_row.created_at}. MSE: {mse}")
    

print("Evaluation of all models logged in MLflow.")
