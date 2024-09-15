```
chmod +x ./script/entrypoint.sh
```
```
docker compose up -d
```
```
docker ps
docker exec -it be2340fd6638 bash
docker exec -it be2340fd6638 airflow dags list
docker exec -it be2340fd6638 airflow dags trigger house_price_dag
docker exec -it be2340fd6638 airflow dags list-runs -d house_price_dag
docker exec -it be2340fd6638 airflow tasks run house_price_dag train_model 2024-09-14T16:04:32.879555+00:00  
docker exec -it be2340fd6638 airflow tasks run house_price_dag train_model 2024-09-13T07:55:06+00:00
```
```
docker exec -it cassandra cqlsh
CREATE KEYSPACE model_repo WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE model_repo;
CREATE TABLE models (
    model_id UUID PRIMARY KEY,
    model_name text,
    model_version text,
    model_file blob,
    created_at timestamp
);
SELECT model_id, model_name, model_version, created_at FROM models;
```
