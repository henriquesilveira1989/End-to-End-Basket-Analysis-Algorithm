# End to End Basket Analysis Algorithm Pipeline with Ephemeral Cluster.

This code is a End to End Pipeline to run a Basket Analysis Algorithm orchestrate by Airflow, creating a Spark cluster in Google Cloud Plataform.

Step 1: Creation of a Dataproc Cluster with all required libraries.
Step 2: Submit of Baskt Analysis Algorithm to the Cluster.
Step 3: Sending the Output of Algorithm to GCS
Step 4: Killing the Cluster
