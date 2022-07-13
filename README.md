#Pipeline End to End of Basket Analysis Algorithm with Ephemeral Cluster.

This code is a End to End Pipeline to run a Basket Analysis Algorithm in pyspark orchestrated by Airflow creating a Spark cluster in Google Cloud Plataform. <br />
<br />
Step 1: Creation of a Dataproc Cluster with all required libraries.<br />
Step 2: Submit of Baskt Analysis Algorithm to the Cluster.<br />
Step 3: Sending the Output of Algorithm to GCS<br />
Step 4: Killing the Cluster<br />
