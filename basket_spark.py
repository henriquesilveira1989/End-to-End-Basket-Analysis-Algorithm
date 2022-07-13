import os
import sys
import pandas as pd
import pandas_gbq
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.ml.fpm import FPGrowth
import pyarrow.parquet as pq
import logging
import os
import pandas as pd
from dependencies.connectors.connector_gbq import ConnectorGBQ
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file('./dependencies/keys/key_gbq.json')

dir_root ='gs://PUT_YOUR_GCS_PATH_HERE'
dict_params = { 'path_basket_data': dir_root + '/Data/Basket-analysis-data/'} 

SPATH = '/usr/lib/spark'

def configure_spark(spark_home=None, pyspark_python=None):
    spark_home = spark_home or SPATH
    os.environ['SPARK_HOME'] = spark_home
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64'

    # Add the PySpark directories to the Python path:
    sys.path.insert(1, os.path.join(spark_home, 'python'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'pyspark'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'build'))

    # If PySpark isn't specified, use currently running Python binary:
    pyspark_python = pyspark_python or sys.executable
    os.environ['PYSPARK_PYTHON'] = pyspark_python
    
def get_data(days, store):
   
    query = f"""SELECT ticketID, PLNT_CD, SLS_DT, CSHR_NR, MTRL_CD
        FROM `brlm-web-data.datalab`.FT_SLS_ZREV_TICKETID 
        WHERE SLS_DT  >= '2019-10-01' and SLS_DT  <= '2020-10-31' AND PLNT_CD = {store}
    """
    # read query
    return pandas_gbq.read_gbq(query, project_id="brlm-web-data",credentials=credentials)
    
    
def basket(days, store):
    # TODO: breack int0 small funcitons
    configure_spark(SPATH)
    spark = SparkSession.builder.master("local[1]") \
                        .appName('FPGrowth') \
                        .getOrCreate()
    spark_df = spark.createDataFrame(get_data(days, store))

    transactions = spark_df.groupBy("ticketID")\
                           .agg(F.collect_set("MTRL_CD"))

    
    # fit model
    fpGrowth = FPGrowth(itemsCol="collect_set(MTRL_CD)",
                        minSupport=0.001,
                        minConfidence=0.001)

    model = fpGrowth.fit(transactions)
    model.freqItemsets.show()
    model.associationRules.show(n=5)
    
    # dfs stored in variables
    freq_items = model.freqItemsets
    association_rules = model.associationRules
    spark2 = SparkSession.builder.getOrCreate()

    
    spark3 = SparkSession.builder.getOrCreate()
    
    # dfs stored in variables
   
    association_rules = model.associationRules
    
    association_rules_pandas = association_rules.toPandas()
    
    association_rules_pandas['antecedent'] = association_rules_pandas['antecedent'].astype(str)
    
    # split antecedent collumn
    association_break = association_rules_pandas['antecedent'].str.split(",",n=5,expand=True)
    
    #filter only product and comp 1
    association_break = association_break[[0,1]]
    
    association_break = association_break.rename(columns={0 : 'produto', 1: 'complementar_1'})
    
    # final concat
    df_final = pd.concat([association_break, association_rules_pandas], axis=1)
    
    # drop antecedent collumn
    del df_final['antecedent']
    
    # loop to strip '['
    cols_to_check = df_final.columns
    
    for col in cols_to_check:
        df_final[col] = df_final[col].map(lambda x: str(x).replace('[',''))
    
    # loop to strip ']'
    for col in cols_to_check:
        df_final[col] = df_final[col].map(lambda x: str(x).replace(']',''))
    
    # rename collumns
    df_final = df_final.rename(columns={'consequent' : 'complementar_2', 'confidence':'confiança', 'lift': 'lift'})
    
    # add store collumn
    df_final['PLNT_CD'] = store
	
    df_final.to_parquet(f'gs://analytics-proa-lmbr/Data/Basket-analysis-data/{store}association_rules.parquet')

      
def main():
    
    days = 365
    stores = [
              57, 58
    ]
      1,  2,  3,  4,  5,  7,  9,
               10, 11, 12, 13, 15, 16, 17, 18, 19,
               20, 21, 22, 23, 24, 26, 27, 28,
               32, 33, 34, 35, 36, 38, 39, 41, 42,
               43, 44, 45, 46, 49, 50, 55,
    
    #single_test = [17]
    
    for store in stores:
        print(store)
        basket(days, store)

    print('-------------FIM-------------')
    
    
if __name__ == '__main__':
    main()
