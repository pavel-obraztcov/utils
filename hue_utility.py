import pandas as pd
import numpy as np
import os

def create_hue_table(input_df, dir_target, tbl_target, run_query=True):
  """
  This function takes a pandas dataframe (input_df) as input and creates a new table on
  the data hub with the specified path (dir_target, tbl_target).
  """
  
  # TO_DO: check if input is correct
  
  # fetch colum names and column types
  col_names = input_df.columns.values
  col_types = input_df.dtypes.values
  
  # create list of column names and types for impala query
  query_cols = ""
  for n, t, i in zip(col_names, col_types, np.arange(len(col_names))):
    if t in ["int","Int8","Int16","Int32","Int64"]:
      query_cols += " {} {} ".format(n, "INT")
    if t in ["float","float16","float32","float64"]:
      query_cols += " {} {} ".format(n, "FLOAT")
    if t == "bool":
      query_cols += " {} {} ".format(n, "BOOLEAN")
    if t == "datetime64[ns]":
      query_cols += " {} {} ".format(n, "TIMESTAMP")
    if t == "O":
      query_cols += " {} {} ".format(n, "STRING")
    
    if i < len(col_names)-1:
      query_cols += ", \n"
      
  full_query = """
  -- create staging table based on uploaded csv-file(s)
  DROP TABLE IF EXISTS REPLACE_DIR.REPLACE_TBL_0 PURGE;
  CREATE EXTERNAL TABLE REPLACE_DIR.REPLACE_TBL_0
  (
  REPLACE_COL
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY "|"
  LOCATION "/project/REPLACE_DIR/REPLACE_TBL_0/";
  
  DROP TABLE IF EXISTS REPLACE_DIR.REPLACE_TBL PURGE;
  CREATE TABLE REPLACE_DIR.REPLACE_TBL
  WITH SERDEPROPERTIES ('serialization.encoding'='UTF-8')
  STORED AS PARQUET
  AS 
  SELECT
  *
  FROM REPLACE_DIR.REPLACE_TBL_0
  ;
  
  invalidate metadata REPLACE_DIR.REPLACE_TBL;
  compute stats REPLACE_DIR.REPLACE_TBL;
  
  DROP TABLE IF EXISTS REPLACE_DIR.REPLACE_TBL_0 PURGE;
  """
  
  # replace query
  full_query = full_query.replace("REPLACE_COL", query_cols)
  full_query = full_query.replace("REPLACE_DIR", dir_target)
  full_query = full_query.replace("REPLACE_TBL", tbl_target)
  
  print("Query that will be used:")
  print(full_query)
  
  # save dataframe as csv without headers and index
  input_df.to_csv("{}.csv".format(tbl_target), header=False, index=False, sep="|")
  
  # save query to sql file
  file = open("upload.sql", "x")
  file.write(full_query)
  file.close()
  
  # upload csv to hdfs
  print("Uploading csv to hdfs...")
  os.system("hdfs dfs -mkdir -p /project/{}/{}/".format(dir_target, tbl_target+"_0"))
  os.system("hdfs dfs -chmod 777 /project/{}/{}/".format(dir_target, tbl_target+"_0"))
  os.system("hdfs dfs -put -f \"{}.csv\" /project/{}/{}/".format(tbl_target, dir_target, tbl_target+"_0"))
  os.system("hdfs dfs -chmod 777 \"/project/{}/{}/{}.csv\"".format(dir_target, tbl_target+"_0", tbl_target))
  
  # create base data table
  if run_query: 
    print("Creating impala table...")
    os.system("(impala-shell -i s-m-prddhdata02.internal.sky.de -k -d cdsw \
      --ssl --ca_cert=/opt/cloudera/security/x509/truststore.pem -f upload.sql)")
    
    # delete sql cdsw
    os.remove("upload.sql")
  os.remove("{}.csv".format(tbl_target)) 
  
  print("Done.")