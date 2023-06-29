'''
in this module we define functions used to communicate with the data hub/impala

'''

# os.system("python3 -m pip install impyla==0.13.8")
# os.system("python3 -m pip install thrift==0.9.3")
# os.system("python3 -m pip install thrift_sasl==0.2.1")
# os.system("python3 -m pip install sasl==0.2.1")

import time
import re
import pandas as pd

import impala.util
from impala.dbapi import connect
from impala.hiveserver2 import HiveServer2Cursor
from impala.error import HiveServer2Error

def setup_remote_sql() -> HiveServer2Cursor:
  # connection details
  IMPALA_HOST='s-m-prddhdata04.internal.sky.de'
  IMPALA_PORT=21050

  # creating connection to impala
  conn = connect(host=IMPALA_HOST,\
                 port=IMPALA_PORT,\
                 auth_mechanism='GSSAPI',\
                 timeout=100000,\
                 use_ssl=True,\
                 database='cdsw',\
                 ca_cert='/opt/cloudera/security/x509/truststore.pem',\
                 ldap_user=None,\
                 ldap_password=None,\
                 kerberos_service_name='impala')
  return conn.cursor()


class ImpalaCursor(object):
    def __init__(self):
        self.IMPALA_HOST = 's-m-prddhdata04.internal.sky.de'
        self.IMPALA_PORT = 21050
        self.cursor: HiveServer2Cursor = connect(
            host=self.IMPALA_HOST,
            port=self.IMPALA_PORT,
            auth_mechanism='GSSAPI',
            timeout=100000,
            use_ssl=True,
            database='cdsw',
            ca_cert='/opt/cloudera/security/x509/truststore.pem',
            ldap_user=None,
            ldap_password=None,
            kerberos_service_name='impala'
        ).cursor()
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.cursor.close()
        if type is not None or value is not None or traceback is not None:
            print(f"Exception {type}:")
            print(value)
            print(traceback)
        return True
    
    def __del__(self):
        try:
            self.cursor.close()
        except HiveServer2Error:
            pass

    def execute(self, sql: str):
        self.cursor.execute(sql)
    
    def to_df(self) -> pd.DataFrame:
        return impala.util.as_pandas(self.cursor)

    def execute_to_df(self, sql: str) -> pd.DataFrame:
        self.execute(sql)
        return self.to_df()


def process_sql(sql, steps):
  for file_name in steps:
    with open(file_name,"r") as f:
      #remove comments
      text1=re.sub(r'--.*(\n|\Z)', ' ',f.read())
      text2=re.sub(r'/\*.*?(\n.*?)*?\*/', ' ',text1)
      text3=text2.replace('\n',' ').replace('\t',' ')
      querries=re.sub(r'\s+',' ',text3).split(';')
      for i,q in enumerate(querries):
        print("{} > {} of {} >> {}".format(file_name,i+1,len(querries),q[:50]+'...'))
        if q.strip():
          #print("{} > {} of {} >> {}".format(file_name,i+1,len(querries),q))
          sql.execute(q.strip())
          time.sleep(1)
  return
