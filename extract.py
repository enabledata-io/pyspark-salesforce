from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from sfdclib import SfdcSession, SfdcToolingApi

import re

# needs to setup spark session

def extract(self, origin_table_name: str, spark) -> DataFrame:
    username = ''
    password = ''
    security_token = ''    

    s = SfdcSession(username=username, password=password, token=security_token, is_sandbox=False)
    s.login()

    tooling = SfdcToolingApi(s)
    result = tooling.anon_query(f"SELECT QualifiedApiName,MasterLabel FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName = '{origin_table_name}'")

    columns_labels = dict()
    for r in result['records']:
        if r['MasterLabel'] != None:
            if r['QualifiedApiName'].endswith("Id"):
                new_column = r['QualifiedApiName']
                new_column = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', new_column)
                new_column = ' '.join(new_column)
            else:
                new_column = r['MasterLabel']
                
            new_column = re.sub(r'[^A-Za-z0-9 ]+', '', new_column)
            new_column = new_column.split()
            new_column = '_'.join(new_column)
            new_column = new_column.lower()
            if new_column not in columns_labels.values(): # avoid duplicated columns 
                columns_labels[r['QualifiedApiName']] = new_column

    soql = f"SELECT FIELDS(ALL) FROM {origin_table_name} LIMIT 1"

    df = spark.read.format("com.springml.spark.salesforce") \
        .option("username", username) \
        .option("password", f"{password}{security_token}") \
        .option("soql", soql) \
        .option("version", "58.0") \
        .load()

    columns = df.columns

    query_columns = ', '.join(columns)

    soql = f"SELECT {query_columns} FROM {origin_table_name}"

    df = spark.read.format("com.springml.spark.salesforce") \
        .option("username", username) \
        .option("password", f"{password}{security_token}") \
        .option("soql", soql) \
        .option("version", "58.0") \
        .load()
    
    return df
