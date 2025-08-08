import psycopg2
import logging
import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
load_dotenv('.env')
schema_name = os.getenv('SCHEMA_NAME')
table_name = os.getenv('TABLE_NAME')
aws_pw = os.getenv('AWS_PW')
iam_role = os.getenv('IAM_ROLE')
        


conn = psycopg2.connect(
        host='default-workgroup.533267168725.eu-north-1.redshift-serverless.amazonaws.com',
        port='5439',
        dbname='dev',
        user='admin',
        password=aws_pw)


def s3_to_redshift() -> None:
    with conn:
        conn.autocommit = True
        cur = conn.cursor()
    
        cur.execute(f"""
            TRUNCATE TABLE {schema_name}.{table_name}
        """)
        
        cur.execute(f"""
            COPY {schema_name}.{table_name} FROM 's3://ds-spotify-extraction-bucket/dataset.csv'
            IAM_ROLE '{iam_role}' 
            CSV
            IGNOREHEADER 1
            DELIMITER ','
        """)

