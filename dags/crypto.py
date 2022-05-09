import io
import boto3
import botocore
import pandas as pd
import psycopg2
import smtplib, ssl
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import config, postgres_query
from datetime import date, datetime, timedelta
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy import create_engine


def s3_put_object(df, s3_client, s3_bucket, key):
    """Put df to s3 bucket parquet file using conversion to BytesIO object"""
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    s3_client.put_object(Body=buffer.getvalue(), Bucket=s3_bucket, Key=key)


def s3_get_object(s3_client, s3_bucket, key):
    """Get df from s3 bucket parquet file using conversion to BytesIO object"""
    buffer = s3_client.get_object(
        Bucket=s3_bucket,
        Key=key
    )['Body'].read()
    df = pd.read_parquet(io.BytesIO(buffer), engine='pyarrow')
    return df


def get_raw(sources, s3_client, s3_bucket):
    """Get raw data from API sources and put data to s3 bucket.
    If file exists, replace with the files with newly added values."""
    for s in sources:
        try:
            # get data from API
            new_df = pd.read_json(sources[s])
            # get data from s3 bucket
            old_df = s3_get_object(s3, s3_bucket, '%s.parquet' % s)
            # find difference between new and old data
            difference = new_df[~new_df.apply(tuple, 1).isin(old_df.apply(tuple, 1))]
            # put old data + difference to s3 bucket
            s3_put_object(pd.concat([old_df, difference], ignore_index=True),
                          s3_client, s3_bucket, '%s.parquet' % s)
        # if file doesn't exist, create it
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                s3_put_object(new_df, s3_client, s3_bucket, '%s.parquet' % s)


def unify_crypto(s3_client, s3_bucket_get, s3_bucket_put, filename_res='result.parquet'):
    """Get files from s3 bucket, extract required columns, 
    unify all files to the parquet file with single format"""
    bitmex = s3_get_object(s3_client, s3_bucket_get, 'bitmex.parquet')[['timestamp', 'side', 'homeNotional', 'price']]
    bitfinex = s3_get_object(s3_client, s3_bucket_get, 'bitfinex.parquet')[['timestamp', 'type', 'amount', 'price']]
    poloniex = s3_get_object(s3_client, s3_bucket_get, 'poloniex.parquet')[['date', 'type', 'amount', 'rate']]

    # renaming the columns
    bitmex.columns.values[1] = 'type'
    bitmex.columns.values[2] = 'amount'
    poloniex.columns.values[0] = 'timestamp'
    poloniex.columns.values[3] = 'price'
    bitmex['timestamp'] = bitfinex.timestamp.dt.ceil(freq='s')

    # combine all dfs in one
    res = pd.concat([bitmex.assign(source='bitmex'), bitfinex.assign(source='bitfinex'),
                     poloniex.assign(source='poloniex')], ignore_index=True)
    cols = list(res.columns)
    # reorder columns for convenience
    res = res[[cols[-1]] + cols[:-1]]
    # ensure type column values are of the same format
    res['type'] = res['type'].apply(lambda x: x.lower())
    # put df to s3
    s3_put_object(res, s3_client, s3_bucket_put, filename_res)


def send_unified_to_postgres(s3_client, s3_bucket_get, s3_unified_key, engine, sql_table):
    """Get df from s3 bucket parquet file, 
    group data by hour and send to PostgreSQL table."""
    # get unified parquet file from s3 bucket
    df = s3_get_object(s3_client, s3_bucket_get, s3_unified_key)

    # group data by hour, source, type, rate
    df = df.groupby([pd.Grouper(key='timestamp', freq='H'), 'source', 'type', 'price']).sum()

    # send hourly data to PostgreSQL table
    df.to_sql(sql_table, engine, if_exists='replace')


def get_hourly_stats(conn_string, sql_table_stats, local_path):
    """Create table with hourly stats in PostgreSQL and extract data to local csv."""
    # create connection and execute query for statistics table 
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    query = postgres_query.query_stats1 + sql_table_stats + postgres_query.query_stats2 + sql_table_stats + \
            postgres_query.query_stats3
    cursor.execute(query)

    # extract data from PostgreSQL to df and save as csv
    query_get_data = 'SELECT * FROM ' + sql_table_stats
    df = pd.read_sql_query(query_get_data, con=engine)
    df.to_csv(local_path)


def send_stats_email(receivers):
    """Extract df from local file, get data from the previous date, 
    store it locally, and send via email to receivers list."""
    yesterday = date.today() - timedelta(days=1)
    body = 'Hi! You\'re receiving this email because you\'re subscribed to the daily statistics of bitcoin stock exchange.' \
           ' You can find attached daily crypto statistics for %s.' \
           ' Please let %s know if you have any issues with viewing it.' % (yesterday.strftime('%d-%m-%Y'), config.sender_email) 
    sender_email = config.sender_email
    password = config.password

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message['Subject'] = 'Daily statistics of bitcoin stock exchange - %s' % yesterday.strftime('%d-%m-%Y')
    # Add body to email
    message.attach(MIMEText(body, 'plain'))

    # getting statistics for the previous day and storing it as separate file
    hourly_stats = pd.read_csv('./data/processed/crypto_stats_hourly.csv', index_col=0)
    filter_query = pd.to_datetime(hourly_stats['timestamp']).dt.date == yesterday
    hourly_stats_yesterday = hourly_stats[filter_query]
    hourly_stats_yesterday.to_csv('./data/processed/hourly_stats_%s.csv' % yesterday)

    filename = './data/processed/hourly_stats_%s.csv' % yesterday

    # Open file in binary mode
    with open(filename, 'rb') as attachment:
        # Add file as application/octet-stream
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())

    # Add header as key/value pair to attachment part
    part.add_header(
        'Content-Disposition',
        'attachment; filename=crypto_stats_hourly.csv',
    )
    # Add attachment to message and convert message to string
    message.attach(part)

    # Log in to server using secure context and send email
    context = ssl.create_default_context()

    server = smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context)
    server.login(sender_email, password)
    for receiver in receivers:
        message['To'] = receiver
        server.sendmail(sender_email, receiver, message.as_string())


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes=182),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# dag every minute
dag_minute = DAG('send_raw_to_S3', default_args=DEFAULT_ARGS,
                 schedule_interval='* * * * *')

# dag every hour 
dag_hourly = DAG('S3_Postgres_exchange', default_args=DEFAULT_ARGS,
                 schedule_interval='0 * * * *')

# dag every day at 00:01
dag_daily = DAG('send_email', default_args=DEFAULT_ARGS,
                schedule_interval='1 0 * * *')

# API services
services = config.services

s3 = boto3.client('s3',
                  endpoint_url=config.localhost,
                  aws_access_key_id=config.aws_access_key_id,
                  aws_secret_access_key=config.aws_secret_access_key,
                  config=botocore.client.Config(signature_version='s3v4'),
                  region_name='us-east-1')

conn_string = 'postgresql://' + config.postgres_user + ':' + config.postgres_password + '@' + config.postgres_host
engine = create_engine(conn_string)

receivers = ['anna.lotica@gmail.com', 'anton.v.semenenko@gmail.com']

t1 = PythonOperator(
    task_id='get_raw',
    provide_context=True,
    python_callable=get_raw,
    op_kwargs={
        'sources': services,
        's3_client': s3,
        's3_bucket': 'crypto-raw'
    },
    dag=dag_minute
)

t2 = PythonOperator(
    task_id='unify_crypto',
    provide_context=True,
    python_callable=unify_crypto,
    op_kwargs={
        's3_client': s3,
        's3_bucket_get': 'crypto-raw',
        's3_bucket_put': 'crypto-processed'
    },
    dag=dag_hourly
)

t3 = PythonOperator(
    task_id='send_unified_to_postgres',
    provide_context=True,
    python_callable=send_unified_to_postgres,
    op_kwargs={
        's3_client': s3,
        's3_bucket_get': 'crypto-processed',
        's3_unified_key': 'result.parquet',
        'engine': engine,
        'sql_table': 'crypto_hourly'
    },
    dag=dag_hourly
)

t4 = PythonOperator(
    task_id='get_hourly_stats',
    provide_context=True,
    python_callable=get_hourly_stats,
    op_kwargs={
        'conn_string': conn_string,
        'sql_table_stats': 'crypto_hourly_stats',
        'local_path': './data/processed/crypto_stats_hourly.csv'
    },
    dag=dag_hourly
)

t5 = PythonOperator(
    task_id='send_stats_email',
    provide_context=True,
    python_callable=send_stats_email,
    op_kwargs={
        'receivers': receivers
    },
    dag=dag_daily
)

t2 >> t3 >> t4
