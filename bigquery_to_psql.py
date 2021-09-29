# doc: https://googleapis.dev/python/bigquerystorage/latest/index.html

import psycopg2
from psycopg2 import extras

import sys

import threading

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

# 引数
bq_table_name = sys.argv[1]
psql_table_name = sys.argv[2]

argc = len(sys.argv) - 1
if argc != 2:
    print(f"Invalid arguments (args:{sys.argv[1:]})")
    sys.exit(1)

# スレッド数
max_thread_num = 10

# Project ID
project_id = "censys-research-311808"
# Dataset Name
dataset_name = "ca_cert"

table = "projects/{}/datasets/{}/tables/{}".format(
    project_id, dataset_name, bq_table_name
)

# Postgresql settings
dsn = "postgresql://root:root@localhost/ca_cert_pem"

batch_size = 1000

def get_connection():
    return psycopg2.connect(dsn)

def insert(_conn, _cur, _list):
    extras.execute_values(
        _cur,
        f"INSERT INTO {psql_table_name} VALUES %s",
        _list
    )
    _conn.commit()

def read_stream(_index, _reader):

    print(f"Start thread (index:{_index})")

    # pip install google-cloud-bigquery-storage[fastavro]
    rows = _reader.rows(session)

    # postgresqlに接続
    conn = get_connection()
    cur = conn.cursor()

    # Do any local processing by iterating over the rows. The
    # google-cloud-bigquery-storage client reconnects to the API after any
    # transient network errors or timeouts.
    insert_list = []
    i = 0
    for row in rows:
        fp = row["fingerprint_sha1"]
        insert_list.append((fp,""))
        i += 1
        if batch_size <= i:
            insert(conn, cur, insert_list)
            i = 0
            insert_list.clear()
    if i > 0:
        insert(conn, cur, insert_list)
        i = 0
        insert_list.clear()

    cur.close()
    conn.close()

def create_read_session():

    s = types.ReadSession()
    s.table = table

    # This API can also deliver data serialized in Apache Arrow format.
    # This example leverages Apache Avro.
    s.data_format = types.DataFormat.AVRO

    # 出力する列の指定　
    s.read_options.selected_fields = ["fingerprint_sha1"]

    # 読み込む条件を指定する場合に使う
    #s.read_options.row_restriction = 'state = "WA"'

    # 必要ならスナップショットの時間を指定
    #if snapshot_millis > 0:
        #snapshot_time = types.Timestamp()
        #snapshot_time.FromMilliseconds(snapshot_millis)
        #s.table_modifiers.snapshot_time = snapshot_time

    return s

requested_session = create_read_session()

client = BigQueryReadClient()

session = client.create_read_session(
    parent = "projects/{}".format(project_id),
    read_session = requested_session,
    # We'll use only a single stream for reading data from the table. However,
    # if you wanted to fan out multiple readers you could do so by having a
    # reader process each individual stream.
    max_stream_count = max_thread_num,
)

thread_num = len(session.streams)

threads = []

print(f"Create ReadSession (stream_count:{thread_num})")

for i in range(thread_num):
    r = client.read_rows(session.streams[i].name)
    t = threading.Thread(target=read_stream, args=(i,r))
    threads.append(t)

for t in threads:
    t.setDaemon(True)
    t.start()

for t in threads:
    t.join()

