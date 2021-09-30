import psycopg2
import sys
import time

import requests
from requests.auth import HTTPBasicAuth

# 引数
psql_table_name = sys.argv[1]

batch_size = 10

# Censys settings
endpoint = "https://search.censys.io"

with open('cache/censys.secret', 'r') as f:
    censys_api_id = f.readline().rstrip('\n')
    censys_api_secret = f.readline().rstrip('\n')

# Postgresql settings
dsn = "postgresql://root:root@localhost/ca_cert_pem"

def get_connection():
    return psycopg2.connect(dsn)

def get_fingerprint_list(_cur, _table):

    fingerprint_list = []

    query = f"SELECT fingerprint_sha256 from {_table} WHERE TRIM(pem) IS NULL"
    _cur.execute(query)
    for row in _cur:
        fingerprint_list.append(row[0])

    return fingerprint_list

def fetch_cert(_fingerprint_sha256):

    resp = requests.get(endpoint + f"/certificates/{_fingerprint_sha256}/pem/raw", auth=HTTPBasicAuth(censys_api_id, censys_api_secret))

    print(endpoint + f"/certificates/{_fingerprint_sha256}/pem/raw")

    if resp.status_code == 429:
        print(f"Error (status_code: {resp.status_code})")
        return "", True

    if resp.status_code != 200:
        print(f"Error (status_code: {resp.status_code})")
        return "", False

    if resp.text[0:27] != "-----BEGIN CERTIFICATE-----":
        print(f"Error (status: not certificate)")
        return "", False

    return resp.text, False

def update_db(_conn, _cur, _table, _cert_list):

    query = f"UPDATE {_table} SET pem = CASE "
    for c in _cert_list:
        query += f"WHEN fingerprint_sha256 = \'{c[0]}\' THEN \'{c[1]}\' "
    query += "END;"

    _cur.execute(query)
    _conn.commit()

def formatPem(_pem_before):
    num = 64
    tmp = []
    for i in range(num):
        tmp.append(_pem_before[i::num])
    tmp = ["".join(i) for i in zip(*tmp)]
    rem = len(_pem_before) % num  # zip で捨てられた余り
    if rem:
        tmp.append(_pem_before[-rem:])

    pem_after = '-----BEGIN CERTIFICATE-----\n'
    pem_after += '\n'.join(tmp) + '\n'
    pem_after += '-----END CERTIFICATE-----\n'
    return pem_after


# postgresqlに接続
with get_connection() as conn:
    with conn.cursor() as cur:

        fingerprint_list = get_fingerprint_list(cur, psql_table_name)

        cert_list = []
        i = 0
        for fp in fingerprint_list:
            while True:
                pem, retry = fetch_cert(fp)
                if not retry:
                    break
                time.sleep(100)
            time.sleep(6)
            cert_list.append((fp, pem))
            i += 1
            if batch_size <= i:
                update_db(conn, cur, psql_table_name, cert_list)
                i = 0
                cert_list.clear()

        if i > 0:
            update_db(conn, cur, psql_table_name, cert_list)
            i = 0
            cert_list.clear()

