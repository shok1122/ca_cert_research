import json
import psycopg2
import sys
import time

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

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

def bulk_fetch_cert(_fingerprint_list):

    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        'fingerprints': _fingerprint_list
    }
    json_data = json.dumps(data)
    resp = requests.post(
        endpoint + f"/api/v1/bulk/certificates",
        headers = headers,
        data = json_data,
        auth = HTTPBasicAuth(censys_api_id, censys_api_secret)
    )
    try:
        resp.raise_for_status()
    except RequestException as e:
        print(json_data)
        print(e.response.text)
        return None, True

    resp_dict = json.loads(resp.text)

    fetch_cert_list = {}
    for fp, v in resp_dict.items():
        fetch_cert_list[fp] = v['raw']

    return fetch_cert_list, False

def fetch_cert1(_fingerprint_sha256):

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

def format_pem(_pem_before):
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

        fingerprint_list_all = get_fingerprint_list(cur, psql_table_name)

        cert_list = []
        i = 0
        while True:
            # get 50 certificates from list
            fingerprint_list = fingerprint_list_all[:50]
            fingerprint_list_all = fingerprint_list_all[50:]

            pem_dict, retry = bulk_fetch_cert(fingerprint_list)

            while retry:
                print("Retry")
                time.sleep(10)
                pem_dict, retry = bulk_fetch_cert(fingerprint_list)

            for fp, pem in pem_dict.items():
                print(fp)
                cert_list.append((fp, format_pem(pem)))

            update_db(conn, cur, psql_table_name, cert_list)

            if len(fingerprint_list_all) <= 0:
                break


#        for fp in fingerprint_list:
#            while True:
#                pem, retry = fetch_cert1(fp)
#                if not retry:
#                    break
#                time.sleep(100)
#            time.sleep(6)
#            cert_list.append((fp, pem))
#            i += 1
#            if batch_size <= i:
#                update_db(conn, cur, psql_table_name, cert_list)
#                i = 0
#                cert_list.clear()
#
#        if i > 0:
#            update_db(conn, cur, psql_table_name, cert_list)
#            i = 0
#            cert_list.clear()

