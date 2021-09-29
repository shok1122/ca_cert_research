export GOOGLE_APPLICATION_CREDENTIALS=/home/kakei/dev/bigquery/cache/key.json

conda install -c conda-forge google-cloud-bigquery google-cloud-bigquery-storage google-cloud-storage pandas pyarrow

CREATE TABLE cert_2018 (fingerprint_sha1 char(40) PRIMARY KEY, pem TEXT);
PRIMARY KEYを設定して，重複しないようにする

python bigquery_to_psql.py 2018h1 cert_2018


