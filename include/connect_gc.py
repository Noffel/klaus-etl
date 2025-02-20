from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

_client = None
def get_bq_client():
    # BigQuery client authentication
    global _client
    if not _client:
        hook = BigQueryHook(
            gcp_conn_id='google_cloud_default',
            use_legacy_sql=False
        )
        _client = hook.get_client()
    return _client

client = get_bq_client()