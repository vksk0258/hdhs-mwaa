from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

md_ven_intl_setup_dtl_columns = ["ALML_CD", "MD_CD", "VEN_CD", "VEN2_CD", "INTL_YN", "CHG_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]
item_intl_dtl_columns = ["ALML_CD", "SLITM_CD", "ALML_ITEM_CD", "ALML_CO_GBCD", "ALML_INTL_NO", "SOON_USE_PRMO_NO", "SOON_USE_PRMO_PRC", "ADD_DC_PRMO_NO", "ADD_DC_PRMO_PRC", "SELL_PRC", "ALML_SELL_GBCD", "SELL_GBCD", "ITNT_DISP_YN", "ITEM_PRC_APLY_DTM", "VEN_CD", "VEN2_CD", "OSHP_VEN_ADR_SEQ", "RTP_EXCH_VEN_ADR_SEQ", "SDLVC_VEN_SEQ", "DLVC_PAY_GBCD", "BNDL_DLVC_GBCD", "NCHG_DLV_BSIC_AMT", "DLVC_BSIC_QTY", "DLV_COST", "RTP_DLV_COST", "EXCH_DLV_COST", "SEND_DTM", "ALML_INTL_RST_GBCD", "ALML_ERR_CD", "ALML_ERR_MSG", "ORGL_ALML_ITEM_CD", "ALML_APRVL_STAT_CD", "ALML_PRC_APRVL_STAT_CD", "RJT_PTC_RSN", "RPROC_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]
intl_excp_setup_dtl_columns = ["ALML_CD", "MD_CD", "VEN_CD", "VEN2_CD", "ITEM_INTL_GBCD", "ITEM_INTL_PTC_CD", "RMRK", "INTL_YN", "CHG_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]



with DAG(
    dag_id="dag_CDC_ODS_SUB_ALLI_01",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    @task(task_id='task_AM_ALML_MD_VEN_INTL_SETUP_DTL_I')
    def task_AM_ALML_MD_VEN_INTL_SETUP_DTL_I(**kwargs):
        postgres_hook = PostgresHook(postgres_conn_id='conn_postgres_hdhs_reading')
        with postgres_hook.get_conn() as postgres_conn:
            query ="select * from AM_ALML_MD_VEN_INTL_SETUP_DTL"
            df = pd.read_sql(query, postgres_conn)
            return df


    task_AM_ALML_MD_VEN_INTL_SETUP_DTL_I()