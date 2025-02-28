from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Snowflake 데이터베이스 연결을 위한 Airflow Hook
import pendulum  # 타임존 처리를 위한 라이브러리
import datetime  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터
import pandas as pd  # 데이터프레임 작업을 위한 pandas

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")
var_batch_id = Variable.get("whole_table_daily_verifi_batch_id")

def split_dataframe(df, chunk_size):
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i : i + chunk_size]

# DAG 정의
with DAG(
    dag_id="hdhs_whole_table_pk_error_merge",  # DAG의 고유 식별자
    start_date=pendulum.datetime(2025, 1, 22, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,  # 과거 데이터 실행을 스킵
    dagrun_timeout=datetime.timedelta(minutes=1000),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑", "POC", "검증"]  # DAG에 붙일 태그
) as dag:

    # Oracle 데이터 조회 및 XCom으로 데이터 전달
    @task(task_id='oracle_value_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def error_merge():
        
        ################
        # 기본 환경 세팅 #
        ################
                
        #고유 batch_id 입력
        batch_id = var_batch_id
        
        # Snowflake Hook을 사용하여 연결 생성
        snowflake_hook = SnowflakeHook(snowflake_conn_id="conn_snowflake_load_verifi")
        
        # Oracle Hook을 사용하여 연결 생성
        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_OCI', thick_mode=True, thick_mode_lib_dir=client_path)
        oracle_connection = oracle_hook.get_conn()
        engine = snowflake_hook.get_sqlalchemy_engine()
        
        ################
        # CHG_DIFF 처리 #
        ################        
        
        chg_diff_table_query = f"""
            SELECT A.TABLE_NM, B.TABLE_SCHEMA
            FROM DW_LOAD_DB.VERIFI_DATA.CDC_DATA_VERIFI_LOG A, DW_LOAD_DB.CONFIG.TB_CDC_LIST B
            WHERE A.TABLE_NM = B.TABLE_NAME
              AND A.BATCH_ID = '{batch_id}'
              AND (A.CHG_DTM_DIFF_COUNT != 0 OR A.ORACLE_ONLY_COUNT != 0)
          GROUP BY A.TABLE_NM, B.TABLE_SCHEMA
        """
        print(chg_diff_table_query)
        
        chg_diff_table_df = pd.read_sql(chg_diff_table_query, con=engine)
        
        print(chg_diff_table_df)
        
        # 테이블 별 MERGE 처리
        for row in chg_diff_table_df.itertuples(index=False):
            try:
                # ① 해당 테이블 PK 조건 확인
                table_pk_list_query = f"""
                    SELECT COLUMN_NAME
                      FROM DW_LOAD_DB.VERIFI_DATA.TB_KEY_V2
                     WHERE TABLE_NAME = '{row.table_nm}'
                """
                print(table_pk_list_query)
            
                table_pk_list_df = pd.read_sql(table_pk_list_query, con=engine)
                
                pk_columns = table_pk_list_df['column_name'].tolist()
                pk_column_str = ", ".join(pk_columns)
                print(pk_column_str)
                
                # ② CHG_DIFF PK 대상 조회
                chg_diff_pk_list_query = f"""
                   SELECT {pk_column_str}
                     FROM DW_LOAD_DB.VERIFI_DATA.TEMP_{row.table_nm}_DIFF_LOG
                    WHERE BATCH_ID = '{batch_id}'
                """
                print(chg_diff_pk_list_query)

                chg_diff_pk_list_df = pd.read_sql(chg_diff_pk_list_query, con=engine)
                chg_diff_pk_list_df.columns = chg_diff_pk_list_df.columns.str.upper()
                print(chg_diff_pk_list_df)
                
                # ② ORACLE_ONLY PK 대상 조회
                ora_only_pk_list_query = f"""
                   SELECT {pk_column_str}
                     FROM DW_LOAD_DB.VERIFI_DATA.TEMP_{row.table_nm}_ORA_ONLY_LOG
                    WHERE BATCH_ID = '{batch_id}'
                """
                print(ora_only_pk_list_query)

                ora_only_pk_list_df = pd.read_sql(ora_only_pk_list_query, con=engine)
                ora_only_pk_list_df.columns = ora_only_pk_list_df.columns.str.upper()
                print(ora_only_pk_list_df)
                
                # 두 DataFrame을 합치기
                final_pk_list_df = pd.concat([chg_diff_pk_list_df, ora_only_pk_list_df], ignore_index=True)
                print(final_pk_list_df)

                pk_values_batch = []  # 1000건씩 처리하기 위한 리스트
                
                # ④ ORACLE에서 PK조건으로 전체 컬럼 조회
                for _, pk_values in final_pk_list_df.iterrows():
                    pk_value_batch = tuple(pk_values[col] for col in pk_columns)
                    pk_values_batch.append(pk_value_batch)
    
                    if len(pk_values_batch) == 1000 or pk_values.equals(final_pk_list_df.iloc[-1]):
                        pk_value_list = ", ".join([f"({', '.join([repr(val) for val in batch])})" for batch in pk_values_batch])
                        
                        ora_value_query = f"""
                            SELECT *
                              FROM {row.table_schema}.{row.table_nm}
                             WHERE ({', '.join(pk_columns)}) IN ({pk_value_list})
                        """
                        print(ora_value_query)
                        pk_value_list = ""

                        ora_value_df = pd.read_sql(ora_value_query, con=oracle_connection)
                        ora_value_df.columns = ora_value_df.columns.str.upper()
                        print(ora_value_df)
                        
                        # 리스트 초기화
                        pk_values_batch = []
                        insert_values = []
                        
                        # Oracle 데이터가 있을 경우만 실행
                        if not ora_value_df.empty:
                            # ⑤ SNOW TEMP테이블에 LOAD 데이터 강제 INSERT
                            target_columns = [col for col in ora_value_df.columns]
                            print("Target Columns:", target_columns)

                            # 첫 번째 행의 데이터를 기준으로 "값 AS 컬럼" 형식 생성
                            for _, row_data in ora_value_df.iterrows():
                                values_clause = ", ".join([
                                    f"NULL" if pd.isna(row_data[col]) else
                                    f"{repr(row_data[col])}" if isinstance(row_data[col], str) else
                                    f"'{row_data[col]}'"
                                    for col in target_columns
                                ])
                                
                                pk_values_clause = ", ".join([f"'{row_data[col]}'" for col in pk_columns])  # PK 값

                                insert_values.append((
                                    "'U'",  # OP
                                    "'0'",  # TRANSACT_ID
                                    "'0'",  # FILE_NAME
                                    "'0'",  # FILE_OFFSET
                                    pk_values_clause,  # PK 값
                                    values_clause  # Other columns values
                                ))
                            
                            # 여러 행을 한번에 처리하는 INSERT 쿼리
                            insert_query = f"""
                                INSERT INTO DW_LOAD_DB.TEMP.TEMP_{row.table_nm}(
                                                                                        OP,
                                                                                        TRANSACT_ID,
                                                                                        FILE_NAME,
                                                                                        FILE_OFFSET,
                                                                                        {', '.join([f'PK_{col}' for col in pk_columns])},
                                                                                        {', '.join([col for col in target_columns])}
                                                                                        )
                                VALUES
                                {', '.join([f"({', '.join(map(str, batch))})" for batch in insert_values])}
                            """
                            
                            with snowflake_hook.get_conn() as snowflake_conn:
                                with snowflake_conn.cursor() as cursor:
                                    cursor.execute(insert_query)
                                    print(insert_query)
                                    
                            # ⑥ MERGE 프로시저 실행
                            
                            # MERGE문 프로시저 실행
                            merge_proc_query = f"""
                                CALL DW_LOAD_DB.CONFIG.PROC_CDC_MERGE_DTL('DW_LOAD_DB', '{row.table_schema}' ,'{row.table_nm}')
                            """
                            
                            with snowflake_hook.get_conn() as snowflake_conn:
                                with snowflake_conn.cursor() as cursor:
                                    cursor.execute(merge_proc_query)
                                    print(merge_proc_query)
                                    

            except Exception as e:
                print(f"Error with value: {e}")
                
        oracle_connection.close()
        
    error_merge()
