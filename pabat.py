import sys
# import psycopg2
import pymysql
import mysql.connector
import pandas as pd
import time
from sqlalchemy import create_engine
from logger import setup_logger

config_file = sys.argv[1]
start_time = time.time()
result_state = 'success'

# config 파일 읽기
with open(config_file, "r") as f:
    lines = f.readlines()

config = {}
for line in lines:
    if line.count('=') != 1:
        continue
    key, value = line.strip().split('=')
    config[key] = value

context_path = config['context_path']
log_file_name = config['log_file_name']
# Log path 설정
log = setup_logger(f'{context_path}/{log_file_name}')

# Source DB 연결 함수
def connect_to_source_db(client, host, port, schema_db, username, password):
    connection = None

    if client == 'MariaDB':
        connection = mysql.connector.connect(
            host=host,
            port=port,
            database=schema_db,
            user=username,
            password=password
        )
    # elif client == 'PostgreSQL':
    #     connection = psycopg2.connect(
    #         host=host,
    #         port=port,
    #         dbname=schema_db,
    #         user=username,
    #         password=password
    #     )

    return connection

# Target DB 연결 함수
def connect_to_target_db(client, host, port, schema_db, username, password):
    connection = None
    if client == 'MariaDB':
        connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{schema_db}"
        connection = create_engine(connection_string)
    # elif client == 'PostgreSQL':
    #     connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{schema_db}"
    #     connection = create_engine(connection_string)

    return connection

# Source DB 연결
source_connection = connect_to_source_db(
    config['source_client'],
    config['source_host'],
    config['source_port'],
    config['source_schema_db'],
    config['source_username'],
    config['source_password']
)

# Target DB 연결
target_connection = connect_to_target_db(
    config['target_client'],
    config['target_host'],
    config['target_port'],
    config['target_schema_db'],
    config['target_username'],
    config['target_password'] 
)

# 연결 확인
if source_connection:
    log.info(f"[Source] Connected to {config['source_client']}")
else:
    log.info(f"[Source] Error connecting to {config['source_client']}") 
    result_state = 'failure'
    

if target_connection:
    log.info(f"[Target] Connected to {config['target_client']}")
else:
    log.info(f"[Target] Error connecting to {config['target_client']}")
    result_state = 'failure'

# where절 처리
def parse_where_clause(where_content):
    where_clauses = ["(" + condition + ")" for condition in where_content if condition]
    return " AND ".join(where_clauses)

def extract_column_name(column):
    if ' as ' in column:
        return column.split(' as ')[1]
    else:
        return column

def get_if_exist_option(value):
    if value.lower() == 'true':
        return 'replace'
    else:
        return 'append'

# 스키마 파일 처리 함수
def process_schema_file(filename):
    global result_state 
    try:
        with open(filename, 'r') as f:
            content = f.read()
            sections = content.split('---')
            
        ##############################################################
        ###################### Source 설정 파싱 ########################
        ##############################################################
        source_config = {}
        source_options = ['schema_db=', 'table=']
        select_lines = sections[0].split('\n')
        for line in select_lines:
            if any(option in line for option in source_options):
                key, value = line.strip().split('=')
                source_config[key] = value

        select_index = select_lines.index("select:")

        if "where:" in select_lines:
            where_index = select_lines.index("where:")
        else:
            where_index = None

        # 컬럼 정보 추출
        columns = select_lines[select_index+1:where_index if where_index is not None else None]
        columns = [col.strip() for col in columns if col.strip()]
        columns_idx = [extract_column_name(col) for col in columns]

        schema_db = source_config.get('schema_db') or config.get('source_schema_db')
        table = source_config.get('table')

        # SELECT CLAUSE
        select_query = f"SELECT {', '.join(columns)} FROM {schema_db}.{table}"

        # where 조건 추출
        if where_index is not None:
            where_conditions = select_lines[where_index+1:]
            where_clause = parse_where_clause(where_conditions)
        else:
            where_clause = ""

        if where_clause:
            # WHERE CLAUSE
            select_query += f" WHERE {where_clause}"

        source_cursor = source_connection.cursor()
        source_cursor.execute(select_query)
        rows = source_cursor.fetchall()                 # excute select query
        
        log.info('Select query = %s', select_query)
        log.info('Result count = %s', len(rows))
    except FileNotFoundError as e:
        log.error('File not found: %s', e)
        
        result_state = 'failure'
        return
    except Exception as e:
        log.error('An unexpected error occurred: %s', e)
        result_state = 'failure'
        return
        
    ##############################################################
    ###################### Target 설정 파싱 ########################
    ##############################################################
    for section in sections[1:]:
        try:
            target_config = {}
            target_options = ['schema_db=', 'table=', 'delete_before_execution=', 'on_error=']
            lines = section.split('\n')
            # Target option 식별
            for line in lines:
                if any(option in line for option in target_options):
                    key, value = line.strip().split('=')
                    target_config[key] = value

            # Target table, schema 정보 추출
            target_schema_db = target_config.get('schema_db') or config.get('target_schema_db')
            target_table = target_config.get('table')

            # Insert clause
            if "insert:" in lines:
                start_idx = lines.index("insert:") + 1
                column_map = {}
                # Insert Column 식별
                for line in lines[start_idx:]:
                    if "=" not in line:
                        continue
                    target_col, source_col = line.split("=")
                    ref_col_name = source_col.strip().replace('{', '').replace('}', '')
                    
                    if target_col not in column_map:
                        column_map[target_col] = []
                    column_map[target_col].append(ref_col_name)

                # Data map & create data frame
                mapped_data = {}
                for df_col in column_map:
                    for ref_col in column_map[df_col]:
                        ref_data_idx = columns_idx.index(ref_col)
                        col_data = [row[ref_data_idx] for row in rows]
                        mapped_data[df_col] = col_data
                df = pd.DataFrame(mapped_data)
                
                # excute 전 삭제 여부
                if_exists_option = get_if_exist_option(target_config.get('delete_before_execution', None))
                log.info(f'existing data -> {if_exists_option}')
            
                df.to_sql(
                    target_table, 
                    schema=target_schema_db, 
                    con=target_connection, 
                    if_exists=if_exists_option, 
                    chunksize=1000, 
                    index=False
                )
        except Exception as e:
            log.error('An unexpected error occurred: %s', e)
            result_state = 'failure'
            return

# 스키마 파일 리스트 처리
schema_files = config['schema_list'].strip('[]').split(', ')
for schema_file in schema_files:
    process_schema_file(f'{context_path}/{schema_file}')

# 연결 종료
source_connection.close()

# 실행 시간 계산
end_time = time.time()
elapsed_time = end_time - start_time

# 실행 시간을 로그로 기록
log.info(f"Execution time: {elapsed_time:.2f} seconds")
log.info(f"result state = {result_state}")
