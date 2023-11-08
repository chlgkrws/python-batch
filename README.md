# Prerequsite

`python 3.7.x` ~

<br/>

# Install dependencies

> pip3 install PyMySQL
>
> pip3 install mysql-connector-python
>
> pip3 install pandas
>
> pip3 install sqlalchemy

<br/>

# Run example

```
python3 pabat.py sample.config
```

<br/>

# Description

원본(Source)DB에서 대상(Target) DB로의 데이터 이관을 위한 파이썬 프로그램

<br/>

### 구성

pabat.py: 데이터이관 메인 로직이 실행되는 프로그램

xxx.config: 접속 DB 정보 및 .schema 파일 정의

xxx.schema: Select문, Insert문 정의

logs/pabat.log: 실행 log 파일

지원되는 DB: [ MariaDB ]

<br/>

# .config properties

설정 프로퍼티는 key=value 형태로 되어있으며 공백은 없어야함.

```python
source_host=                # 원본 DB 호스트
source_port=                # 원본 DB 포트
source_schema_db=           # 작업을 수행할 default 스키마 or database
source_username=            # 연결할 유저 이름
source_password=            # 연결할 유저 패스워드
source_client=MariaDB       # client명 (MariaDB, xxx~)

target_host=                # 대상 DB 호스트
target_port=                # 대상 DB 포트
target_schema_db=           # 작업을 수행할 default 스키마 or database
target_username=            # 연결할 유저 이름
target_password=            # 연결할 유저 패스워드
target_client=MariaDB       # client명 (MariaDB, xxx~)

schema_list=[sample.schema] # 작업 스키마 파일이름(.schema properties 참고)
context_path=/Users/choehagjun/batch-python     # log, .config, .schema 파일이 존재하는 path
log_file_name=logs/pabat.log  # 로그 path

```

<br/>

# .schema properties

Select 컬럼 정보 및 Insert 컬럼 정보 정의

- ---으로 블록을 구분하며, 첫 번째 블록은 Select절에 해당 (Select은 모든 블록에서 1개만 존재)
- 주석 사용제한

```python
# source table
schema_db=                  # [옵션] 스키마 or database, 미작성 시 .config 파일의 source_schema_db 적용
table=                      # Select 대상 table (from ${})
select:                     # Select 키워드, 해당 키워드 아래에 있는 컬럼들을 조회함
column1                     # 조회할 컬럼명
column2
column3
column4
column5 as sample_text      # alias, alias를 사용한다면 아래 insert 문에서도 alias를 사용해야 함.
where:                      # Where 키워드, 각 라인은 ()로 묶이며, 개행은 and로 처리 됨.
column1 is not null or column2 is not null
column4 is not null
---
# target table
schema_db=                  # [옵션] 스키마 or database, 미작성 시 .config 파일의 target_schema_db 적용
table=                      # Insert 대상 table (into ${})
delete_before_execution=true        # 작업 전 target table delete 수행 여부 (true/false)
insert:                     # Insert 키워드column={value} 형태며, value는 Select절에서 조회한 컬럼명 혹은 alias
column1={column1}           # Insert into table (column...) values({value}...)
column2={column2}
column3={column3}
column4={column4}
column5={sample_text}
---
```

<br/>

# Crontab settings

```shell
crontab -e

# add line
* * * * * /usr/bin/python3 /Users/choehagjun/batch-python/pabat.py /Users/choehagjun/batch-python/sample.config

chmod +x pabat.py
```

<br/>

## Etc command

```sh
# 로컬 -> 원격 파일 옮기기
scp -r -P {port} {file path} {user}@{host}:{remote file path}

# 크론탭 재실행
service crond restart
```

<br/>

## Next Step

- Add a join clause.
- Support for multiple database clients.
- Support for multiple data sources, schemas, and tables.
- Custom logic for processing select statement result rows.
- Add update statements.
