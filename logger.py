import logging
import os
import re
from logging.handlers import TimedRotatingFileHandler

def setup_logger(log_path):
    # 지정한 경로의 디렉터리가 존재하지 않으면 디렉터리를 생성
    directory = os.path.dirname(log_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    # 로거 객체 생성
    logger = logging.getLogger("PabatLogger")
    logger.setLevel(logging.DEBUG)  # 로그 레벨 설정

    # TimedRotatingFileHandler 설정
    handler = TimedRotatingFileHandler(log_path, when="midnight", interval=1, backupCount=30)
    handler.suffix = "%Y%m%d"  # 로그 파일명에 날짜를 추가하기 위한 형식
    handler.extMatch = re.compile(r"^\d{8}$")  # 파일 확장자 매치

    # 로그 포맷 설정
    formatter = logging.Formatter('%(asctime)s [%(name)s] [%(levelname)s] :: %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

