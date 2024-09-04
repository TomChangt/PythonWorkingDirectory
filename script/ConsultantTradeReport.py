import os
import sys
import pandas as pd
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging
from datetime import datetime, timedelta
from email.mime.text import MIMEText

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# 获取文件名（不带扩展名）
current_file_name = os.path.splitext(os.path.basename(__file__))[0].upper()


# 加载环境变量
load_dotenv()
