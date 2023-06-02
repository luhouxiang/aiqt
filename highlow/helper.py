# -*- coding: utf-8 -*-
"""
@author: luhx
@file: helper.py
@time: 2023/5/31 21:41
@desc:
"""
import string
import pandas as pd
from sqlalchemy import create_engine
from kds_util.user_logbook import user_log as logger


def init_ths_trading_day(ip):
    """
    初始化交易日
    :return:
    """
    engine = create_engine(
        f'mysql+pymysql://root:2234567@{ip}:3306/hqdb?charset=utf8')  ###建立连接↓qh_index_db
    con = engine.connect()
    sql = "SELECT trading_day from tbl_ths_trading_day order by trading_day asc"
    logger.info(sql)
    df = pd.read_sql(sql, con)
    con.close()
    if len(df) == 0:
        return
    trading_days = []
    for i in range(len(df)):
        trading_day = df["trading_day"].iloc[i]
        trading_days.append(trading_day)
    days = pd.DataFrame()
    days["trading_days"] = trading_days
    return days


def get_breed_from_contract(code):
    """根据合约解析得到品种"""
    return code.rstrip(string.digits)