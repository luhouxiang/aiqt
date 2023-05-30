# -*- coding: utf-8 -*-
"""
@author: luhx
@file: kbase.py
@time: 2023/5/30 7:43
@desc:
"""
from datetime import datetime
import json
import os
import time
from typing import Dict, List, Tuple
import pymysql
import pymysql.cursors
import string
import requests
import re

from kds_util.user_logbook import init_logger as init_logs
from kds_util.user_logbook import system_log as logger
from kds_util.user_logbook import user_log as user_logger


class xjconfig:
    config = {
        "all_acconts": {
            "moni153": {
                "investor_id": "moni153",
                "url": "http://192.168.1.153:8088/api/mgr",
            },
        },
        "headers": {'Content-Type': 'application/json;charset=UTF-8', 'Connection': 'keep-alive'},
        "investor_id": "moni153",
        "kline_url": "http://192.168.1.100:8086",
        "kline_bak_url": "http://192.168.1.101:8086",
        "all_acconts_url": "http://192.168.1.100:8088",
        "dbIP": "192.168.1.100",
    }

    def __init__(self) -> None:
        if os.path.exists("etc/config.json"): self.config.update(json.load(open("etc/config.json", "r")))

        self.config.update(json.load(open("etc/config.json","r"))) if os.path.exists("etc/config.json") else None


def cfg(): return xjconfig().config


def sign(a): return 0 if not a else 1 if a > 0 else -1


def avlist(l): return (sum(l) / len(l)) if l else 0


class TimeProp:
    def __init__(self, ts=0) -> None:
        if type(ts) == str:
            self.time = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').timestamp()
        else:
            # 小数字, 返回当前时间前后ts秒 # 1000000000 = 2001-09-09 09:46:40 = 31.7年
            self.time = ts if ts >= 1000000000 else time.time() + ts

    @property
    def intTradeDay(self):  # 根据时标获取交易日
        tm = datetime.fromtimestamp(self.time + 14400)  # 加4个小时,判断日期 4*3600
        if tm.weekday() > 4: tm = datetime.fromtimestamp(
            self.time + 1440 + (7 - tm.weekday()) * 86400)  # 周六是5 周日是6 周五夜盘,加完是周六,需要再加多两天 (48+4)*3600
        return int(tm.strftime("%Y%m%d"))

    @property
    def intTradeTime(self):  # 根据时标获取交易时间 int
        return int(self.dt.strftime("%H%M%S"))

    @property
    def intSeg(self):  # 0:夜盘 1:上午盘1 2:上午盘2 3:下午盘
        tt = self.intTradeTime
        if 80000 <= tt < 102000: return 1
        if 102000 <= tt < 120000: return 2
        if 120000 <= tt < 200000: return 3
        return 0

    @property
    def trade_day(self):  # 根据时标获取交易日
        return self.intTradeDay

    @property
    def dt(self):
        return datetime.fromtimestamp(self.time)

    @property
    def trade_date(self):
        return self.dt.strftime("%Y-%m-%d")

    @property
    def trade_time(self):
        return self.dt.strftime("%Y-%m-%d %H:%M:%S")


class KLine(TimeProp):
    def __init__(self, ktime=0, open=0.0, high=0.0, low=0.0, close=0.0, volume=0, oi=0, instrument="", period=1):
        self.time: int = ktime
        self.open: float = open
        self.close: float = close
        self.high: float = high
        self.low: float = low
        self.volume: int = volume
        self.oi: int = oi
        self.instrument: str = instrument  # 合约号
        self.period: int = period
        self.signal: int = 0

    def appendkline(self, kline):  # 合并k线，首条k线需要把time清零
        if self.time == 0:
            period = self.period
            self.__dict__.update(kline.__dict__)
            self.period = period
        elif self.time < kline.time:
            self.time = kline.time
            if self.high < kline.high: self.high = kline.high
            if self.low > kline.low: self.low = kline.low
            self.close = kline.close
            self.volume += kline.volume

    def loadklines(codeL: str, limit=60000, s_time="", e_time="", period="1m"):  # 通过接口获取k线数据，增加基础计算
        klines: List[KLine] = []
        tf = int(period[:-1]) if period[-1] == "m" else 1440  # 只支持1分钟和日线

        if e_time == "": e_time = TimeProp().trade_time
        etimestamp = datetime.strptime(e_time, '%Y-%m-%d %H:%M:%S').timestamp()
        if s_time != "":
            stimestamp = datetime.strptime(s_time, '%Y-%m-%d %H:%M:%S').timestamp()
            if etimestamp - stimestamp < 10: return klines
        e_time = TimeProp(int(etimestamp) + 30).trade_time
        s_time_str = f"and `datetime`>'{s_time}'" if s_time != "" else ""
        sql: str = ""
        database = "qh_index_db"
        if codeL[-2:] == "L6":  # 数据库获取
            sql = f"SELECT *,UNIX_TIMESTAMP(datetime) as time FROM  tbl_code_{codeL} where period={tf} {s_time_str} and `datetime`<='{e_time}' order by `datetime` desc limit {limit}"
        elif codeL[-2:].isdigit():  # 后面俩数字是合约
            database = "kline"
            breed = codeL.rstrip(string.digits)
            sql = f"SELECT *,UNIX_TIMESTAMP(datetime) as time FROM  tbl_code_{breed}_all where period={tf} and code='{codeL}' {s_time_str} and `datetime`<='{e_time}' order by `datetime` desc limit {limit}"
        elif codeL[-2:] != "L9":  # 数据库获取 板块
            sql = f"SELECT *,UNIX_TIMESTAMP(datetime) as time FROM  tbl_bk_{codeL} where period={tf} {s_time_str} and `datetime`<='{e_time}' order by `datetime` desc limit {limit}"
        else:  # L9
            database = "hqdb"
            breed = codeL[:-2]
            s_time_str = f"and trading_day>={TimeProp(stimestamp).intTradeDay}" if s_time != "" else ""
            sql = f"SELECT *,UNIX_TIMESTAMP(STR_TO_DATE(CONVERT(trading_day*10000+1500,char),'%Y%m%d%H%i')) as `time` FROM tbl_future_all_kline_day where breed='{breed}' {s_time_str} and trading_day<={TimeProp(int(etimestamp) + 30).intTradeDay} ORDER BY trading_day desc"

        if sql:
            db = pymysql.connect(user="root", password="2234567", host=cfg()["dbIP"], database=database,
                                 cursorclass=pymysql.cursors.DictCursor)
            cursor = db.cursor()
            cursor.execute(sql)
            result = list(cursor.fetchall())
            db.close()
            result.reverse()
            for d in result:
                kline = KLine(d['time'], float(d['open']), float(d['high']), float(d['low']), float(d['close']),
                              int(d['volume']), int(d['close_oi']), d['code'])
                # kline.__Dict__.update(d)
                kline.period = tf
                klines.append(kline)

        if len(klines) > 0: logger.info(
            f"Load [{klines[0].trade_time}]->[{klines[-1].trade_time}] {len(klines)} {period} klines from {codeL}")
        return klines


# 获取所有账号信息，每分钟刷新一次
def getallacconts():
    allacconts: Dict[str, Dict] = {}
    url = cfg().get("all_acconts_url")
    if url:
        headers = {'Content-Type': 'application/json;charset=UTF-8', 'Connection': 'keep-alive'}
        r = requests.request('GET', url + '/api/mgr/allaccounts', headers=headers)
        if r.status_code == 200: allacconts: Dict[str, Dict] = r.json()["result"]
    else:
        allacconts: Dict[str, Dict] = cfg().get("all_acconts", {})
    return allacconts


class TimeLineClass:
    CurTime: str = "16:36:31"
    TradingDay = 20230321
    UpdateTime: str = "15:16:34"
    PreSettlementPrice = 5167.000
    LastPrice = 5178.000
    BidPrice1 = 5178.000
    BidVolume1 = 7
    AskPrice1 = 5181.000
    AskVolume1 = 1
    SettlementPrice = 5170.000
    PreClose = 5194.000
    open = 5161.000
    high = 5218.000
    low = 5123.000
    close = 5178.000
    volume = 12755
    amount = 989250060.000
    OpenInterest = 24531.0
    PreOpenInterest = 24733.0
    UpperLimitPrice = 5683.000
    LowerLimitPrice = 4650.000
    MinDgDay = 20230321
    MaxDgDay = 20230321

    def __init__(self, rds, symbol) -> None:
        if symbol == '': return logger.error(f"无合约参数")
        str_data = rds.hget('future_realtime', symbol)
        if not str_data: return logger.error(f"{symbol}:无合约数据")
        str_data = re.sub("(\w+)=", '"\g<1>"=', str_data)
        str_data = re.sub("(\d+:\d+:\d+)", '"\g<1>"', str_data)
        self.__dict__.update(json.loads("{" + str_data.replace("=", ":") + "}"))
