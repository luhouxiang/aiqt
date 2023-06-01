# -*- coding: utf-8 -*-
"""
@author: luhx
@file: baseobj.py
@time: 2023/5/30 22:11
@desc:
"""
from datetime import datetime
import time
from typing import Dict, List, Tuple
import requests
import re
import pymysql
from pymysql.cursors import DictCursor
import string
import json
import redis
from kds_util.user_logbook import user_log as logger

from base.cfg import cfg


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


class KlineList(TimeProp):
    def __init__(self, tf: int = 1) -> None:
        self.time = 0
        self.klines: List[KLine] = []
        self._id = 0
        self.tf = tf
        self.tid = 0
        self.newkline = KLine()

    def input(self, kline1m: KLine, intCloseTime: int):
        self.tid += 1
        if kline1m.intTradeTime == intCloseTime: self.tid = self.tf  # 收盘时间
        if kline1m.time - self.newkline.time > 200 and self.tf - self.tid < 4:  # 时间有较大跨度# 合并快完成，说明有丢失k线
            self.newkline.time = (self.newkline.time + 299) // 300 * 300  # 往后对齐到5分钟边界
            self.klines.append(self.newkline)
            self.newkline = KLine()
            self.tid = 1
        self.newkline.appendkline(kline1m)
        if self.tid == self.tf:
            self.klines.append(self.newkline)
            self.newkline = KLine()
            self.tid = 0

    @property
    def last_kline(self) -> KLine:
        if not self.klines: return KLine()
        if self._id >= len(self.klines):
            self._id = len(self.klines)
            return self.prev_kline()
        kline = self.klines[self._id]
        self.time = kline.time
        return kline

    def move_next(self, tm: int = 0):
        if self.last_time == tm or tm == 0: self._id += 1

    @property
    def last_time(self) -> int:
        return self.last_kline.time

    @property
    def id(self) -> int:
        if self._id >= len(self.klines): return len(self.klines) - 1
        return self._id

    def have_kline(self, tm) -> int:
        if self._id < len(self.klines) and self.last_time <= tm: return self.last_time
        return 0

    def last_n_klines(self, N) -> List[KLine]:
        return self.klines[:self._id + 1][-N:]

    def prev_kline(self, N=-1) -> KLine:  # 上一根k线
        if self.klines and self._id + N >= 0: return self.klines[self._id + N]
        return self.last_kline




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
                             cursorclass=DictCursor)
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


class Order():
    def __init__(self, instrument_id="", effect_type='0', posi_direction='2', price=0.0, volume=1, trade_time=0,
                 price_type="high_low_price", condition_type="1") -> None:
        self.price_type: str = price_type  # "high_low_price":最高或最低价成交,此种类型，不需要"price"参数，"limit"：限价成交,同时需要提供price参数
        self.condition_type: str = condition_type  # "1": 立即完成，否则撤销（指令单), "3": 当日有效, 可选项，若不存在或不为"1"表示为当日有效的指令单
        self.effect_type: str = effect_type  # "0": 开仓， "1": 平仓
        self.posi_direction: str = posi_direction  # "2": 多头， "3":空头
        self.open_price: float = price
        self.volume: int = volume
        self.instrument_id: str = instrument_id
        self.open_time = trade_time if trade_time != 0 else TimeProp().time


# 合约/指数/板块
class Contract(TimeProp):
    def __init__(self, acc, codeL="", code="") -> None:
        # self.investor_id="moni156"
        self.time = 0
        self.codeL: str = codeL  # agL6,agL9,guijinshu
        self.breed: str = ""  # ag,ag,""
        self.typ: str = ""  # "L9","L6","BK"
        self.code: str = code  # OI209,"",""
        self.ExchangeId: str = ""  # CZCE
        self.InstrumentName: str = ""  # 白银2012
        self.Ma20Turnover: float = 0
        self.OpenRatioByVolume: float = 0
        self.CloseTodayRatioByVolume: float = 0
        self.CloseRatioByVolume: float = 0
        self.OpenRatioByMoney: float = 0.0013
        self.CloseTodayRatioByMoney: float = 0.0003
        self.CloseRatioByMoney: float = 0.0003
        self.MarginRatioByMoney: float = 1
        self.Multiplier: float = 100
        self.Minunit: float = 0.01
        self.klines1m: KlineList = KlineList()  # 分钟k线
        self.klines5m: KlineList = KlineList(5)  # 5分钟k线
        self.klines15m: KlineList = KlineList(15)  # 15分钟k线
        self.klines60m: KlineList = KlineList(60)  # 60分钟k线
        self.klines120m: KlineList = KlineList(120)  # 120分钟k线
        self.klines1d: KlineList = KlineList(1440)  # 日线
        self.orders: List[Order] = []  # 本合约开单, 建议L9数据才开仓
        self.Disable: bool = False  # True:盘中不刷新1分钟数据. 需要刷新,需要置成False
        self.signal = 0
        self.period = 0
        self.acc = acc

        if len(codeL) < 3: return
        self.codeL = codeL
        self.typ = codeL[-2:]
        if self.typ[-2] == "L":  # 指数,不需要合约交易数据
            self.breed = codeL[:-2]
            if self.typ == "L9":  # 主连, 读取对应合约交易数据
                r = requests.request('GET',
                                     "%s%s?investor_id=%s&code=%s" % (acc.url, '/accountcode', acc.investor_id, code),
                                     headers={'Content-Type': 'application/json;charset=UTF-8'})
                if r.status_code == 200: self.__dict__.update(r.json()["result"])
        else:
            self.typ = "BK"

    def load_day_klines(self, stime: str = "", etime: str = "") -> int:
        self.klines1d.klines = loadklines(self.codeL, 1000, stime, etime, '1d')  # 读入日线数据,固定最多500日
        return len(self.klines1d.klines)

    # 跑日线之前，调用
    def load_minute_klines(self, stime="", etime="") -> int:
        if self.typ == "BK" or self.typ == "L6":
            self.code = self.codeL
        try:
            klines = loadklines(self.code, 1000, stime, etime, '1m')  # 读入分钟线数据
            for k in klines:  # 合并k线
                for kx in (self.klines5m, self.klines15m, self.klines60m, self.klines120m):
                    kx.input(k, self.intCloseTime)
            if etime:
                self.klines1m.klines = klines
                self.klines1m._id = 0
            else:
                self.klines1m.klines += klines
            return len(klines)
        except:
            pass
        return 0

    @property
    def intCloseTime(self):
        return 151500 if self.breed in ["T", "TS", "TF"] else 150000

    @property
    def Last_Day_Kline(self):
        return self.klines1d.last_kline

    @property
    def Last_minute_Kline(self):
        if self.klines1m.last_kline.time: return self.klines1m.last_kline
        return self.klines1d.last_kline

    @property
    def LastPrice(self):
        return self.Last_minute_Kline.close

    def calorder(self, o, price, v) -> Tuple[float, float, float]:  # 假设price价格平仓v手,计算盈亏
        d: int = 1 if o.posi_direction == "2" else -1
        Margin: float = v * price * self.Multiplier * self.MarginRatioByMoney  # 保证金
        Earn: float = v * d * (price - o.open_price) * self.Multiplier  # 浮动盈亏
        Fee: float = v * (
                    o.open_price * self.Multiplier * self.CloseTodayRatioByMoney + self.CloseTodayRatioByVolume)  # 平仓手续费
        return Margin, Earn, Fee

    @property
    def long_position(self):  #
        pos = 0
        for o in self.orders:
            if o.posi_direction == "2" and o.instrument_id == self.code: pos += o.volume
        return pos

    @property
    def short_position(self):  #
        pos = 0
        for o in self.orders:
            if o.posi_direction == "3" and o.instrument_id == self.code: pos += o.volume
        return pos

    @property
    def net_position(self):  # 关联持仓
        return self.long_position - self.short_position

    @property
    def one_margin(self):
        return self.LastPrice * self.Multiplier * self.MarginRatioByMoney


class Hold():
    def __init__(self, hold: Dict) -> None:
        self.InstrumentID = ""
        self.Direction = ""
        self.Position = 0
        self.UseMargin = 0.0
        self.PositionProfit = 0.0
        self.PositionProfitByTrade = 0.0
        self.CloseProfit = 0.0
        self.Commission = 0.0
        self.open_price = 0.0
        self.LastPrice = 0.0
        self.__dict__.update(hold)

    def refresh_profit(self, m: Contract, price, close_price):
        d = 1 if self.Direction == "2" else -1
        self.UseMargin = self.Position * price * m.Multiplier * m.MarginRatioByMoney
        self.PositionProfit = self.Position * d * (close_price - self.open_price) * m.Multiplier
        self.PositionProfitByTrade = self.Position * d * (price - self.open_price) * m.Multiplier
        self.LastPrice = price


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


# 通过接口获取一个账户信息
class Accont(TimeProp):
    def __init__(self, investor_id: str = ""):
        self.initEquity: float = 1e7  # 初始权益
        self.investor_id: str = investor_id
        self.url: str = ""
        self.ip: str = ""
        self.Contracts: List[str] = ['']
        self.Codes: List[str] = ['']
        self.dtqy: float = 0.0  # http动态权益  未结算权益+浮动盈亏
        self.sxf: float = 0.0  # http手续费    已发生手续费
        self.kyzj: float = 0.0  # http可用资金  动态权益-保证金
        self.time = time.time()
        # self.rds: redis.Redis = None
        self.Holds: Dict[str, Hold] = {}

        if investor_id == "": investor_id = cfg()['investor_id']
        acc: Dict = getallacconts().get(investor_id, {})
        if acc:
            self.__dict__.update(acc)
            ip = re.search(r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])',
                           self.url)
            if not ip: return logger.error(f"Access {self.url} Error!")

            self.ip = ip.group()
            self.rds = redis.Redis(self.ip, decode_responses=True)
            try:
                r = requests.request('GET', f"{self.url}/oneaccountinfo?investor_id={self.investor_id}",
                                     headers=cfg()["headers"], timeout=2)
            except:
                logger.critical(f"Access {self.url}/oneaccountinfo?investor_id={self.investor_id} Error!");
                raise Exception(f"Access {self.url}/oneaccountinfo?investor_id={self.investor_id} Error!");

            self.updatetime = time.time()
            if r.status_code == 200: self.__dict__.update(r.json()["result"])  # 通过接口获取一个账户信息

    def qhaccountdata(self, tradingday=0):
        if tradingday:
            db = pymysql.connect(user="root", password="2234567", host=self.ip, database="hqdb",
                                 cursorclass=pymysql.cursors.DictCursor)
            cursor = db.cursor()
            cursor.execute(
                f"SELECT CurBalance,Available,Commission from tbl_future_trading_account WHERE TradingDay={tradingday};")
            result = cursor.fetchall()
            cursor.close()
            if not result: return
            self.dtqy = result[0]["CurBalance"]
            self.initEquity = self.dtqy
            self.kyzj = result[0]["Available"]
            self.sxf = result[0]["Commission"]
        else:
            for i in range(3):
                try:
                    r = requests.request('POST', self.url + "/qhaccountdata",
                                         json={"account_id": self.investor_id, "token": "123456"},
                                         headers=cfg()["headers"], timeout=1)
                    if r.status_code == 200:
                        self.dtqy = r.json()["dtqy"]
                        self.initEquity = self.dtqy
                        self.sxf = r.json()["sxf"]
                        self.kyzj = r.json()["kyzj"]
                        return
                except:
                    logger.critical(self.url + "/qhaccountdata" + " request error!")
                time.sleep(3)
            logger.critical(self.url + "/qhaccountdata" + " request error 3 times!")
            raise Exception(self.url + "/qhaccountdata" + " request error 3 times!")

    def qhholdlist(self, tradingday=0):  # 还原某天持仓,缺省当天
        self.qhaccountdata(tradingday)
        if tradingday == 0: tradingday = TimeProp().intTradeDay
        db = pymysql.connect(user="root", password="2234567", host=self.ip, database="hqdb",
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = db.cursor()
        cursor.execute(f"SELECT * from tbl_future_position_general WHERE TradingDay={tradingday};")
        result = list(cursor.fetchall())
        cursor.close()

        self.Holds.clear()
        for row in result:
            open_price = float(row["OpenAmount"]) / float(row["OpenVolume"]) / float(row["VolumeMultiple"]) if float(
                row["OpenVolume"]) else float(row["LastPrice"])
            self.Holds[f'{row["InstrumentID"]}.{row["Direction"]}'] = Hold({
                "InstrumentID": row["InstrumentID"],
                "Direction": row["Direction"],
                "UseMargin": float(row["UseMargin"]),
                "Position": int(row["Position"]),
                "PositionProfit": float(row["PositionProfit"]),
                "open_price": float(open_price),
                "Commission": float(row["Commission"]),
                "PositionProfitByTrade": float(row["PositionProfitByTrade"]),
                "CloseProfit": float(row["CloseProfit"]),
                "LastPrice": float(row["LastPrice"])})

    def TimeLine(self, symbol: str) -> TimeLineClass:
        return TimeLineClass(self.rds, symbol)
