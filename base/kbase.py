# -*- coding: utf-8 -*-
"""
@author: luhx
@file: kbase.py
@time: 2023/5/30 7:43
@desc:
"""
from datetime import datetime
import json
import time
from typing import Dict, List, Tuple
import pymysql
import pymysql.cursors
import string
import requests
import copy
import os
import sys

from base.baseobj import TimeProp
from kds_util.user_logbook import system_log as logger
from kds_util.user_logbook import user_log as user_logger, init_logger as init_logs
from base.cfg import cfg
from base.baseobj import Contract, Order, Accont, Hold, loadklines, KLine


def sign(a): return 0 if not a else 1 if a > 0 else -1


def avlist(l): return (sum(l) / len(l)) if l else 0


def GetBkTable() -> Tuple[Dict[str, str]]:
    db = pymysql.connect(user="root", password="2234567", host=cfg()["dbIP"], database="qh_index_db",
                         cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()
    cursor.execute(
        "SELECT BkCode,BkName,ContractWeightCode,ContractWeightName,BkType,Filter FROM  tbl_component_bk")  # where `Filter`='要'
    result: Tuple[Dict[str, Dict[str, str]]] = cursor.fetchall()
    db.close()
    return result


class kBase(Accont):
    def __init__(self, investor_id="", days=100, mindays=1, stime="", etime="", codes=[], playback=False,
                 turnover_threshod=1e8):  #
        # stime 开始时间，对齐到交易日,空表示当前时间,用于实盘/模拟盘,非空用于回放
        # etime 结束时间
        # days stime之前的日线数量
        # mindays stime之前的分钟线天数
        super().__init__(investor_id)
        self.allContract: Dict[str, Contract] = {}  # 所有个票数据，key是品种+L9/L6/板块，value是Class Contract
        self.Breed2Symbol: Dict[str, str] = {}  # 所有品种数据，key是品种，value是主力合约。
        self.bkinfo: Dict[str, List[str]] = {}  # key是板块名，value是板块关联品种列表
        self.RealCount: int = 0  # 实盘计数器,模拟状态为0,实盘每过一分钟加一
        self.playback = playback  # 是否是回放 True:回放 False:实盘 默认是实盘
        self.pre_dtqy = 0
        self.turnover_threshod = turnover_threshod
        self.etime = int(datetime.strptime(etime, '%Y-%m-%d %H:%M:%S').timestamp()) if etime != "" else time.time()
        self.main_contract_info = {}
        self.data_center_ip = cfg()["dbIP"]
        self.data_center_url = cfg().get("all_acconts_url")

        if mindays >= days: logger.critical(f"mindays={mindays} 不能大于 days={days}")
        if mindays <= 0: mindays = 1  # 保护传入分钟数为0的情况

        if etime == "": etime = TimeProp().trade_time

        wenhuashangpin = loadklines("wenhuashangpin", days, "", stime, "1d")
        self.stime = wenhuashangpin[-1].time
        sdate = wenhuashangpin[0].trade_time
        self.minute_start_time = wenhuashangpin[-mindays].time + 3600  # 分钟回放时间

        user_logger.info(
            f"初始化：investor_id={investor_id} days={days} mindays={mindays} stime={stime} etime={etime} codes={codes} playback={playback} minute_start_time={TimeProp(self.minute_start_time).trade_time}")

        self.load_main_contract_tbl(sdate, etime)  # 获取主力合约次主力合约信息，并分配日k线

        # 个票
        for i in range(len(self.Contracts)):
            breed = self.Codes[i]
            if codes and breed not in codes: continue
            self.Breed2Symbol[breed] = self.Contracts[i]
            self.allContract[breed + "L9"] = Contract(self, breed + "L9", self.Contracts[i])
            self.allContract[breed + "L6"] = Contract(self, breed + "L6", "")
            self.allContract[breed + "L7"] = Contract(self, breed + "L7", "")
            self.allContract[breed + "L8"] = Contract(self, breed + "L8", "")
            if self.allContract[breed + "L9"].Ma20Turnover > self.turnover_threshod:
                self.allContract[breed + "L9"].load_day_klines(sdate, etime)
                self.distrabute_kline(self.allContract[breed + "L9"])
                self.allContract[breed + "L6"].load_day_klines(sdate, etime)

        # 板块
        self.BkTable: Tuple[Dict[str, str]] = GetBkTable()
        for row in self.BkTable:
            bk = row["BkCode"]
            if codes and bk not in codes: continue
            breed: str = row["ContractWeightCode"].replace("L6", "")
            if not self.allContract.get(bk):  # 首次出现板块
                self.bkinfo[bk] = []
                self.allContract[bk] = Contract(self, bk, "", )  # 调入板块数据
                self.allContract[bk].load_day_klines(sdate, etime)
            if self.Breed2Symbol.get(breed) and row['Filter'] == "要":  # 有品种数据
                self.bkinfo[bk].append(breed)  # 生成板块查询个票的信息

        if not self.playback:
            self.GetAccontInfo()  # 按照芦总要求，跑之前调入实盘资金和持仓情况

        user_logger.info(f"初始化完成：investor_id={investor_id} initEquity={self.initEquity}")

    def L7(self, code: str) -> Contract:
        return self.allContract.get(code.replace("L6", "").rstrip(string.digits) + "L7", Contract(self))

    def L8(self, code: str) -> Contract:
        return self.allContract.get(code.replace("L6", "").rstrip(string.digits) + "L8", Contract(self))

    def L9(self, code: str) -> Contract:
        return self.allContract.get(code.replace("L6", "").rstrip(string.digits) + "L9", Contract(self))

    def LL(self, code: str) -> Contract:  # 根据合约号查询对应品种的对象
        L9 = self.L9(code)
        L7 = self.L7(code)
        L8 = self.L8(code)
        if L9.code == code: return L9
        if L7.code == code: return L7
        if L8.code == code: return L8

    def GetAccontInfo(self, tradingday=0):  # 实盘查询仓位, 主要在子线程调用,增加回调处理
        self.qhholdlist(tradingday)
        # 还原订单
        for m in self.allContract.values():
            m.orders.clear()
        # holds = copy.deepcopy(self.Holds)
        for hold in self.Holds.values():
            jc = hold.Position
            m = self.LL(hold.InstrumentID)
            if m:  # 反向还原订单
                if jc:
                    m.orders.append(Order(hold.InstrumentID, '0', hold.Direction, float(hold.open_price),
                                          jc))  # 根据持仓,补充订单,全部按照今仓? 不影响算法
            else:
                logger.warning(f"{hold.InstrumentID}在{self.url}上面有持仓,不在列表中,可能是非主力合约,需要手动处理")

    def load_main_contract_tbl(self, stime: str = "1970-1-1", etime: str = ""):
        db = pymysql.connect(user="root", password="2234567", host=cfg()["dbIP"], database="hqdb",
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = db.cursor()
        cursor.execute(f"select * from tbl_factor_main_contract where ktime>='{stime}' and ktime<='{etime}'")
        data = cursor.fetchall()
        for row in data:
            self.main_contract_info[TimeProp(row["ktime"].timestamp()).intTradeDay] = json.loads(row["info"])
        db.close()

    def distrabute_kline(self, m):
        allk = m.klines1d.klines
        m.klines1d.klines = []
        pinfo = {}
        if m.typ == "L9":
            for k in allk:
                info = pinfo = self.main_contract_info.get(k.intTradeDay, pinfo)
                if not info: continue
                if k.instrument == info[m.breed][0]: m.klines1d.klines.append(k)
                if k.instrument == info[m.breed][1]: self.L7(m.breed).klines1d.klines.append(k)
                if k.instrument == info[m.breed][2]: self.L8(m.breed).klines1d.klines.append(k)

    def orderbuysell(self, o=Order()):  # 实盘模拟盘下单
        logger.info(
            f"http:{self.investor_id}:{o.instrument_id} {o.open_price:8.3f} vol:{o.volume:3.0f} dir:{o.posi_direction} {o.effect_type}")
        url: str = self.url.replace("8088", "8084").replace("mgr", "virtual") + '/orderbuysell'
        param: Dict[str, str] = {
            "admin_id": "100000",
            "token": "jiang",
            "user": "jiang",
            "account_id": self.investor_id,
            "account_type": "virtual",
        }
        param.update(o.__dict__)
        try:
            requests.request('POST', url, json=param,
                             headers={'Content-Type': 'application/json;charset=UTF-8', 'Connection': 'keep-alive'})
        except:
            logger.error(f"下单失败 {o.instrument_id}")

    def LastPrice(self, instrument_id):
        LL: Contract = self.LL(instrument_id)
        if LL: return LL.LastPrice
        return self.L9(instrument_id).LastPrice

    def simorderbuysell(self, o=Order()):  # 模拟下单
        m: Contract = self.L9(o.instrument_id)  # 只在L9下单,因为L9有合约信息
        if not m: logger.info(f"Order error:{o.instrument_id}");return

        hold = self.Holds.get(f"{o.instrument_id}.{o.posi_direction}")
        if not hold: hold = self.Holds[f"{o.instrument_id}.{o.posi_direction}"] = Hold(
            {"InstrumentID": o.instrument_id, "Direction": o.posi_direction})

        D: Dict[str, str] = {"0": "开", "1": "平", "2": "多", "3": "空", }
        o.volume = int(o.volume)
        o.open_price = self.LastPrice(o.instrument_id)  # 这里强制用最新价格下单，不做撮合
        logger.info(
            f"Sim:{D[o.posi_direction]}{D[o.effect_type]}{o.volume:3.0f}手 {o.open_price:8.3f} {o.instrument_id}")  # 模拟交易日志
        if o.open_price == 0:
            user_logger.error(f"{m.codeL} {o.instrument_id} error![zero price] 下单失败")
            return
        if o.effect_type == "0":  # 开仓
            hold.Commission += o.volume * (
                    o.open_price * m.Multiplier * m.OpenRatioByMoney + m.OpenRatioByVolume)  # 手续费
            m.orders.append(o)
            hold.open_price = (hold.open_price * hold.Position + o.volume * o.open_price) / (
                    hold.Position + o.volume)  # 计算持仓均价
            hold.Position += o.volume
        else:  # 平仓
            for o1 in m.orders:
                # if o1.instrument_id != m.Last_minute_Kline.instrument:
                #     logger.error(f"Sim: Close different instrument. Open:{o1.instrument_id} Close{m.Last_minute_Kline.instrument}") # 模拟交易日志
                # continue
                if o1.instrument_id != o.instrument_id or o1.posi_direction != o.posi_direction: continue
                v: int = min(o1.volume, o.volume)  # 可平量
                o.volume -= v
                o1.volume -= v
                hold.open_price = (hold.open_price * hold.Position - v * o1.open_price) / (
                        hold.Position - v) if hold.Position - v > 0 else o.open_price  # 计算持仓均价
                mr, e, f = m.calorder(o1, o.open_price, v)
                hold.Position -= v
                hold.CloseProfit += e
                hold.Commission += f
                if o.volume <= 0: break
            m.orders = [x for x in m.orders if x.volume != 0]  # 清理掉平完仓的订单

    def doOrder(self, o):
        if self.time < self.stime: return  # stime之前,不处理订单
        self.simorderbuysell(copy.deepcopy(o))
        if not self.playback: self.orderbuysell(o)  # 实盘下单

    # %%主要策略代码，每分钟调用一次
    def run_minute_kline(self, m: Contract):
        pass

    def run_5_minute_kline(self, m: Contract):
        pass

    def run_15_minute_kline(self, m: Contract):
        pass

    def run_60_minute_kline(self, m: Contract):
        pass

    def run_120_minute_kline(self, m: Contract):
        pass

    def run_day_kline(self, m: Contract):
        pass

    def run_minute_kline2(self):
        pass

    def run_day_kline2(self):
        pass

    def refresh_profit(self):
        self.dtqy = self.initEquity  # 动态权益
        Margin: float = 0.0  # 保证金
        self.sxf = 0.0
        for h in self.Holds.values():  # 统计所有票的保证金/盈亏/手续费
            L9 = self.L9(h.InstrumentID)
            LL = self.LL(h.InstrumentID)
            if not L9.code or not LL: continue  # 找不到L9，或者LL 都不计算
            h.refresh_profit(L9, LL.LastPrice, LL.klines1d.last_kline.close)  # 刷新收益和日内收益
            Margin += h.UseMargin
            self.dtqy += h.CloseProfit
            self.dtqy += h.PositionProfitByTrade
            self.sxf += h.Commission
        self.dtqy -= self.sxf  # 动态权益扣除手续费
        self.kyzj = self.dtqy - Margin  # 可用资金

    def save_networth2db(self):
        if self.pre_dtqy == 0: self.pre_dtqy = self.initEquity
        if self.pre_dtqy == self.dtqy: return
        user_logger.info(f"{self.investor_id}:{self.intTradeDay}:动态权益{self.dtqy:.2f} 可用资金{self.kyzj:.2f}")
        db = pymysql.connect(user="root", password="2234567", host=self.ip, database="hqdb",
                             cursorclass=pymysql.cursors.DictCursor)
        db.query(f"""replace into tbl_investor_networth(InvestorID,trading_day,original_networth,yes_networth,cur_networth,income_rate,available_cash,`datetime`)
                values('{self.investor_id}',{self.intTradeDay},{self.initEquity},{self.pre_dtqy},{self.dtqy},{self.dtqy / self.initEquity - 1},{self.kyzj},now())""")
        db.commit()
        db.close()
        self.pre_dtqy = self.dtqy

    def log_hold_list(self):
        strhold = ""
        for hold in self.Holds.values():
            if hold.Position == 0: continue
            strhold += f"[{hold.InstrumentID}:{hold.Position}]"
        if strhold: user_logger.info(strhold)

    def runallklines(self):
        self.time = 0
        pdtm = 0
        user_logger.info(f"开始回放：investor_id={self.investor_id}")
        # try:
        while 1:  # 日线循环
            dtm = int(time.time()) + 900
            for m in self.allContract.values():  # 找最老的分钟k线
                m.code = m.Last_Day_Kline.instrument
                if m.klines1d.have_kline(dtm + 3600):  # 在dtm前面有没有k线
                    dtm: int = m.klines1d.last_time
            if dtm > time.time() and self.playback:  # 日线跑完了
                return
            if dtm > self.minute_start_time:  # 调入一天分钟线，并运行
                self.log_hold_list()
                for m in self.allContract.values():  # loadklines
                    L9 = self.allContract.get(m.breed + "L9")
                    if L9 and L9.Ma20Turnover < self.turnover_threshod: continue
                    m.load_minute_klines(TimeProp(pdtm).trade_time, TimeProp(dtm + 3600).trade_time)

                while 1:
                    tm = int(time.time()) + 300  # 初始化一个5分钟后的时间
                    for m in self.allContract.values():  # 找最老的分钟k线
                        if m.klines1m.have_kline(tm):
                            tm: int = m.klines1m.last_time

                    if tm < time.time():  # 有分钟线，就跑
                        self.time = tm  # 新的时刻来临，对齐所有k线
                        for m in self.allContract.values():
                            m.time = tm
                            if m.klines1m.last_time == tm: self.run_minute_kline(m)
                            if m.klines5m.last_time == tm: self.run_5_minute_kline(m)
                            if m.klines15m.last_time == tm: self.run_15_minute_kline(m)
                            if m.klines60m.last_time == tm: self.run_60_minute_kline(m)
                            if m.klines120m.last_time == tm: self.run_120_minute_kline(m)
                        self.run_minute_kline2()  # 对齐后，全部调用一次分钟线
                        if self.RealCount == 0: self.refresh_profit()  # 回放分支 动态计算,实盘修正
                        for m in self.allContract.values():
                            m.klines1m.move_next(tm)
                            m.klines5m.move_next(tm)
                            m.klines15m.move_next(tm)
                            m.klines60m.move_next(tm)
                            m.klines120m.move_next(tm)

                        Margin = self.dtqy - self.kyzj  # 保证金计算
                        logger.info(
                            f"{self.trade_time}({self.intTradeDay}) 动态权益:{self.dtqy / 10000:.2f}万 保证金:{Margin / 10000:.2f}万({100 * Margin / self.dtqy:.2f}%) 可用资金:{self.kyzj / 10000:.2f}万({100 * self.kyzj / self.dtqy:.2f}%)")
                    else:  # 说明k线走完,进入等待k线
                        if self.intTradeDay < TimeProp().intTradeDay: break  # 回放没有到达今日，退出
                        if self.playback: return
                        self.real_refresh()

            dtm = datetime.strptime(str(TimeProp(dtm).intTradeDay * 10000 + 1600), '%Y%m%d%H%M').timestamp()
            pdtm = dtm
            self.time = dtm
            for m in self.allContract.values():
                m.time = self.time
                if m.klines1d.intTradeDay == self.intTradeDay:  # 15:00 15:15 日线同时跑
                    self.run_day_kline(m)
            self.run_day_kline2()  # 对齐后，全部调用一次日线
            if self.RealCount == 0: self.refresh_profit()  # 回放分支 动态计算,需要修正
            if self.playback: self.save_networth2db()

            for m in self.allContract.values():
                if m.klines1d.intTradeDay == self.intTradeDay:  # 15:00 15:15 日线同时跑
                    m.klines1d.move_next()
            Margin = self.dtqy - self.kyzj  # 保证金计算
            logger.info(
                f"{self.trade_time}({self.intTradeDay}) 动态权益:{self.dtqy / 10000:.2f}万 保证金:{Margin / 10000:.2f}万({100 * Margin / self.dtqy:.2f}%) 可用资金:{self.kyzj / 10000:.2f}万({100 * self.kyzj / self.dtqy:.2f}%)")
        # except Exception:
        #     user_logger.critical(traceback.format_exc())

    def real_refresh(self):
        tm = int((time.time() + 59) / 60) * 60
        if self.RealCount:
            logger.info(f"sleep {tm + 5 - time.time()}s")
            time.sleep(tm + 5 - time.time())  # 再次进入实盘,等到每分钟的05秒
        else:  # 首次进入实盘,不等待
            user_logger.info(f"回放结束,开始实盘/模拟盘[{self.investor_id}]时间 {self.trade_time}\n")

        ccount: int = 0
        kcount: int = 0
        codes: str = ""
        for m in self.allContract.values():
            if m.Disable: continue  # 跳过Disable的票
            if m.klines1m.klines == []: continue
            kls = m.load_minute_klines(m.trade_time)
            if kls:
                codes += " " + m.codeL
                kcount += kls
                ccount += 1
        logger.info(f"load {ccount} codes all {kcount} 1m klines [{codes}]")
        self.RealCount += 1
        self.GetAccontInfo()  # 获取实盘信息,然后跑下一分钟


def main_kbase():
    xj = xjclass("", 100, 1, "2023-4-21 14:00:00", "", codes=["i", "wenhuashangpin"], playback=False)  # ,"au"
    xj.runallklines()


# 以下部分是示例
if __name__ == '__main__':
    from kds_util.user_logbook import user_log as logger

    os.chdir(os.path.abspath(os.path.dirname(sys.argv[0])))
    init_logs(path="./log", name="MoniHighLow", level="INFO")


    # demo
    class Param():
        def __init__(self) -> None:
            self.score = 0.0
            self.dkline = KLine()  # 当日合成的日线，动态的
            self.count1m = 0
            self.turnover = 0.0
            self.volume = 0
            self.datr5 = 0.0
            self.dma5 = 0.0
            self.ddir = 0
            self.Select = 0
            self.dir = 0
            self.kline = KLine()  # 当前分钟线
            self.kline30 = KLine()  # 当前30分钟线
            self.mav = 0.0


    class xjclass(kBase):
        def __init__(self, investor_id="", days=150, mindays=1, stime="", etime="", codes=[], playback=False):
            super().__init__(investor_id, days, mindays, stime, etime, codes, playback)
            self.Params: Dict[str, Param] = {}
            for key in self.allContract: self.Params[key] = Param()

        def run_day_kline(self, m: Contract):  # 每个日线串行回调
            k = m.klines1d.last_kline
            p = self.Params[m.codeL]
            p.turnover = 0
            p.volume = 0
            p.datr5 = (k.high - k.low + p.datr5 * 2) / 3 if p.datr5 else k.high - k.low  # 5日波幅
            p.score = p.datr5 / k.close / m.MarginRatioByMoney if m.MarginRatioByMoney else 0
            p.dma5 = avlist([x.close for x in m.klines1d.last_n_klines(5)])  # 5日均
            p.ddir = 1 if k.close > p.dma5 else -1
            p.Select = 0
            p.dir = 0
            p.count1m = 0

        def run_day_kline2(self):  # 日线跑完，全部回调  {筛票阶段}
            for bk in self.bkinfo:
                for breed in self.bkinfo[bk]: self.Params[breed + "L6"].Select = 1  # 先全部选中，后面启用黑名单

            for bk in self.bkinfo:
                if self.Params[bk].ddir != self.Params["wenhuashangpin"].ddir:
                    for breed in self.bkinfo[bk]:
                        if not self.Params.get(breed + "L6"): continue
                        self.Params[breed + "L6"].Select = 0  # 板块和文华指数不一致,屏蔽整个板块(拉黑)

            for bk in self.bkinfo:
                if self.Params[bk].ddir == self.Params["wenhuashangpin"].ddir:
                    scores = sorted([(breed, self.Params[breed + "L6"].score) for breed in self.bkinfo[bk] if
                                     self.Params[breed + "L6"].Select], key=lambda x: -x[1])
                    i = 0
                    for s in scores:
                        self.Params[s[0] + "L6"].Select = 0 if i else 1  # 在没有拉黑的票中，在板块中选择杠杆最高的，获利能力最好的一个
                        i += 1

        def run30kline(self, m: Contract, k: KLine):  # 分钟线回调，计算个票{信号阶段}
            pass

        def run_minute_kline(self, m: Contract):  # 分钟线回调，计算个票{信号阶段}
            k = m.klines1m.last_kline
            p = self.Params[m.codeL]
            p.dkline.appendkline(k)  # 合成日线
            p.kline30.appendkline(k)  # 合成30分钟线
            p.count1m += 1
            if p.count1m % 30 == 0:
                self.run30kline(m, p.kline30)  # 生成了30分钟线
                p.kline30.time = 0

            p.kline = k
            p.turnover += k.volume * (k.open + k.high + k.low + k.close) / 4
            p.volume += k.volume
            p.mav = p.turnover / p.volume if p.volume else k.close  # 分钟线生成的日内均线
            p.dir = 1 if k.close > p.mav else -1

        def run_minute_kline2(self):  # 每分钟跑完，回调，计算票和票、票和板块之间关系阶段，修正个票信号
            # 计算{仓位},修正状态
            for m in self.allContract.values():
                if m.typ != "L6": continue
                p = self.Params[m.codeL]
                if p.Select == 0: continue  # 日线没有选中,不做
                if p.dir == p.ddir:
                    n = self.allContract[m.breed + "L9"]  # 在L9上下单(n)
                    if p.dir > 0:
                        if n.short_position > 0:
                            self.doOrder(Order(n.code, "1", '3', n.LastPrice, 1))
                        if n.long_position <= 0:
                            self.doOrder(Order(n.code, "0", '2', n.LastPrice, 1))
                    if p.dir < 0:
                        if n.long_position > 0:
                            self.doOrder(Order(n.code, "1", '2', n.LastPrice, 1))
                        if n.short_position <= 0:
                            self.doOrder(Order(n.code, "0", '3', n.LastPrice, 1))


    ################
    main_kbase()