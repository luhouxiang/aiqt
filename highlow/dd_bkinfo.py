# -*- coding: utf-8 -*-
"""
@author: luhx
@file: dd_bkinfo.py
@time: 2023/3/7 16:58
@desc: 板块相关信息的类
"""
from highlow.dd_obj import TBkId, TBreed, TContract, BkItem, TCodeL6
from typing import Dict, List
import pandas as pd
from highlow.envs import Envs
from kds_util.user_logbook import user_log as logger
from sqlalchemy import create_engine
from datetime import datetime
from highlow import dd_obj as obj
from base.kbase import kBase
from base.baseobj import KLine, Contract
# from base.BaseCD import RunDCond002, ATR, CDBASE5, BSma5
from highlow.dd_obj import BreedItem, ESignalType
# from highlow import store
from datetime import timedelta
from highlow.dd_obj import DDParam
from highlow import helper
from kds_util.mysql_database import PyMySql
from highlow import algo


class DdBkInfo:
    """
    板块的信息
    """

    def __init__(self, acc):
        self.bk_component: Dict[TBreed, str] = {}  # key为breed,value为bk_id
        self.bk_priority: Dict[TBreed, int] = {}  # 用来记录优先级
        self.bk_items: Dict[TBkId, BkItem] = {}
        self.bk_dict: Dict[TBkId, List[TBreed]] = {}
        self.wx_send_flag = Envs.dest_wx_send
        self.params = None
        data_center_ip = "192.168.1.100"
        if acc:
            data_center_ip = acc.data_center_ip
        self.init_bk_component(data_center_ip)
        self.pd_days = self.init_ths_trading_day(data_center_ip)
        self.kline_pd = self.init_kline_data(data_center_ip)
        self.acc = acc

    @staticmethod
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

    @staticmethod
    def init_kline_data(ip):
        """
        初始化半年内的数据
        :return:
        """
        dt = (datetime.now() - timedelta(days=300))
        engine = create_engine(
            f'mysql+pymysql://root:2234567@{ip}:3306/hqdb?charset=utf8')  ###建立连接↓qh_index_db
        con = engine.connect()
        sql = f"SELECT trading_day,code from tbl_future_all_kline_day WHERE  trading_day>{dt.strftime('%Y%m%d')}"
        logger.info(sql)
        df = pd.read_sql(sql, con)
        con.close()
        # newdf = df[(df["trading_day"]<trade_date)&(df["code"]==code)].copy()
        # return len(df[(df["trading_day"]<trade_date)&(df["code"]==code)])
        return df

    def get_breed_from_l6(self, code_l6: TCodeL6) -> TBreed:
        """
        通过L6指数名获取到品种名
        :param code_l6:
        :return:
        """
        return code_l6[:-2]

    def init_bk_component(self, ip):
        """
         初始化指数属于哪些板块
        :return:
        """
        mysql = PyMySql(f'mysql+pymysql://root:2234567@{ip}:3306/qh_index_db?charset=utf8')
        sql = "SELECT ContractWeightCode,BkCode,BkType,Priority from tbl_component_bk where Priority>0"
        logger.info(sql)
        datas = mysql.query(sql)
        for i in datas:
            breed = self.get_breed_from_l6(i[0])
            bk_code = i[1]
            if bk_code not in self.bk_dict:
                self.bk_dict[bk_code] = []
            if breed not in self.bk_dict[bk_code]:
                self.bk_dict[bk_code].append(breed)
            priority = int(i[3])
            if breed not in self.bk_priority:
                self.bk_component[breed] = bk_code
                self.bk_priority[breed] = priority
            elif priority > self.bk_priority[breed]:
                self.bk_component[breed] = bk_code
                self.bk_priority[breed] = priority
        return

    def get_all_bk_breeds(self):
        d = {}
        for bk_code in self.bk_dict.keys():
            arr = self.get_breeds_from_bk(bk_code)
            d[bk_code] = arr
        return d

    def get_breeds_from_bk(self, bk_code: TBkId) -> List[TBreed]:
        breeds = self.bk_dict[bk_code]
        new_breeds = []
        for breed in breeds:
            if self.get_bk_from_breed(breed) == bk_code:
                new_breeds.append(breed)
        return new_breeds

    def get_bk_from_breed(self, breed: TBreed) -> TBkId:
        if breed in self.bk_component:
            return self.bk_component[breed]
        else:
            logger.warn(F"品种:{breed} not in 任何板块中!!!")
        return ""

    def init_bk_item_from_dd_signal(self, params):
        """
        通过params中的dd_signal整理出整个板块->breed_item数据结构
        :param params:
        :return:
        """
        for key in params.keys():
            if key[-2:] != "L6":  # 只针对指数的
                continue
            breed_id = key[:-2]
            p1: DDParam = params[breed_id + "L6"]
            side = p1.dd_side

            bk_code = self.get_bk_from_breed(breed_id)
            if not bk_code:
                continue
            if bk_code not in self.bk_items:
                obj.create_new_bk_item(self.bk_items, bk_code, born_type="dd_base_signal_pure")
            bk_item: BkItem = self.bk_items[bk_code]
            if breed_id not in bk_item.components:
                bk_item.components[breed_id] = obj.create_new_breed_item(
                    breed_id, side, born_type="dd_base_signal_pure")

        for bk_code in self.bk_items.keys():
            self.reject_invalid_signal(bk_code)  # 去除无效信号
            self.select_bk_delegate(params, self.bk_items[bk_code])  # 排序，选一个指数信号作为板块信号
        # logger.info(f"[{self.acc.trade_time}]:init_bk_item_from_dd_signal end.")

    def init_bk_item_from_hold(self, bk_info, account):
        """更新持仓数据到信号中
        如果持仓在信号中不存在，新建一个纯持仓的信号
        如果持仓在信号中存在且方向相同，直接使用持仓中的breed替换bk中的breed
        如果持仓在信号中存在，但方向相反，忽略持仓数据，什么都不做，因为后面会直接清仓，重新开仓
        ... 此函数最主要的用途是将持仓数据归于bk_items中，不做任何处理
        """
        account.update_account_status(account.contract_opt_positions)
        holds = account.get_holds()
        bk_items: Dict[TBkId, BkItem] = bk_info.bk_items
        for breed in holds.keys():  # 以持仓为依据
            if holds[breed].net_position == 0:
                continue
            bk_code = bk_info.get_bk_from_breed(breed)
            if not bk_code:
                continue
            if bk_code not in bk_items:  # 不存在,创建新的
                obj.create_new_bk_item(bk_items, bk_code)
            if breed not in bk_items[bk_code].components:
                bk_items[bk_code].components[breed] = obj.create_new_breed_item(breed, breed_side=0)  # 对于纯粹的持仓，不要去动信号方向
            bk_items[bk_code].components[breed].born_type += ".hold"

    def merge_prev_big_wave_to_signal(self, params, prev_trade_day, mysql):
        """
        合并之前波动信号到当前信号中
        :param bk_info:
        :param prev_trade_day:
        :param mysql:
        :return:
        """
        # waves: Dict[TBreed, BigWaveItem] = store.get_wave_bk_from_mysql(prev_trade_day, mysql)
        # for breed in waves.keys():
        #     p2: DDParam = params[breed + "L9"]
        #     p2.yesterday_big_wave_position = waves[breed].position
        #     p2.yesterday_big_wave_contract = waves[breed].contract
        return

    def calc_signal_days(self, signals, breed_id, end_day) -> int:
        """
        给定当前交易日，品种，获取该品种最早同向信号距现在相隔多少个交易日
        :param signals:
        :param breed_id:
        :param end_day:
        :return:
        """
        if breed_id not in signals:
            return -1
        trading_day = int(signals[breed_id][0])
        index1 = self.pd_days[self.pd_days["trading_days"] <= trading_day].index
        index2 = self.pd_days[self.pd_days["trading_days"] >= end_day].index
        if index1.empty or index2.empty:
            return -1
        r = index2[0] - index1[-1]
        return r

    def merge_prev_wave_to_signal(self, params: Dict[str, DDParam]):
        """
        合并昨天大波动到信号中
        :param params:
        :param bk_info:
        :return:
        """
        # for k in params:
        #     p: DDParam = params[k]
        #     p.yesterday_big_wave_flag = p.Atr.MTR > p.Atr.atr
        #     # if k == "buL9":
        #     #     if p.contract_info.Last_Day_Kline.intTradeDay >= 20230403:
        #     #         logger.warn(f"####*##[{p.contract_info.Last_Day_Kline.intTradeDay}]: merge:yesterday_big_wave_flag:"
        #     #                     f"{p.yesterday_big_wave_flag}")
        return

    def get_or_new_breed_item(self, bk_item: BkItem, breed: TBreed, side: int) -> BreedItem:
        """存在就返回对应的BreedItem,不存在就新建一个并返回"""
        if breed not in bk_item.components:
            bk_item.components[breed] = obj.create_new_breed_item(breed, side)
        return bk_item.components[breed]

    def get_or_new_bk_item(self, bk_items, bk_code):
        if bk_code not in bk_items:
            return obj.create_new_bk_item(bk_items, bk_code)
        return bk_items[bk_code]

    def reject_invalid_signal(self, bk_code):
        """
        去除无效信号：将无效信号加上标识，使之不参与排序选拔
        :param bk_code:
        :return:
        """
        # bk_item: BkItem = self.bk_items[bk_code]
        # kongs: List[BreedItem] = []
        # duos: List[BreedItem] = []
        # for breed in bk_item.components.keys():
        #     breed_item: BreedItem = bk_item.components[breed]
        #     if breed + "L6" not in self.params:
        #         continue
        #     p1: DDParam = self.params[breed + "L6"]
        #     p2: DDParam = self.params[breed + "L9"]
        #     if p1.dd_side == 0:  # 没有顶底信号的，不排序
        #         breed_item.sort_by_logo = False
        #         continue
        #     else:
        #         breed_item.sort_by_logo = True  # 有的需要重新参与进来
        #     if Envs.dest_bk_type == "dd_activate" or Envs.dest_bk_type == "dd_activate003":  # 板块顶底
        #         p5: DDParam = self.params[bk_code]
        #         p5_dd_side = int(abs(p5.signal_strength) // p5.signal_strength) if p5.signal_strength != 0 else 0
        #         if helper.is_difference_side(p1.dd_side, p5_dd_side, 0):
        #             logger.info(f"指数信号不参与排序,{breed}.{p1.dd_side},bk:{p5.signal_strength}")
        #             breed_item.sort_by_logo = False  # 如果板块顶底与信号顶底不相同，则该指数不参与排序
        #             continue
        #     if helper.is_difference_side(p1.dd_side, p2.signal_strength, 0):  # 如果L6与L9冲突，不参与代表选择
        #         breed_item.sort_by_logo = False
        #         continue
        #     elif breed_item.breed_side == 1:  # 将多头信号距离当前最近的放在最后面
        #         if not duos:
        #             duos.append(breed_item)
        #         else:
        #             p1: DDParam = self.params[duos[-1].breed_id + "L6"]
        #             p_cur: DDParam = self.params[breed_item.breed_id + "L6"]
        #             if abs(p1.signal_strength) > abs(p_cur.signal_strength):
        #                 duos.append(breed_item)
        #             else:
        #                 duos.insert(0, breed_item)
        #     else:  # 将空头信号距离当前最近的放在最后面
        #         if not kongs:
        #             kongs.append(breed_item)
        #         else:
        #             p1: DDParam = self.params[kongs[-1].breed_id + "L6"]
        #             p_cur: DDParam = self.params[breed_item.breed_id + "L6"]
        #             if abs(p1.signal_strength) > abs(p_cur.signal_strength):
        #                 kongs.append(breed_item)
        #             else:
        #                 kongs.insert(0, breed_item)
        #
        # if kongs and duos:
        #     p_kong: DDParam = self.params[kongs[-1].breed_id + "L6"]
        #     p_duo: DDParam = self.params[duos[-1].breed_id + "L6"]
        #     if abs(p_kong.signal_strength) == abs(p_duo.signal_strength):  # 多空在同一天
        #         # 将所有多空不在这一天的排除出去
        #         for breed in bk_item.components.keys():
        #             breed_item: BreedItem = bk_item.components[breed]
        #             p3: DDParam = self.params[breed + "L6"]
        #             if abs(p3.signal_strength) != abs(p_kong.signal_strength):
        #                 breed_item.sort_by_logo = False
        #     elif abs(p_kong.signal_strength) < abs(p_duo.signal_strength):
        #         # 将所有多的排除出去
        #         for breed_item in duos:
        #             breed_item.sort_by_logo = False
        #     else:
        #         # 将所有空的排除出去
        #         for breed_item in kongs:
        #             breed_item.sort_by_logo = False
        return

    def get_dd_day_klines(self, params, breed, days) -> list:
        """
        获取顶底信号往前几日的K线
        :param params:  数据相关的参数信息
        :param breed:   品种名
        :param days:    顶底信号时间向前推进的天数
        :return:    返回K线数组
        """
        l6_breed = breed + "L6"
        if l6_breed not in params:
            return []
        p: DDParam = params[l6_breed]
        day_time = p.dd_time
        klines = p.contract_info.klines1d.klines
        find = False
        arr = []
        for item in reversed(klines):
            if datetime.fromtimestamp(item.time).strftime("%Y-%m-%d %H:%M:%S") == day_time:
                find = True
            if find:
                arr.append(item)
                if len(arr) == days:  # 获取到对应的天数，退出
                    arr = list(reversed(arr))
                    return arr
        return []

    def select_bk_delegate(self, params, bk_item: BkItem):
        """
        选出板块代表
        :param params:
        :param bk_item:
        :return:
        """
        for breed in bk_item.components.keys():
            breed_item = bk_item.components[breed]
            if not breed_item.sort_by_logo:  # 不参与排序的，先过滤
                continue
            kline_item = self.get_dd_day_klines(params, breed, 5)
            if not kline_item:
                continue
            breed_item.dd_forward_5_day_zdf = (kline_item[-1].close - kline_item[0].close) / kline_item[0].close if \
                kline_item[0].close > 0 else 0.0
        breed = self.sort_zdf(bk_item.components)  # 排序同一板块中到信号的涨跌幅，取最大的
        if breed:  # 找出来，板块方向就用选出来的方向和信号
            bk_item.signal_breed = breed

        return True

    def sort_zdf(self, components: Dict[str, BreedItem]) -> TBreed:
        """涨跌幅排序，对于顶，取跌幅最大的，对于底，取涨幅最大的"""
        limit_value = 0
        breed = ""
        for k in components.keys():
            if not components[k].sort_by_logo:  # 不是有效的，下一个
                continue
            if abs(components[k].dd_forward_5_day_zdf) >= limit_value:
                limit_value = abs(components[k].dd_forward_5_day_zdf)
                breed = k
        return breed