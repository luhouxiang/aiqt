# -*- coding: utf-8 -*-
"""
@author: luhx
@file: dd_bs.py
@time: 2023/6/1 21:49
@desc:
"""
from typing import Dict
from highlow import algo
from highlow.envs import Envs
from highlow.dd_obj import DDParam
from base.kbase import kBase
from highlow.dd_trader import TraderMan
from highlow.dd_bkinfo import DdBkInfo
from highlow.account import DDAccount
from kds_util.mysql_database import PyMySql


class DdBs:
    def __init__(self, acc: kBase):
        self.acc = acc
        # self.investor_id = acc.investor_id
        url = self.acc.url
        self.ip = url.rstrip(":8088/api/mgr").lstrip("http://")
        self.mysql = PyMySql(f"mysql+pymysql://root:2234567@{self.ip}:3306/hqdb?charset=utf8")
        self.bk_info: DdBkInfo = DdBkInfo(acc)
        self.params: Dict[str, DDParam] = None
        self.prev_day_end: bool = False
        self.dd_account = DDAccount(acc, self.ip, acc.investor_id)
        self.trader = TraderMan(acc, self.dd_account, self.bk_info, Envs.dest_wx_send, self.mysql)
        # self.signal_mgr = SignalSource(acc, acc.data_center_ip)
        Envs.dest_ip = self.ip

    def check_cur_trading_day_end(self):
        """
        判断当前的交易日已经完成
        """
        dt = self.acc.dt
        return Envs.cur_trade_day == algo.get_kline_trading_day(self.bk_info.pd_days, dt)