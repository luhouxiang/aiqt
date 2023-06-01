# -*- coding: utf-8 -*-
"""
@author: luhx
@file: dd_obj.py
@time: 2023/5/31 21:52
@desc:
"""
from base.baseobj import KLine, Contract
class DDParam:
    def __init__(self, contract) -> None:
        self.score = 0.0
        self.day_kline = KLine()  # 当日合成的日线，动态的
        self.count1m = 0  # 当天1分钟K线计数
        self.turnover = 0.0
        self.volume = 0
        self.dma5 = 0.0
        self.ddir = 0
        self.Select = 0
        self.dir = 0
        self.kline30 = KLine()  # 当前30分钟线
        self.mav = 0.0
        self.contract_info: Contract = contract
        # self.Atr: ATR = ATR()  # ATR计算类,用于计算大波动
        # self.today_big_wave_flag = False  # 当前大波动标识(针对的是今天的）
        # self.bs_minute: CDBASE5 = CDBASE5()  # 计算日内金叉死叉
        # self.bs_ma5: BSma5 = BSma5()  # 计算模糊ma5多空
        self.signal_strength = 0  # 信号强度
        self.dd_side = 0  # 顶底方向，1表示底（多），-1表示顶（空）
        self.dd_time: str = ""  # 顶底信号的时间
        self.dd_kt_time: str = ""  # 生成顶底信号K线的时间
        self.dd_period: int = 1440  # 顶底周期，默认1440分钟
        self.dd_from: str = ""  # 信号来源
        self.signal_duration_days: int = 0  # 顶底信号持续的天数(需要特别计算）
        self.yesterday_big_wave_contract: str = ""  # 昨大波动清仓票的合约号
        self.yesterday_big_wave_position: int = 0  # 如果存在昨天大波动且清仓的，记录下来,此值表示清仓量，负数表示空单，正数为多单
        self.yesterday_big_wave_reopen: int = 0  # 如果存在昨天大波动且清仓的，此变量有效,默认为0，-1表示已失效，为1表示已买回来
        self.yesterday_big_wave_flag: bool = False  # 昨天是否存在大波动
        # self.ma5: MA = MA()  # 实时ma5值，昨日及之前用日线，当天用分钟线合并成日线
        self.ma5_long_activate: bool = False  # ma5激活，当ma5值在均线之上时，表示多头被激活
        self.ma5_short_activate: bool = False  # ma5激活，当ma5值在均线之下时，表示空头被激活
        self.ma5_long_time: str = ""  # ma5多头激活时间
        self.ma5_short_time: str = ""  # ma5空头激活时间
        self.ma5_activate_side: int = 0  # 默认为0，表示未激活，1：表示激活为多头，-1：表示激活为空头
        self.ma5_activate_time: str = ""    # 激活时间
        self.ma5_real_time_time: str = ""  # ma5实时激活时间
        self.ma5_real_time_side: int = 0    # ma5实时激活方向
        self.jx_flag: bool = False  # 日内金叉标识
        self.sx_flag: bool = False  # 日内死叉标识
        self.zhengshu: bool = False  # 日内碰整数标识
        self.dd_disabled: bool = False  # 黑名
        self.forced_stop: bool = False  # 强制止损标识，3个交易日内被强制止损
        self.old_table_factor: int = 0    # 上次的表格系数
        self.cur_table_factor: int = 0    # 当前的表格系数
        self.dd_120m_strength: int = 0  # 120分钟顶底强度，0表示无信号，正数表示多，负数表示空
        self.hq_status: bool = False

    def set_hq_status(self, status: bool):
        self.hq_status = status

    @property
    def trade_able(self):
        """当前分钟有行情，就认为可以交易"""
        if self.hq_status:
            dg_time = self.contract_info.dt.hour * 100 + self.contract_info.dt.minute
            if self.contract_info.ExchangeId == "CFFEX":    # 中金所
                trading = (945 <= dg_time < 1500)
            else:
                trading = (915 <= dg_time < 1500) or (2115 <= dg_time)
            return trading
        else:
            return False

    def reset_status(self):
        """
        重置部分状态值
        :return:
        """
        pass

    def reset_new_one_day(self):
        self.dd_side = 0
        self.dd_time = ""
        self.signal_strength = 0
        self.signal_duration_days = 0
        self.day_kline = KLine()    # 新的一天，重新初始化

        self.ma5_long_activate = False
        self.ma5_short_activate = False
        self.ma5_long_time = ""
        self.ma5_short_time = ""
        self.ma5_activate_side = 0
        self.jx_flag = False
        self.sx_flag = False
        self.zhengshu = False