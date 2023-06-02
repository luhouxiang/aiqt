# -*- coding: utf-8 -*-
"""
@author: luhx
@file: strategy_imp.py
@time: 2023/5/31 21:27
@desc:
"""
# -*- coding: utf-8 -*-
"""
@author: luhx
@file: strategy_imp.py
@time: 2023/5/24 10:11
@desc: 策略具体实现
"""
import string
from base.kbase import kBase
from base.baseobj import KLine, TimeProp, Contract
from kds_util.user_logbook import init_logger as init_logs, user_log as logger
from highlow import algo
from datetime import datetime
# from highlow import store
# from highlow.dd_bkinfo import DdBkInfo
from highlow.dd_bs import DdBs
# from highlow.dd_bkinfo import DDParam
# from highlow.dd_obj import BkItem, TBkId, ESignalType
from highlow.envs import Envs
from highlow.dd_obj import DDParam
from highlow import envs
from typing import Dict
# from util import helper
# from highlow.database_opt import create_strategy_high_low_table
# from base.BaseCD import MA
# from datetime import timedelta
# from datetime import datetime
# from highlow.dd_obj import OpenOptItem, IncItem, OpenOptInfo
from kds_util.mysql_database import PyMySql


class StrategyImp(kBase):
    def __init__(self, investor_id: str, days=100, mindays=7, stime="", etime="", codes=[]):
        logger.info("系统正在数据初始化，请稍后...")
        super().__init__(investor_id, days, mindays, stime, etime, codes, playback=Envs.dest_play_back)
        bs = DdBs(self)
        logger.info("系统初始化完成.")
        Envs.data_center_ip = self.data_center_ip

        self.Params: dict[str, DDParam] = {}
        self.bs: DdBs = bs
        self.mysql = bs.mysql
        # create_strategy_high_low_table(bs.mysql)
        self.bs.dd_account.params = self.bs.params = self.bs.bk_info.params = self.bs.trader.params = self.Params
        for key in self.allContract:
            self.Params[key] = DDParam(self.allContract[key])
        # for key in self.Breed2Symbol.keys():
        #     l6 = key + "L6"
        # self.is_inited = True

    def reset_all_day_status(self):
        for k in self.Params.keys():
            self.Params[k].reset_new_one_day()
        # self.bs.bk_info.bk_items = {}

    def reset_hq_status(self):
        for k in self.Params.keys():
            self.Params[k].set_hq_status(False)

    def reset_night_trading_status(self):
        """
        重置夜盘的状态
        :return:
        """
        trade_time = self.trade_time
        if trade_time[-8:] == "02:30:00":
            logger.info(f"[{self.trade_time}]:夜盘时间结束，后面的为白盘...")
            for k in self.Params.keys():
                self.Params[k].reset_status()
            # for bk_code in self.bs.bk_info.bk_items.keys():
            #     self.bs.bk_info.bk_items[bk_code].trade_type = ""

    def init_open_position(self, bk_end_time):
        # mysql = self.mysql
        # self.bs.dd_account.contract_opt_positions = self.init_opt_position(mysql, bk_end_time)
        # return
        pass
    #
    # @staticmethod
    # def init_opt_position(mysql, bk_end_time) -> Dict[str, OpenOptItem]:
    #     """
    #     获取开仓对象，每种类型的开仓包含一组加仓列表
    #     @param mysql:
    #     @param bk_end_time:
    #     @return:
    #     """
    #     half_year_ago = datetime.strptime(bk_end_time, "%Y-%m-%d %H:%M:%S") + timedelta(days=-200)
    #     trade_day = int(half_year_ago.strftime("%Y%m%d"))
    #     sql = "SELECT contract,breed_side,change_position,record_time,opt_type,strategy_type,opt_price,opt_status," \
    #           "multiplier from tbl_strategy_opt_position where trade_day>{} order by record_time". \
    #         format(trade_day)
    #     datas = mysql.query(sql)
    #     opts: Dict[str, OpenOptItem] = {}
    #     for i in datas:
    #         contract = i[0]
    #         side = int(i[1])
    #         key = "{}.{}".format(contract, side)
    #         volume = int(i[2])
    #         record_time = i[3]
    #         opt_type = i[4]
    #         price = float(i[6])
    #         opt_status = int(i[7])
    #         multiplier = int(i[8])
    #         if opt_type == "open":
    #             opts[key] = OpenOptItem(contract, side, price, volume, record_time, multiplier)
    #         elif opt_type == "close":
    #             if key in opts:
    #                 del opts[key]
    #         else:
    #             if key not in opts:
    #                 continue
    #             if opt_type == "inc":  # 只记录加仓的
    #                 if opt_status == 2:  # 加仓后被减仓的，不记录在案
    #                     opts[key].inc_has_dec_arr.append(IncItem(side, price, volume, record_time, opt_status))
    #                 else:
    #                     opts[key].inc_arr.append(IncItem(side, price, volume, record_time, opt_status))
    #             if opt_type == "dec":
    #                 opts[key].dec_arr.append(IncItem(side, price, volume, record_time, opt_status))
    #     for key in opts.keys():
    #         # 排序，多仓价格从小到大排，空仓价格从大到小排
    #         opts[key].inc_arr.sort(key=lambda x: x.opt_price, reverse=opts[key].side != 1)
    #     return opts
    #
    # def init_force_stop(self):
    #     mysql = self.mysql
    #     spec_trade_day = algo.get_next_trading_day(self.bs.bk_info.pd_days, Envs.cur_trade_day, day_index=-3)
    #     sql = "SELECT contract,trade_day from tbl_strategy_opt_position WHERE trade_day>={}" \
    #           " and strategy_type='force_stop'". \
    #         format(spec_trade_day)
    #     datas = mysql.query(sql)
    #     for i in datas:
    #         contract = i[0]
    #         trade_day = i[1]
    #         breed = contract.rstrip(string.digits)
    #         if breed + "L6" not in self.bs.params:
    #             continue
    #         p1: DDParam = self.bs.params[breed + "L6"]
    #         p1.forced_stop = True
    #         logger.info(f"[{breed}].[{contract}]于交易日{trade_day}被强损")

    def is_same_white_or_night(self):
        if Envs.dest_play_back:  # 如果是回放，不再管是白盘还是夜盘
            return True
        # white_plate = Envs.white_plate_flag and self.bs.is_kline_white_market()
        # night_plate = not Envs.white_plate_flag and not self.bs.is_kline_white_market()
        # return white_plate or night_plate

    def clear_all_position(self):
        """

        清仓所有合约
        @return:
        """
        # holds = self.bs.dd_account.get_holds()
        # for hold_item in holds.values():
        #     self.bs.trader.opt_clear_position(hold_item, self.mysql)
        pass

    # def run_strategy(self):
    #     self.bs.activate_bk_ma5_status(self.Params)  # 激活板块ma5状态
    #     self.bs.dd_account.update_account_status(self.bs.dd_account.contract_opt_positions)  # 更新账户资金及持仓信息
    #     if Envs.dest_play_back and Envs.dest_clear_position_time:
    #         if self.dt >= datetime.strptime(Envs.dest_clear_position_time, "%Y-%m-%d %H:%M:%S"):
    #             self.clear_all_position()
    #             return
    #     bk_items: Dict[TBkId, BkItem] = self.bs.bk_info.bk_items
    #     holds = self.bs.dd_account.get_holds()
    #     for bk_code in bk_items.keys():
    #         bk_item: BkItem = bk_items[bk_code]
    #         if bk_item.signal_breed + "L6" not in self.Params:
    #             continue
    #         if bk_item.signal_breed and self.Params[bk_item.signal_breed + "L6"].dd_side != 0:  # 有顶底信号的处理
    #             self.bs.deal_has_signal_strategy(bk_item, holds)
    #         else:  # 无顶底信号的处理
    #             self.bs.deal_no_signal_strategy(bk_item, holds)
    #     # if not Envs.dest_play_back:
    #     self.bs.deal_force_stop_loss()  # 强制止损
    #     self.bs.deal_force_stop_win()  # 止赢
    #     self.bs.run_wave_strategy(self.Params)  # 下午2点50分进行当天大波动计算
    #     self.bs.moving_position(self.Params)

    def run_day_kline(self, m: Contract):  # 每个日线串行回调
        if m.klines1d.id >= len(m.klines1d.klines):
            logger.warn(f"{m.codeL}.{m.code}: m.klines1d.id:{m.klines1d.id} >= m.klines1d.id({len(m.klines1d.klines)})")
            return
        if self.bs.check_cur_trading_day_end():  # 如果当前交易日也走完了，直接退出
            # logger.warn(f"cur_trading_day_is_over. cur_trade_day: {Envs.cur_trade_day}")
            if Envs.dest_play_back:
                pass
            else:
                return
        k = m.klines1d.klines[m.klines1d.id]
        p: DDParam = self.Params[m.codeL]
        p.minute1_kline = k
        p.count1m = 0
        # p.Atr.input(k)  # 波动

    def run_day_kline2(self):
        # 日线跑完，全部回调  {筛票阶段}
        # if self.bs.check_prev_trading_day_end():
        #     if self.bs.check_cur_trading_day_end():  # 如果当前交易日也走完了，直接退出
        #         logger.warn(f"cur_trading_day_is_over. cur_trade_day: {Envs.cur_trade_day}")
        #         if Envs.dest_play_back:
        #             new_trading_day = algo.get_next_trading_day(self.bs.bk_info.pd_days, Envs.cur_trade_day)
        #             new_dt = datetime.strptime(str(new_trading_day), "%Y%m%d")
        #             new_dt += timedelta(hours=16)
        #             new_date_time = new_dt.strftime("%Y-%m-%d %H:%M:%S")
        #             algo.init_trade_day(self.bs.bk_info.pd_days, spec_time=new_date_time)
        #             self.init_daily_level_data()  # 重新初始化日线级数据
        #             self.init_cycle_level_data()
        #         else:
        #             pass
        #     else:
        #         self.init_daily_level_data()
        #         self.init_cycle_level_data()
        return

    def init_daily_level_data(self):
        """
        初始化日线级的数据
        这段代码用于初始化交易系统的日线级别数据。它重置每日状态，将前一天的大波动数据合并到信号中，
        如果适用则初始化开仓仓位，初始化强制平仓设置，运行黑名单策略，初始化日线级别的信号，并检查合同 dd 信号。
        1. 函数被定义，并有一个说明文档解释其目的。
        2. 打印一条日志消息，其中包括系统的开始和结束时间，还有初始化过程的开始时间。
        3. 函数调用一个名为 reset_all_day_status 的方法来重置每日状态。
        4. 函数使用一个名为 init_cycle_param 的方法初始化信号的周期参数。
        5. 函数使用一个名为 merge_prev_big_wave_to_signal 的方法将前一天的大波动数据合并到信号中。
        6. 函数使用一个名为 merge_prev_wave_to_signal 的方法将前一天的波动数据合并到信号中。
        7. 如果系统已经初始化，则函数使用一个名为 init_open_position 的方法初始化开仓仓位。
        8. 函数使用一个名为 init_force_stop 的方法初始化强制平仓设置。
        9. 函数使用一个名为 run_black_list 的方法运行黑名单策略。
        10. 函数使用一个名为 init_day_signal 的方法初始化日线级别的信号。
        11. 函数使用一个名为 check_contract_dd_signal 的方法检查合同 dd 信号。
        """
        logger.info(f"[{self.trade_time}]:系统运行起始日，开始：{Envs.bk_end_time}--》结束：{Envs.will_end_time}")
        self.reset_all_day_status()  # 重新
        # self.bs.signal_mgr.init_cycle_param(Envs.bk_end_time[:10] + " 16:00:00")
        # # 合并昨天大波动清仓数据到信号中
        # self.bs.bk_info.merge_prev_big_wave_to_signal(self.bs.params, Envs.prev_trade_day, self.bs.mysql)
        # self.bs.bk_info.merge_prev_wave_to_signal(self.bs.params)
        # if self.is_inited:
        #     self.init_open_position(Envs.bk_end_time)
        #     self.is_inited = False
        # self.init_force_stop()
        # self.bs.run_black_list()
        # # self.bs.signal_mgr.init_day_signal(Envs.bk_end_time[:10] + " 15:00:00", self.bs.params)  # 日线级别的数据
        # # self.check_contract_dd_signal(self.Params)

    def init_cycle_level_data(self):
        """
        初始化周期的数据
        @return:
        """
        logger.info(f"[{self.trade_time}]:初始化周期信号")
        # self.bs.deal_dd_signal()
        # self.bs.bk_info.init_bk_item_from_dd_signal(self.bs.params)  # 初始化板块顶底信号，每个板块从最强的选出一个来
        # self.bs.bk_info.init_bk_item_from_hold(self.bs.bk_info, self.bs.dd_account)  # 合并持仓数据到信号中
        # self.bs.log_signal_info()

    def run_minute_kline(self, m: Contract):  # 分钟线回调，计算个票{信号阶段}
        if m.klines1m.id >= len(m.klines1m.klines):
            logger.warn(
                f"{m.codeL}.{m.code}: m.klines1m.id:{m.klines1m.id} >= len(m.klines1m.id)({len(m.klines1m.klines)})")
            return
        k = m.klines1m.klines[m.klines1m.id]
        p: DDParam = self.Params[m.codeL]
        p.day_kline.appendkline(k)  # 合成日线
        p.kline30.appendkline(k)  # 合成30分钟线
        p.count1m += 1

        p.minute1_kline = k  # 合成分钟线
        p.turnover += k.volume * (k.open + k.high + k.low + k.close) / 4
        p.volume += k.volume
        p.mav = p.turnover / p.volume if p.volume else k.close  # 分钟线生成的日内均线
        p.dir = 1 if k.close > p.mav else -1
        p.set_hq_status(True)  # 标识当前分钟有行情
        # if not self.bs.prev_day_end:  # 对于分钟数据，只有到达新交易日了才能使用
        #     return
        # self.update_fenshi_jx_sx(k, p)  # 更新分时金死叉
        # self.update_ma5_status(p)  # 计算ma5
        # self.bs.calc_today_big_wave(p, p.minute1_kline)  # 计算当天是否有大波动
        return

    def whole_n_minute(self, minute=15) -> bool:
        """
        判断系统时间是否为整minute(15)分钟
        @param minute:
        @return:
        """
        if not Envs.dest_multi_cycle_flag:
            return False
        rel = ((self.intTradeTime // 100) % 100) % minute == 5
        return rel

    def manage_positions(self):
        """管理持仓，检查信号方向与持仓方向一致性，不一致进行清仓，
        一致的话根据盈亏情况进行加仓或减仓操作
        """
        # self.bs.dd_account.update_account_status(self.bs.dd_account.contract_opt_positions)  # 更新账户资金及持仓信息
        # holds = self.bs.dd_account.get_holds()
        # for breed in holds.keys():
        #     hold_item = holds[breed]
        #     if breed + "L6" not in self.Params:
        #         continue
        #     if hold_item.net_position == 0:  # 持仓为0的，就不用管了
        #         continue
        #     p1: DDParam = self.Params[breed + "L6"]
        #     p2: DDParam = self.Params[breed + "L9"]
        #     if not p2.trade_able:
        #         continue
        #     bk_code = self.bs.bk_info.get_bk_from_breed(breed)
        #     breed = hold_item.contract.rstrip(string.digits)
        #     hold_side = 1 if hold_item.net_position > 0 else -1
        #     if helper.is_difference_side(p1.signal_strength, p1.signal_strength, hold_side):
        #         logger.info(f"[{self.trade_time}]指数与持仓方向不同，清仓{bk_code}.{breed}.{hold_item.contract},"
        #                     f"指数方向:{p1.signal_strength},持仓方向:{hold_side}, 多头持仓:{hold_item.long_position},"
        #                     f"空头持仓:{hold_item.short_position}:{hold_item.contract}")
        #     if helper.is_difference_side(p2.signal_strength, p2.signal_strength, hold_side):
        #         logger.info(f"[{self.trade_time}]主力与持仓方向不同，清仓{bk_code}.{breed}.{hold_item.contract},"
        #                     f"主力方向：{p2.signal_strength},持仓方向:{hold_side},多头持仓:{hold_item.long_position},"
        #                     f"空头持仓:{hold_item.short_position}:{hold_item.contract}")
        #
        #     # 指数与持仓或是主力与持仓的方向不同
        #     if helper.is_difference_side(p1.signal_strength, p2.signal_strength, hold_side):
        #         self.bs.trader.opt_clear_position(hold_item, self.mysql)
        #     else:
        #         self.bs.wait_jx_sx_modify_position(hold_item, risk_rate=0.9)  # 加减仓,0.9相当于不做风控
        return

    def execute_signal_trades(self):
        """执行信号交易操作，买入未持仓的票"""
        # self.bs.activate_bk_ma5_status(self.Params)  # 激活板块ma5状态
        # self.bs.dd_account.update_account_status(self.bs.dd_account.contract_opt_positions)  # 更新账户资金及持仓信息
        # if Envs.dest_play_back and Envs.dest_clear_position_time:
        #     if self.dt >= datetime.strptime(Envs.dest_clear_position_time, "%Y-%m-%d %H:%M:%S"):
        #         self.clear_all_position()
        #         return
        #
        # bk_items: Dict[TBkId, BkItem] = self.bs.bk_info.bk_items
        # holds = self.bs.dd_account.get_holds()
        # for bk_code in bk_items.keys():
        #     hold_item = algo.find_hold(self.bs.bk_info, holds, bk_code)
        #     if hold_item and hold_item.net_position != 0:  # 有持仓的前面已经处理过了
        #         continue
        #     bk_item: BkItem = bk_items[bk_code]
        #     self.bs.handing_no_position(bk_item)
        #
        # self.bs.deal_force_stop_loss()  # 强制止损
        # self.bs.deal_force_stop_win()  # 止赢
        # self.bs.run_wave_strategy(self.Params)  # 下午2点50分进行当天大波动计算
        # self.bs.moving_position(self.Params)

        return

    def run_minute_kline2(self):  # 每分钟跑完，回调，计算票和票、票和板块之间关系阶段，修正个票信号
        # if not self.bs.prev_day_end:
        #     return
        # if self.whole_n_minute():
        #     self.init_cycle_level_data()  # 更新周期信息
        #
        # self.bs.dd_account.calc_contract_opt_profit()

        if self.is_same_white_or_night():  # 系统和K线同是白盘或同是夜盘
            self.manage_positions()     # 管理持仓
            # self.run_strategy()
            self.execute_signal_trades()    # 无持仓的执行信号开仓操作
        k = self.Params["wenhuashangpin"].contract_info.Last_minute_Kline
        # logger.info(f"wenhuashangpin_dt:{k.trade_time}, sys_dt:{self.trade_time}")
        self.reset_night_trading_status()
        self.reset_hq_status()  # 将每个品种的行情重新置为不可用
