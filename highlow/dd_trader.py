import string

from highlow.dd_obj import HoldItem, TBreed, BkItem, BreedItem, DDParam
from kds_util.user_logbook import user_log as logger
# from util import helper
# from highlow.wx import WchatSend
# from highlow.store import insert_strategy_opt_position
# from highlow import store
from highlow import algo
import time
from highlow.envs import Envs
from base.baseobj import Order, TimeProp
from base.kbase import kBase


class TraderIo:
    """交易接口，只是单纯的为了完成交易"""

    def __init__(self, acc: kBase, dd_account, mysql):
        self.acc = acc
        self.dd_account = dd_account
        self.mysql = mysql

    def open_position(self, h: HoldItem, contract_id: str, opt_vol: int) -> (int, str):
        """
        开仓给定操作的手数，返回预期的手数
        """
        expected_volume = opt_vol
        if h:
            expected_volume = (h.long_position + opt_vol) if opt_vol > 0 else (opt_vol + h.short_position)
        position_direction = "2" if opt_vol > 0 else "3"
        # self.account.SendBS(MHTP=0, OCBS="0", LSBS=position_direction, price=0, VOL=abs(opt_vol), CODE=contract_id)
        self.acc.doOrder(Order(contract_id, "0", position_direction, 0, abs(opt_vol)))
        return expected_volume, position_direction

    def close_position(self, h: HoldItem, contract_id: str, opt_vol: int) -> (int, str):
        """
        平仓给定操作的手数，返回预期的手数
        """
        if h:
            expected_volume = (h.long_position - opt_vol) if opt_vol > 0 else (h.short_position - opt_vol)
        else:
            return 0, ""
        position_direction = "2" if opt_vol > 0 else "3"
        # self.account.SendBS(MHTP=0, OCBS="1", LSBS=position_direction, price=0, VOL=abs(opt_vol), CODE=contract_id)
        self.acc.doOrder(Order(contract_id, "1", position_direction, 0, abs(opt_vol)))
        return expected_volume, position_direction

    def open_and_wait(self, h: HoldItem, breed: TBreed, opt_vol: int) -> bool:
        """开仓并等待结果"""
        if opt_vol == 0:
            return False
        self.open_position(h, h.contract, opt_vol)
        return True

    def close_and_wait(self, h: HoldItem, breed: TBreed, opt_vol: int) -> bool:
        """平仓并等待结果"""
        if opt_vol == 0:
            return False
        self.close_position(h, h.contract, opt_vol)
        return True


class TraderMan:
    def __init__(self, acc: kBase, dd_account, bk_info, wx_send, mysql):
        self.trader_io = TraderIo(acc, dd_account, mysql)
        self.bk_info = bk_info
        self.dest_wx_send = wx_send
        self.mysql = mysql
        self.params = None
        # self.wx = WchatSend(acc, self.dest_wx_send)

    def opt_open_new_position(self, params, position, breed, bk_item: BkItem, holds, mysql, strategy_type="open",
                              remark="新开仓"):
        """
        开新仓
        """
        # if helper.is_forbid_opt_open_position(bk_item):
        #     return
        # p1: DDParam = params[breed + "L6"]
        # p2: DDParam = params[breed + "L9"]
        # if not p2.trade_able:   # 不具备交易条件，直接退出
        #     return
        # bk_code = bk_item.bk_code
        # p5: DDParam = params[bk_code]
        # if helper.is_difference_side(p1.signal_strength, p5.ma5_real_time_side, p5.ma5_real_time_side):
        #     logger.info(f"系统时间:{self.trader_io.acc.trade_time}, 指数[{breed}]与板块[{bk_code}]ma5方向不同"
        #                 f"[{p1.signal_strength}]!=[{p5.ma5_real_time_side}], "
        #                 f"不可开新仓. 板块时间：{p5.ma5_real_time_time}")
        #     return
        # contract = p2.contract_info.code
        # contract_price = p2.contract_info.LastPrice
        # hold_item = HoldItem()
        # hold_item.contract = contract
        # holds[breed] = hold_item
        # expected_volume = position  # 预期仓位
        # opt_vol = self.trader_io.dd_account.risk_control(expected_volume, contract, contract_price, risk_rate=0.9)
        # if abs(opt_vol) == 0:
        #     logger.warn(f"由于资金管控开仓数量为0，当前交易不成功, {contract}: side: {p1.dd_side}, "
        #                 f"kt: {p1.contract_info.Last_minute_Kline.trade_time}")
        #     return
        # side = int(abs(position) // position)
        # logger.info(f"[{p2.contract_info.Last_minute_Kline.trade_time}]will open new {contract}: side:{side},"
        #             f"num:{opt_vol}, kt: {p1.contract_info.Last_minute_Kline.trade_time}")
        # insert_strategy_opt_position(strategy_type, "open", contract, side, abs(opt_vol),
        #                              abs(opt_vol), margin=0.0, profit=0.0, opt_status=1,
        #                              opt_price=self.trader_io.acc.LastPrice(hold_item.contract),
        #                              kt=p2.contract_info.Last_minute_Kline.trade_time,
        #                              multiplier=p2.contract_info.Multiplier,
        #                              opts=self.trader_io.dd_account.contract_opt_positions, mysql=mysql)
        # rel = self.trader_io.open_and_wait(hold_item, breed, opt_vol)
        # investor_msg = f"账号：{self.trader_io.dd_account.dest_investor}"
        # code_msg = f"[{bk_code}].{contract}"
        # hold_info = f"【{['多头', '空头'][0 if side > 0 else 1]}】:{remark}:{abs(opt_vol)}手,status:{rel}"
        # dest_time = f"板块K线时间:{[p5.ma5_real_time_time]}, " \
        #             f"指数K线时间:{p1.contract_info.Last_minute_Kline.trade_time},指数信号{p1.signal_strength}," \
        #             f"周期:{p1.dd_period}m," \
        #             f"合约信号{p2.signal_strength},周期:{p2.dd_period}m"
        # self.wx.send_wx_opt_msg(investor_msg, code_msg, hold_info, dest_time, self.dest_wx_send)
        # trade_day = Envs.bk_end_time[:10].replace("-", "")
        # if p2.yesterday_big_wave_position != 0:
        #     p2.yesterday_big_wave_reopen = 1
        #     store.update_big_wave_state(trade_day, contract, mysql, flag=p2.yesterday_big_wave_reopen)
        # bk_item.trade_type = "OpenOptPosition"
        return

    def opt_modify_position(self, hold_item: HoldItem, bk_item: BkItem, duo, jx, kong, sx,
                            zhengshu, price: float, risk_rate, mysql):
        """
        加减仓操作
        """
        pass
        # if hold_item.net_position > 0:  # 多头持仓
        #     hold_position = hold_item.long_position
        #     trade_profit = hold_item.long_profit
        #     open_position = hold_item.long_open_position
        #     hold_side = 1
        # else:  # 空头持仓
        #     hold_position = hold_item.short_position
        #     trade_profit = hold_item.short_profit
        #     open_position = hold_item.short_open_position
        #     hold_side = -1
        # # if Envs.dest_inc_type == "from_table":  # 如果是从表取参数，直接运行加减仓逻辑
        # #     self.opt_table_modify_position()
        # #     return
        #
        # new_position = algo.calc_new_position_from_open_position(  # 返回新的持仓数
        #     self.trader_io.dd_account, hold_item.contract, price, trade_profit, open_position, hold_side)
        # if not (duo or jx or kong or sx):  # 当前做多不满足，做空不满足，退出
        #     return
        # if hold_item.net_position == 0:  # 净持仓为0，直接退出（空仓的情况下，不得加减仓)
        #     return
        #
        # opt_vol, offset = algo.calc_increment_position(new_position, hold_item)
        # if opt_vol == 0:
        #     return
        # if not (offset == "0" or offset == "1"):
        #     return
        #
        # if helper.is_forbid_opt_position(bk_item):
        #     return
        #
        # rel = False
        # side = 1 if opt_vol > 0 else -1
        # breed = hold_item.contract.rstrip(string.digits)
        # p2: DDParam = self.params[breed + "L9"]
        # if not p2.trade_able:   # 不具备交易条件，直接退出
        #     return
        # if offset == "0":  # 加仓
        #     opt_vol = self.trader_io.dd_account.risk_control(opt_vol, hold_item.contract, price, risk_rate=risk_rate)
        #     if opt_vol == 0:
        #         return
        #     insert_strategy_opt_position("inc", "inc", hold_item.contract, side,
        #                                  abs(opt_vol), abs(hold_position) + abs(opt_vol), margin=0.0, profit=0.0, opt_status=1,
        #                                  opt_price=self.trader_io.acc.LastPrice(hold_item.contract),
        #                                  kt=p2.contract_info.Last_minute_Kline.trade_time,
        #                                  multiplier=p2.contract_info.Multiplier,
        #                                  opts=self.trader_io.dd_account.contract_opt_positions, mysql=mysql)
        #     rel = self.trader_io.open_and_wait(hold_item, breed, opt_vol)
        #     bk_item.trade_type = "IncreasePosition"
        # elif offset == "1":  # 减仓
        #     insert_strategy_opt_position("dec", "dec", hold_item.contract, side,
        #                                  abs(opt_vol), abs(hold_position) - abs(opt_vol), margin=0.0, profit=0.0, opt_status=1,
        #                                  opt_price=self.trader_io.acc.LastPrice(hold_item.contract),
        #                                  kt=p2.contract_info.Last_minute_Kline.trade_time,
        #                                  multiplier=p2.contract_info.Multiplier,
        #                                  opts=self.trader_io.dd_account.contract_opt_positions, mysql=mysql)
        #     rel = self.trader_io.close_and_wait(hold_item, breed, opt_vol)
        #     bk_item.trade_type = "ReducePosition"
        # kline = p2.contract_info.Last_minute_Kline
        # self.send_opt_msg(bk_item, breed, hold_item, kline, opt_vol, rel)

    def send_opt_msg(self, bk_item, breed, hold_item, kline, opt_vol, rel):
        # side = 1 if hold_item.net_position > 0 else -1
        # hold_info = f"【{['多头', '空头'][0 if side > 0 else 1]}】:原有仓位" \
        #             f"{abs([hold_item.long_position, hold_item.short_position][0 if side > 0 else 1])}手," \
        #             f"当前{bk_item.trade_type}:{abs(opt_vol)}手,status={rel}"
        # bk_code = self.bk_info.get_bk_from_breed(breed)
        # if not bk_code:
        #     return
        # investor_msg = f"账号：{self.trader_io.dd_account.dest_investor}"
        # code_msg = f"[{bk_code}].{hold_item.contract}"
        # dest_time = f"指数K线时间:{kline.trade_time}"
        # self.wx.send_wx_opt_msg(investor_msg, code_msg, hold_info, dest_time, self.dest_wx_send)
        return

    def opt_clear_position(self, hold_item: HoldItem, mysql, strategy_type="close", msgs="清仓"):
        """清仓"""
        # hold_breed = hold_item.contract.rstrip(string.digits)
        # bk_code = self.bk_info.get_bk_from_breed(hold_breed)
        # if not bk_code:
        #     return
        # if hold_breed + "L6" not in self.params:
        #     return
        # p1: DDParam = self.params[hold_breed + "L6"]
        # p2: DDParam = self.params[hold_breed + "L9"]
        # if not p2.trade_able:   # 不具备交易条件，直接退出
        #     return
        # investor_msg = f"账号：{self.trader_io.dd_account.dest_investor}"
        # code_msg = f"[{bk_code}].{hold_item.contract}"
        # time_msg = f"指数方向：{p1.dd_side},顶底时间:{p1.dd_time},主力方向:{p2.dd_side}," \
        #            f"K线时间：{p2.contract_info.Last_minute_Kline.trade_time}"
        # if abs(hold_item.short_position) > 0:
        #     insert_strategy_opt_position(strategy_type, "close", hold_item.contract, -1,
        #                                  abs(hold_item.short_position), 0,
        #                                  margin=0.0, profit=0.0, opt_status=1,
        #                                  opt_price=self.trader_io.acc.LastPrice(hold_item.contract),
        #                                  kt=p2.contract_info.Last_minute_Kline.trade_time,
        #                                  multiplier=p2.contract_info.Multiplier,
        #                                  opts=self.trader_io.dd_account.contract_opt_positions, mysql=mysql)
        #     rel = self.trader_io.close_and_wait(hold_item, hold_breed, hold_item.short_position)
        #     hold_msg = f"【空头】{msgs}:原有仓位{abs(hold_item.short_position)}手, 当前清仓:{abs(hold_item.short_position)}手"
        #     self.wx.send_wx_opt_msg(investor_msg, code_msg, hold_msg, time_msg, Envs.dest_wx_send)
        #
        # if abs(hold_item.long_position) > 0:
        #     insert_strategy_opt_position(strategy_type, "close", hold_item.contract, 1,
        #                                  abs(hold_item.long_position), 0,
        #                                  margin=0.0, profit=0.0, opt_status=1,
        #                                  opt_price=self.trader_io.acc.LastPrice(hold_item.contract),
        #                                  kt=p2.contract_info.Last_minute_Kline.trade_time,
        #                                  multiplier=p2.contract_info.Multiplier,
        #                                  opts=self.trader_io.dd_account.contract_opt_positions, mysql=mysql)
        #     rel = self.trader_io.close_and_wait(hold_item, hold_breed, hold_item.long_position)
        #     hold_msg = f"【多头】{msgs}:原有仓位{abs(hold_item.long_position)}手, 当前清仓:{abs(hold_item.long_position)}手"
        #     self.wx.send_wx_opt_msg(investor_msg, code_msg, hold_msg, time_msg, Envs.dest_wx_send)
        return
