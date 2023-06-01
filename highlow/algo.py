# -*- coding: utf-8 -*-
"""
@funcs: 算法相关
"""
from kds_util.user_logbook import user_log as logger
from typing import Dict, List, Any
# from highlow.dd_obj import BreedItem, TBreed, HoldItem, TBkId, BkItem, ESignalType, DDParam
from highlow.envs import Envs
from datetime import datetime, timedelta


def calc_new_position_from_open_position(account, contract, price, trade_profit, open_position, breed_side: int) -> int:
    """
    根据浮动盈亏，初始持仓，计算期待的持仓
    返回的期待持仓数与hold_position一致
    """
    margin = account.calc_one_margin(contract, price)
    vol = int(trade_profit // margin + abs(open_position))
    if vol < 0:  # 如果亏掉的手数比原始持仓还大，则当前的仓位应该清空成0
        return 0  # 0表示清仓
    return vol * breed_side  # 否则反回当前应有的仓位，正数表示多头，负数表示空头


def calc_increment_position(expected_vol: int, h: Any):
    """
    根据预期仓位，计算出实际要开或是要平的增量仓位, 返回值str为"0"表示开仓，为"1"表示平仓
    """
    if expected_vol > 0:
        if abs(expected_vol) == abs(h.long_position):
            return 0, ""
        if abs(expected_vol) > abs(h.long_position):
            return abs(abs(expected_vol) - abs(h.long_position)), "0"
        if abs(expected_vol) < h.long_position:
            return abs(abs(expected_vol) - abs(h.long_position)), "1"
    if expected_vol < 0:
        if abs(expected_vol) == abs(h.short_position):
            return 0, ""
        if abs(expected_vol) > abs(h.short_position):
            return -abs(abs(expected_vol) - abs(h.short_position)), "0"
        if abs(expected_vol) < abs(h.short_position):
            return -abs(abs(expected_vol) - abs(h.short_position)), "1"
    return 0, ""
#
#
# def find_hold(bk_info, holds: Dict[TBreed, HoldItem], bk_code) -> (HoldItem, TBreed):
#     """通过板块code获取持仓信息"""
#     hold_item, hold_id = None, None
#     for breed in holds.keys():
#         breed_id = breed
#         bk2 = bk_info.get_bk_from_breed(breed)
#         if not bk2:
#             continue
#         if bk_code == bk2:
#             hold_item = holds[breed_id]
#             if hold_item and hold_item.net_position != 0:  # 找到一个有持仓的就退出，否则一直找
#                 break
#     return hold_item


def parse_dd(ding_di_list: List[int], time_list: List[str]) -> (int, str):
    for i in range(-1, -len(ding_di_list) - 1, -1):
        if ding_di_list[i] != 0:
            return ding_di_list[i] * (-1), time_list[i]
    return 0, ""


def get_next_trading_day(pd_days, trading_day: int, day_index=1) -> int:
    """获取下一个交易日, day_index为1表增下一个，为-1表示前一个"""
    index = pd_days[pd_days["trading_days"] <= trading_day].index
    if index.empty:
        return 0
    trading_day = int(pd_days["trading_days"][index[-1] + day_index])
    return trading_day


def is_white_market(trade_day, dt: datetime) -> bool:
    """
    针对给定任意的时间，判断在此时间下是白盘还是夜盘
    @param trade_day:
    @param dt:
    @return:
    """
    if int(dt.strftime("%Y%m%d")) == trade_day:
        if 8 <= dt.hour < 20:
            return True
    return False


def is_kline_dt_white_market(kt: datetime) -> bool:
    """
    针对实际的K线时间，判断白盘还是夜盘
    @param kt:
    @return:
    """
    if 8 <= kt.hour < 20:
        return True
    else:
        return False


def get_kline_trading_day(pd_days, dt: datetime) -> int:
    """
    传入给定的K线时间，返回当前K线时间代表的交易日
    """
    if dt.hour < 8:  # 夜盘中的凌晨
        dt = dt - timedelta(hours=8)
        trade_day = get_next_trading_day(pd_days, int(dt.strftime("%Y%m%d")), day_index=1)
    elif 8 <= dt.hour < 20:
        trade_day = get_next_trading_day(pd_days, int(dt.strftime("%Y%m%d")), day_index=0)
        if trade_day != int(dt.strftime("%Y%m%d")):
            trade_day = get_next_trading_day(pd_days, int(dt.strftime("%Y%m%d")), day_index=1)
    else:
        trade_day = get_next_trading_day(pd_days, int(dt.strftime("%Y%m%d")), day_index=1)
    return trade_day


def calc_margin(price, multiplier, margin_ratio) -> float:
    """
    计算一手的保证金
    :param contract:
    :param price:
    :param multiplier:
    :param margin_ratio:
    :return:
    """
    return round(price * (multiplier * margin_ratio), 2)


def calc_interval_trading_days(pd_days, trading_day: int, end_time: str) -> int:
    """
    给定两个日期，计算间隔的交易日
    :return:
    """
    if end_time:
        end_day = int(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d"))
    else:
        end_day = int(datetime.now().strftime("%Y%m%d"))

    index1 = pd_days[pd_days["trading_days"] <= trading_day].index
    index2 = pd_days[pd_days["trading_days"] >= end_day].index
    if index1.empty or index2.empty:
        return -1
    r = index2[0] - index1[-1]
    return r


def init_trade_day(pd_days, spec_time="") -> (int, int):
    """
    根据指定时间计算所在交易日
    然后根据所在交易日往前推算一个交易日，表达出从前一个交易日运行到指定交易日结束的逻辑
    """
    if spec_time:
        dt = datetime.strptime(spec_time, "%Y-%m-%d %H:%M:%S")
    else:
        dt = datetime.now()
    cur_trading_day = get_kline_trading_day(pd_days, dt)
    Envs.white_plate_flag = is_white_market(cur_trading_day, dt)  # 判断是白盘还是夜盘
    pre_trading_day = get_next_trading_day(pd_days, cur_trading_day, day_index=-1)
    Envs.cur_trade_day = cur_trading_day
    Envs.prev_trade_day = pre_trading_day
    Envs.bk_end_time = "{:04d}-{:02d}-{:02d} 16:00:00". \
        format(Envs.prev_trade_day // 10000, Envs.prev_trade_day % 10000 // 100, Envs.prev_trade_day % 100)
    Envs.will_end_time = "{:04d}-{:02d}-{:02d} 16:00:00". \
        format(Envs.cur_trade_day // 10000, Envs.cur_trade_day % 10000 // 100, Envs.cur_trade_day % 100)
    logger.info(
        f"给定时间：{dt.strftime('%Y-%m-%d %H:%M:%S')}, 系统当前是:【{['白盘', '夜盘'][0 if Envs.white_plate_flag else 1]}】，"
        f"交易日起点终点[{Envs.prev_trade_day}]---->[{Envs.cur_trade_day}]")
    return Envs.prev_trade_day, Envs.cur_trade_day
#
#
# def is_opt_timing_from_breed(p2: DDParam, dt: datetime) -> bool:
#     """
#     交易操作时间,区分中金所和非中金所，要求针对票盘前15分钟，盘尾15分钟不操作
#     :param dt:
#     :return:
#     """
#     dg_time = dt.hour * 100 + dt.minute
#     if p2.contract_info.ExchangeId == 'CFFEX':  # 如果是中金所
#         if 1500 >= dg_time >= 945:
#             return True
#     else:
#         if 1445 >= dg_time >= 915 or dg_time <= 300 or dg_time >= 2115:
#             return True
#     return False

#
# def is_opt_timing_from_bk(bk_code: TBkId, dt: datetime) -> bool:
#     """
#     交易操作时间,区分中金所和非中金所，要求针对票盘前15分钟，盘尾15分钟不操作
#     :param dt:
#     :return:
#     """
#
#     dg_time = dt.hour * 100 + dt.minute
#     if bk_code == 'jinrongqihuo':  # 如果是金融期货板块
#         if 1500 >= dg_time >= 945:
#             return True
#     else:
#         if 1445 >= dg_time >= 915 or dg_time <= 300 or dg_time >= 2115:
#             return True
#     return False


def is_moving_timing(dt: datetime) -> bool:
    """
    移仓时间定义移仓时间在1125至1130间
    :param dt:
    :return:
    """
    dg_time = dt.hour * 100 + dt.minute
    if 1130 > dg_time >= 1125:
        return True
    return False


def is_big_wave_opt_sell_timing(dt: datetime) -> bool:
    """
    大波动卖出时机定义在下午2点43分
    :param dt:
    :return:
    """
    dg_time = dt.hour * 100 + dt.minute
    if 1445 >= dg_time >= 1443:
        return True
    return False


def is_big_wave_opt_buy_timing(dt: datetime) -> bool:
    """
    大波动买回的操作时机定义在下午2点40分
    :param dt:
    :return:
    """
    dg_time = dt.hour * 100 + dt.minute
    if 1445 >= dg_time >= 1440:
        return True
    return False


def get_kline_len(df, contract, trading_day):
    """
    获取某个合约从某个日期开始k线数据的长度
    :param contract:
    :param trading_day:
    :return:
    """
    # newdf = df[(df["trading_day"] < trading_day) & (df["code"] == contract)].copy()
    return len(df[(df["trading_day"] < trading_day) & (df["code"] == contract)])
