# -*- coding: utf-8 -*-
"""
@author: luhx
@file: strategy_main.py
@time: 2023/5/31 21:27
@desc:
"""
import sys
import traceback

from highlow.envs import Envs
from highlow import envs
from highlow import helper, algo
from strategy_imp import StrategyImp
from kds_util.user_logbook import init_logger as init_logs, user_log as logger


def main_kbase(n_days=120, n_minute_days=2, stime="", etime="", codes=[]):
    xj = StrategyImp(Envs.investor_id, n_days, n_minute_days, stime=stime, etime=etime, codes=codes)
    xj.runallklines()
    return


# 以下部分是示例
if __name__ == '__main__':
    try:
        if len(sys.argv) < 2:
            raise "参数len(sys.argv) < 2, 必须提供investor_id"
        Envs.investor_id = sys.argv[1]
        envs.init_envs(Envs.investor_id)
        init_logs(path="./log", name=f"strategy_{Envs.investor_id}", level="INFO")
        pdays = helper.init_ths_trading_day("192.168.1.100")
        if Envs.dest_play_back:
            trading_days = algo.init_trade_day(pdays, Envs.dest_play_start_time)
            start_time = Envs.dest_play_start_time
            end_time = Envs.dest_play_end_time
        else:
            trading_days = algo.init_trade_day(pdays, "")
            start_time = ""
            end_time = ""
        main_kbase(n_days=120, n_minute_days=2, stime=start_time,
                   etime=end_time, codes=Envs.dest_codes)

    except Exception as ex:
        logger.error(str(ex))
        logger.error(traceback.format_exc())
