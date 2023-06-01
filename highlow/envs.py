# -*- coding: utf-8 -*-
"""
@funcs: 全局环境变量，所有配置及全局变量最终集成到这里
"""
import json
import os
import platform
from kds_util.user_logbook import user_log as logger
from typing import List


class Envs:
    data_center_ip: str = ""    # 数据中心ip,由系统传送进来
    investor_id: str = ""   # 账号
    dest_ip: str = ""
    dest_investor: str = ""
    dest_type: str = "fenshi_jx_sx"  # 默认fenshi_jx_sx,否则为mohu
    dest_wx_send: int = 0
    dest_debug: int = 0
    prev_trade_day: int = 0  # 真正交易日的前一个交易日，由最后一个板块日线给出，由于板块日线都是延迟一天即昨天的，因此刚好用来计算真正交易日的前一个交易日
    cur_trade_day: int = 0  # 当前交易日
    bk_end_time: str = ""
    will_end_time: str = ""
    white_plate_flag: bool = False  # 白盘标识
    dest_bk_type: str = "ma5_activate"  # ma5_activate, dd_activate, dd_activate003, 默认ma5_activate
    dest_play_back: int = 0
    dest_play_start_time: str = ""  # 开始时间
    dest_play_end_time: str = ""  # 结束时间
    dest_clear_position_time: str = ""  # 清仓时间
    dest_multi_cycle_flag: int = 0   # 为0表示单周期，为1表示多周期,单周期只取日线级周期，多周期需根据提示取对应周期
    dest_codes: List[str] = []  # 默认为[]
    dest_index_maximum_period: int = 0  # 默认为0，表示指数信号不受周期限制，为3表示3周期内的信号有效果
    dest_big_wave_swings: int = 0   # 默认为0，表示不支持大波动相关操作，若设为0，表示与大波动相关的都不进行处理
    dest_force_stop_win: str = ""  # 默认为空，ma5表示板块ma5 作为清仓条件

class JsonConf:
    def __init__(self, file_name):
        self.file_name = file_name
        self._data = self.load_file(file_name)

    def get(self, arge):
        return self._data[arge]

    def load_file(self, file_name):
        if not os.path.exists(file_name):
            return {}
        with open(file_name, encoding="utf-8") as json_file:
            try:
                data = json.load(json_file)
            except Exception as e:
                print(e)
                data = {}
        return data


def init_envs(investor_id):
    if platform.system().lower() == 'windows':
        config_path = "F:/work/aiqt/etc"
    else:
        config_path = "/opt/kds/work/etc"
    relative_config_path = "./etc"
    config_path = os.path.abspath(relative_config_path)
    config_file = f"{config_path}/{investor_id}_config.json"
    try:
        Envs.investor_id = investor_id
        Envs.dest_investor = investor_id
        Envs.dest_type = JsonConf(f"{config_file}").get("dest_type")
        try:
            Envs.dest_wx_send = JsonConf(f"{config_file}").get("dest_wx_send")
        except:
            pass
        try:
            Envs.dest_debug = JsonConf(f"{config_file}").get("dest_debug")
        except:
            pass
        try:
            Envs.dest_bk_type = JsonConf(f"{config_file}").get("dest_bk_type")
        except:
            pass
        try:
            Envs.dest_play_back = JsonConf(f"{config_file}").get("dest_play_back")
        except:
            pass
        try:
            Envs.dest_play_start_time = JsonConf(f"{config_file}").get("dest_play_start_time")
        except:
            pass
        try:
            Envs.dest_play_end_time = JsonConf(f"{config_file}").get("dest_play_end_time")
        except:
            pass
        try:
            Envs.dest_clear_position_time = JsonConf(f"{config_file}").get("dest_clear_position_time")
        except:
            pass
        try:
            Envs.dest_multi_cycle_flag = JsonConf(f"{config_file}").get("dest_multi_cycle_flag")
        except:
            pass
        try:
            Envs.dest_codes = JsonConf(f"{config_file}").get("dest_codes")
        except:
            pass
        try:
            Envs.dest_index_maximum_period = JsonConf(f"{config_file}").get("dest_index_maximum_period")
        except:
            pass
        try:
            Envs.dest_big_wave_swings = JsonConf(f"{config_file}").get("dest_big_wave_swings")
        except:
            pass
        try:
            Envs.dest_force_stop_win = JsonConf(f"{config_file}").get("dest_force_stop_win")
        except:
            pass
    except Exception as exception:
        logger.info(str(exception))


# init_envs()
