# -*- coding: utf-8 -*-
"""
@author: luhx
@file: cfg.py
@time: 2023/5/30 22:43
@desc:
"""
import os
import json


class xjconfig:
    config = {
        "all_acconts": {
            "moni153": {
                "investor_id": "moni153",
                "url": "http://192.168.1.153:8088/api/mgr",
            },
        },
        "headers": {'Content-Type': 'application/json;charset=UTF-8', 'Connection': 'keep-alive'},
        "investor_id": "moni153",
        "kline_url": "http://192.168.1.100:8086",
        "kline_bak_url": "http://192.168.1.101:8086",
        "all_acconts_url": "http://192.168.1.100:8088",
        "dbIP": "192.168.1.100",
    }

    def __init__(self) -> None:
        if os.path.exists("etc/config.json"): self.config.update(json.load(open("etc/config.json", "r")))

        self.config.update(json.load(open("etc/config.json","r"))) if os.path.exists("etc/config.json") else None


def cfg(): return xjconfig().config