import sys
from pathlib import Path
from .init_conf import Config, Logger, ClickHouseConf, MysqlConf

# 全局：初始化配置参数和日志格式
# Configs = Config("\\unicost\\conf\\unicost_server.ini")
Configs = Config(Path(sys.argv[1].strip()) if len(sys.argv) > 1 else None)
Log = Logger(Configs.log().log_path, Configs.log().log_level).logger()



