from typing import (
    Optional,
    Dict
)

import sys
import os
import configparser
from pathlib import Path
import logging


class Error(Exception):
    pass


class ConfigError(Error):
    pass


class MysqlConf:
    __slots__ = (
        "__host",
        "__user",
        "__password",
        "__database",
    )

    __host: str
    __user: int
    __password: str
    __database: str

    def __init__(self, mysql: Dict = None):
        try:
            self.__host = mysql["Host"]
        except KeyError:
            raise ConfigError("mysql Host not found.")

        try:
            self.__database = mysql["Database"]
        except KeyError:
            raise ConfigError("mysql database not found.")

        try:
            self.__user = mysql["User"]
        except KeyError:
            raise ConfigError("mysql User not found.")

        try:
            self.__password = mysql["Password"]
        except KeyError:
            raise ConfigError("mysql Password not found.")

    @property
    def host(self) -> str:
        return self.__host

    @property
    def database(self) -> int:
        return self.__database

    @property
    def user(self) -> str:
        return self.__user

    @property
    def password(self) -> str:
        return self.__password


class ClickHouseConf:
    __slots__ = (
        "__ck_host",
        "__ck_port",
        "__ck_user",
        "__ck_password",
    )

    __ck_host: str
    __ck_port: int
    __ck_user: str
    __ck_password: str

    def __init__(self, clickhouse: Dict = None):
        try:
            self.__ck_host = clickhouse["Host"]
        except KeyError:
            raise ConfigError("clickhouse Host not found.")

        try:
            self.__ck_port = int(clickhouse["Port"])
        except KeyError:
            raise ConfigError("clickhouse Port not found.")

        try:
            self.__ck_user = clickhouse["User"]
        except KeyError:
            raise ConfigError("clickhouse User not found.")

        try:
            self.__ck_password = clickhouse["Password"]
        except KeyError:
            raise ConfigError("clickhouse Password not found.")

    @property
    def ck_host(self) -> str:
        return self.__ck_host

    @property
    def ck_port(self) -> int:
        return self.__ck_port

    @property
    def ck_user(self) -> str:
        return self.__ck_user

    @property
    def ck_password(self) -> str:
        return self.__ck_password


class LogConf:
    __slots__ = (
        "__log_path",
        "__log_level"
    )

    __log_path: Path
    __log_level: str

    def __init__(self, log: Dict = None):
        try:
            self.__log_path: Path = Path(log["Path"])
        except KeyError:
            raise ConfigError("Log path not found.")
        else:
            if self.__log_path.is_dir():
                raise ConfigError("Invalid log path.")

        try:
            log_level: str = log["Level"].upper()
        except KeyError:
            raise ConfigError("Log level not found.")
        else:
            try:
                self.__log_level = log_level
            except KeyError:
                raise ConfigError("Invalid log level.")

    @property
    def log_path(self) -> Path:
        return self.__log_path

    @property
    def log_level(self) -> str:
        return self.__log_level


class Server:
    __slots__ = (
        "__first",
    )

    __first: bool

    def __init__(self, server: Dict = None):
        try:
            self.__first: Path = bool(Path(server["First"]))
        except KeyError:
            raise ConfigError("server path not found.")

    @property
    def first(self) -> str:
        return self.__first


class Config:
    __slots__ = (
        "__mysql",
        "__ck",
        "__log",
        "__server"
    )

    __ck: ClickHouseConf
    __mysql: MysqlConf
    __log: LogConf
    __server: Server

    def __init__(self, path: Optional[Path] = None):
        # # 获取当前文件的绝对路径
        # current_file_path = os.path.abspath(__file__)
        # # 获取当前文件的目录路径
        # current_file_directory = os.path.dirname(current_file_path)
        # # 获取项目目录（假设当前文件位于项目的子目录中）
        # project_directory = os.path.dirname(current_file_directory)
        # path = project_directory + path

        if path is None:
            path = Path(sys.argv[0]).with_suffix(".ini")

        ini: configparser.ConfigParser = configparser.ConfigParser(
            delimiters='=',
            comment_prefixes=';',
            interpolation=None
        )

        try:
            if not ini.read(path):
                raise ConfigError(f"Configuration not found from path: {path}")

            try:
                mysql: configparser.SectionProxy = ini["Mysql"]
            except KeyError:
                raise ConfigError("mysql section not found.")
            else:
                self.__mysql = MysqlConf(mysql)

            try:
                clickhouse: configparser.SectionProxy = ini["Clickhouse"]
            except KeyError:
                raise ConfigError("clickhouse section not found.")
            else:
                self.__ck = ClickHouseConf(clickhouse)

            try:
                log: configparser.SectionProxy = ini["Log"]
            except KeyError:
                raise ConfigError("Log section not found.")
            else:
                self.__log: LogConf = LogConf(log)

            try:
                server: configparser.SectionProxy = ini["Server"]
            except KeyError:
                raise ConfigError("Server section not found.")
            else:
                self.__server: Server = Server(server)
        except configparser.Error:
            raise ConfigError("Invalid configuration.")

    def log(self):
        return self.__log

    def ck_conf(self):
        return self.__ck

    def mysql_conf(self):
        return self.__mysql

    def server(self):
        return self.__server


class Logger:
    __slots__ = (
        "__path",
        "__level",
        "_logger",
    )

    __path: Path
    _logger: logging.Logger

    def __init__(self, path: Path, level: str):
        self.__path = path
        self._logger = logging.getLogger('unicost_logger')
        # 设置采集的日志级别
        self._logger.setLevel(level)
        # 采取终端输出
        stream_handler = logging.StreamHandler()
        # 设置采集的日志级别
        stream_handler.setLevel(level)
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self._logger.addHandler(stream_handler)

    def logger(self) -> logging.Logger:
        return self._logger
