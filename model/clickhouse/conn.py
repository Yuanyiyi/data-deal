from clickhouse_driver import Client
from config import Log as logger
from config import ClickHouseConf


class Error(Exception):
    pass


class Connection(Error):
    pass

class NotExist(Error):
    pass


class ClickHose:
    __slots__ = (
        "__ck",
    )

    __ck: Client

    def __init__(self, db_info: ClickHouseConf):
        try:
            self.__ck = Client(
                host=db_info.ck_host,
                port=db_info.ck_port,
                user=db_info.ck_user,
                password=db_info.ck_password,
                settings={'use_numpy': True}
            )
            logger.info("clickhouse connection success")
        except Exception as e:
            logger.error(f"clickhouse connection error: {e}")
            raise Connection(e)

    def get_conn(self) -> Client:
        return self.__ck

    def query(self, sql: str):
        return self.__ck.execute(sql)

    def insert(self, sql: str):
        return self.__ck.execute(sql)

    def insert_dataframe(self, sql: str, df: pd.DataFrame, external_tables=None, query_id=None,
            settings=None):
        return self.__ck.insert_dataframe(sql, df, external_tables, query_id, settings)

    def close(self):
        self.__ck.disconnect()
        logger.info("ck close")
