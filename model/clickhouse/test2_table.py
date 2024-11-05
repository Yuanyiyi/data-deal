import pandas as pd

from .conn import ClickHose
from config import Log as logger
from .conn import NotExist,Error


class Test2CK:
    __slots__ = (
        "__ck",
        "__table",
    )
    __ck: ClickHose
    __table: str

    def __init__(self, ck: ClickHose):
        self.__ck = ck
        self.__table = "db.test2"
        if not self.check_table():
            raise NotExist(f"not exist table: {self.__table}")

    def check_table(self)->bool:
        try:
            table_exists = self.__ck.query(f"EXISTS TABLE {self.__table}")[0][0]
            return table_exists
        except Exception as e:
            logger.error(f"check table: {self.__table} error: {e}")
            raise Error(e)

    def get_bill(self, cloud: str, region: str, start_time: int, end_time: int):
        query = f"select * from {self.__table} where (cloud = {cloud}) AND (region = {region}) AND (time > {start_time}) AND (time < {end_time})"
        try:
            data = self.__ck.query(query)
            return data
        except Exception as e:
            logger.error(f"get_bill from table: {self.__table} error: {e}")

    def insert_batch(self, month: str, df: pd.DataFrame):
        if df is None or len(df) < 1:
            return
        try:
            # 定义每批次插入的行数
            batch_size = 1000
            query = self.get_insert_sql(df)
            # 分批次插入数据
            for start in range(0, len(df), batch_size):
                end = min(start + batch_size, len(df))
                tuples = [tuple(x) for x in df[start:end].to_numpy()]
                sql = f"{query}" + ', '.join(map(str, tuples))
                self.__ck.insert(sql)
            logger.info(f"insert month: {month} data success")
        except Exception as e:
            logger.error(f"insert_batch from table: {self.__table}, sql: {sql} error: {e}")

    def insert_dataframe(self, month: str, df: pd.DataFrame):
        if df is None or len(df) < 1:
            return
        try:
            # 定义每批次插入的行数
            batch_size = 1000
            cols = df.columns.tolist()
            query = f"INSERT INTO {self.__table} ({','.join(map(str, cols))}) VALUES "
            self.__ck.insert_dataframe(query, df, settings={'max_insert_block_size': batch_size})
            logger.info(f"insert month: {month} data success")
        except Exception as e:
            logger.error(f"insert_batch from table: {self.__table}, sql: {query} error: {e}")

    def get_insert_sql(self, df: pd.DataFrame) -> str:
        cols = df.columns.tolist()
        query = f"INSERT INTO {self.__table} ({','.join(map(str, cols))}) VALUES "
        logger.debug(f"get_insert_sql: {query}")
        return query