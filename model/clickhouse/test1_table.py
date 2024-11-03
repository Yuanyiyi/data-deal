from .conn import ClickHose
from config import Log as logger
from .conn import NotExist,Error


class Test1CK:
    __slots__ = (
        "__ck",
        "__table"
    )
    __ck: ClickHose
    __table: str

    def __init__(self, ck: ClickHose):
        self.__ck = ck
        self.__table = "db.test1"
        if not self.check_table():
            raise NotExist(f"not exist table: {self.__table}")

    def check_table(self)->bool:
        try:
            table_exists = self.__ck.query(f"EXISTS TABLE {self.__table}")[0][0]
            return table_exists
        except Exception as e:
            logger.error(f"check table: {self.__table} error: {e}")
            raise Error(e)

    def get_bill_by_region(self, cloud: str, region: str, start_time: int, end_time: int)->list:
        logger.debug(f"get_bill_by_region, start_time: {start_time}, end_time: {end_time}")
        query = f"select * from {self.__table} where (cloud = '{cloud}') AND (region = '{region}') AND (time > {start_time}) AND (time < {end_time})"
        try:
            data = self.__ck.query(query)
            return data
        except Exception as e:
            logger.error(f"get_bill from table: {self.__table}, sql: {query}, error: {e}")

    def get_bill_by_cloud(self, cloud: str, start_time: int, end_time: int)->list:
        logger.debug(f"get_bill_by_cloud, start_time: {start_time}, end_time: {end_time}")
        query = f"select * from {self.__table} where (cloud = '{cloud}') AND (time > {start_time}) AND (time < {end_time})"
        try:
            data = self.__ck.query(query)
            return data
        except Exception as e:
            logger.error(f"get_bill from table: {self.__table} error: {e}")