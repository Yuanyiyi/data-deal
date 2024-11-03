# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from config import Configs, Log
from model.clickhouse import ClickHose, Test2CK, Test1CK
from manager.data_manager import DataManager



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    ck_db = ClickHose(Configs.ck_conf())
    try:
        test1_table = Test1CK(ck_db)
        test2_table = Test2CK(ck_db)
        ds_manager = DataManager(test2_table, test1_table)
        ds_manager.exec()
    except Exception as e:
        Log.error(f"error {e}")


