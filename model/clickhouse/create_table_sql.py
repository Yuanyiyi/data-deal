"""
存放所有查询语句
"""

class CreateTable:
    # 创建数据库
    CreateDatabase = """
    CREATE DATABASE [IF NOT EXISTS] db [ON CLUSTER cluster_name] [ENGINE = engine]
    """

    # 创建本地表
    CreateMwBillLocalTable = """
        CREATE TABLE IF NOT EXISTS db.test2 on cluster 'cluster'(
            date DateTime64(3, 'Asia/Shanghai'),
            cloud LowCardinality(String),
            product LowCardinality(String),
            productType LowCardinality(String),
            region LowCardinality(String),
            name_number UInt16,
            day_originPrice Float64,
            day_realPrice Float64,
            dd_unique UInt16,
            not_fill UInt16
            )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(date)
        PRIMARY KEY (date, cloud, product, region)
        ORDER BY (date, cloud, product, region, day_realPrice)
        SETTINGS index_granularity = 8192, storage_policy = 'policy_name_1';
    """

    # 创建分布式表
    CreateMwBillCluster = """
        CREATE TABLE db.test2 on cluster 'cluster' (
            date DateTime64(3, 'Asia/Shanghai'),
            cloud LowCardinality(String),
            product LowCardinality(String),
            productType LowCardinality(String),
            region LowCardinality(String),
            name_number UInt16,
            day_originPrice Float64,
            day_realPrice Float64,
            dd_unique UInt16,
            not_fill UInt16
        )
        ENGINE = Distributed('cluster', 'db', 'test2', rand());
        """

    # 创建视图表
    CreateMwBillView = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS db.test2_view on cluster 'cluster'  TO db.test2_view_1 (
            date LowCardinality(String),
            cloud LowCardinality(String),
            product LowCardinality(String),
            productType LowCardinality(String),
            region LowCardinality(String),
            realPrice Float64,
        ) 
        ENGINE = MergeTree
        AS
        SELECT *
        FROM db.test2_cluster
        WHERE 
        ;
    """




