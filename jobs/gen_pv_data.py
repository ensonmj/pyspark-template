#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
#
# Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
#
########################################################################
"""
File:         gen_pv_data.py
Date:         2020/05/15 13:44:25
Description:  产出基础询量数据
"""
import re
import sys
import time
from datetime import datetime, timedelta
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T

from dependencies.spark import start_spark
from dependencies.spark import del_hdfs_path
# from dependencies.sign import creat_sign_fs64


control_chars = ''.join(map(unichr, range(0, 32) + range(127, 160)))
control_char_re = re.compile('[%s]' % re.escape(control_chars))


def remove_control_chars(line):
    """
    去掉字符串中所有的不可见字符
    """
    return control_char_re.sub('', line)


tag_schema = T.StructType([
    T.StructField("province", T.StringType(), False),
    T.StructField("city", T.StringType(), False),
    T.StructField("tags", T.StringType(), False)
])


def tag_ori(rg, oi, user_orient):
    """
    用户画像标签解析
    """
    tag_set = set()
    try:
        province = "0"
        city = "0"
        if rg is not None and "_" in rg:
            province, city = rg.split("_", 1)

        if oi is not None:
            tag_set.add("19_%s" % oi)

        if user_orient not in [None, '']:
            for tag in user_orient.split(","):
                if "_" in tag:
                    prefix = tag.split("_", 1)[0]
                    if prefix in ["12", "1", "13", "14", "15", "16", "17", "8"]:
                        tag_set.add(tag)
        return T.Row('province', 'city', 'tags')(province, city, ",".join(sorted(tag_set, key=lambda x: map(int, x.split("_")))))
    except Exception as err:
        return T.Row('province', 'city', 'tags')("", "", "")


tag_ori_udf = F.udf(tag_ori, tag_schema)


def date_encode(date):
    """
    日期编码
    """
    t = time.strptime(date, '%Y%m%d')
    wday = t.tm_wday
    return 1 << wday


def date_merge(dates):
    res =  0
    for date in dates:
        res |= date
    return res


def extract_data(spark):
    """Load data
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """

    sql = \
    """
    select
        event_srcid, cuid, oi, rg, user_orient, log_time, query, event_day
    from
        udw_ns.default.brand_xuzhang_bp_adretrieval_log
    where
        event_day >='%s' and event_day <='%s'
        and event_srcid = '735'
        and cuid != ''
        and log_time != ''
    """ % (begin_date, end_date)
    return spark.sql(sql)


def transform_data(spark, df):
    """Transform original dataset.

    :param spark: Spark session object.
    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    """

    # event_srcid, cuid, oi, rg, user_orient, log_time, query, event_day
    df_ori_data = (
        df
        .withColumn('event_day', F.udf(date_encode, T.IntegerType())('event_day'))
        # .withColumn('log_time', F.unix_timestamp('log_time','yyyy/MM/dd HH:mm'))
        .cache()
    )
    # df_ori_data.explain()
    # df_ori_data.show(truncate=False)

    # user_tags
    w = Window.partitionBy('event_srcid', 'cuid').orderBy(F.desc('log_time'))
    df_user_tags = (
        df_ori_data
        .withColumn("row", F.row_number().over(w))
        .where("row=1")
        .withColumn('tmp', tag_ori_udf('rg', 'oi', 'user_orient'))
        .select('event_srcid', 'cuid', 'tmp.province', 'tmp.city', 'tmp.tags')
    )
    # df_user_tags.explain()
    # df_user_tags.show(truncate=False)

    # weekly_uv_pv
    df_weekly_uv_pv = (
        df_ori_data
        .groupBy('event_srcid', 'cuid')
        .agg(F.countDistinct('event_day').alias('wuv'), F.count('event_day').alias('wpv'))
    )
    # df_weekly_uv_pv.explain()
    # df_weekly_uv_pv.show(truncate=False)

    # weekly_pv and query_info
    df_query_info = (
        df_ori_data
        .withColumn('query', F.udf(remove_control_chars)('query'))
        .select('event_srcid', 'cuid', F.explode(F.split('query', ',')).alias('query'), 'event_day')
        .where('query!="" and query!=" " and query!="  "')
        # .withColumn('query', F.udf(creat_sign_fs64)('query'))
        .groupBy('event_srcid', 'cuid', 'query')
        .agg(F.udf(date_merge, T.IntegerType())(F.collect_list('event_day')).alias('query_wau'))
        .withColumn('query_info', F.concat_ws('\002', 'query', 'query_wau'))
        .groupBy('event_srcid', 'cuid')
        .agg(F.concat_ws('\001', F.collect_list('query_info')).alias('query_info'))
    )
    # df_query_info.explain()
    # df_query_info.show(truncate=False)

    df_transformed = (
        df_weekly_uv_pv
        .join(df_user_tags, ['event_srcid', 'cuid'], 'left')
        .join(df_query_info, ['event_srcid', 'cuid'], 'left')
        .select(df_weekly_uv_pv.event_srcid, df_weekly_uv_pv.cuid,
                df_user_tags.province,
                df_user_tags.city,
                df_user_tags.tags,
                df_weekly_uv_pv.wuv,
                df_weekly_uv_pv.wpv,
                df_query_info.query_info)
        .fillna('', 'query_info')
        .select(F.concat_ws('\t',
                   'event_srcid', 'cuid', 'province', 'city',
                   'tags', 'wuv', 'wpv', 'query_info').alias('res'))
    )
    # df_transformed.explain()
    # df_transformed.show(truncate=False)

    return df_transformed


def load_data(df, output_path):
    """Collect data.

    :param df: DataFrame to print.
    :param output_path: output file path
    :return: None
    """
    (
        df
        .repartition(100)
        .write
        .mode('overwrite')
        .text(output_path)
    )
    return None


def main(begin_date, end_date):
    """
    品牌序章手百用户分析spark作业
    """
    spark, log, config = start_spark(
        app_name="brand_xuzhang_gen_pv_data_%s_%s" % (begin_date, end_date),
        master='yarn',
        spark_config= {
            "spark.yarn.queue": "brand",
            "spark.shuffle.dce.enable": "true",
            "spark.executor.memory": "8g",
            "spark.executor.cores": 1,
            "spark.executor.instances": 500,
            "spark.default.parallelism": 1000,
            "spark.sql.shuffle.partitions": 1000,
        })

    output_path = "/app/ecom/brand/majian06/moirai/gen_pv_data/%s-%s" % (begin_date, end_date)

    # execute ETL pipeline
    log.warn('job etl is up-and-running')
    data = extract_data(spark)
    data_transformed = transform_data(spark, data)
    load_data(data_transformed, output_path)

    # log the success and terminate Spark application
    log.warn('job etl is finished')
    spark.stop()
    return None


if __name__ == "__main__":
    if len(sys.argv) < 1:
        print "Param error"
        sys.exit(1)

    end_date = sys.argv[1]
    N = 0
    if len(sys.argv) > 2:
        N = int(sys.argv[2]) - 1
    begin_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=N)).strftime('%Y%m%d')

    main(begin_date, end_date)

    sys.exit(0)
