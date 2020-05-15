#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_gen_pv_data.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in gen_pv_data.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import sys
import os
import unittest

from dependencies.spark import start_spark
from jobs.gen_pv_data import transform_data

reload(sys)
sys.setdefaultencoding('UTF-8')

class SparkGenPvDataTests(unittest.TestCase):
    """Test suite for transformation in gen_pv_data.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        # self.spark, _, _ = start_spark(spark_config={"spark.python.profile":"true"})
        self.spark, _, _ = start_spark()

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        arrayData = [
            ('735', '1A72E63B7AD84763120F0BEE9E5993A2|708387340033568', 2, '12_95', '8_70120200,12_27,13_1,14_5,17_2,23_106470', '2020/4/22 09:56', u'西高校1人高烧，宿舍人隔离', '20200422'),
            ('735', '40D9354F1A4E759629DFDE57CABAB7B9|B1A880E600000A', 2, '14_311', '12_5,12_7,12_8,12_10,12_13,12_15,12_16,12_17,12_20,12_21,12_25,12_27,13_1,14_2,15_3,15_5,17_2,23_106497,23_106502,23_106470', '2020/4/22 09:50', u'汽车之家2020货车最新报价,齐昆仑吕嫣然小说,汽车之家2019款价大全', '20200422'),
            ('735', '992CF31141F5519556CC2A3E52E13FA5|194349840525668', 2, '5_51', '12_1,12_8,12_9,12_13,12_17,12_18,12_20,12_21,12_25,12_26,12_27,12_28,13_1,14_4,15_3,15_5,16_2,17_4,23_106470', '2020/4/22 09:58', u'官方回应记者采访遭殴打,华大基因,核酸检测概念股,达安基因,违约国家,首个受疫情冲击而倒下的国家,做完胃镜吃什么东西比较好', '20200422'),
            ('735', 'F0035D1BBA89DB1DDDA776D44F731A2947971C7C0OHBTRRPKDT', 1, '0_0', '', '2020/4/22 09:47', u'"带孩子到哪里验光"', '20200422'),
            ('735', 'F0035D1BBA89DB1DDDA776D44F731A2947971C7C0OHBTRRPKDT', 1, '0_1', '', '2020/4/22 19:47', u'带"孩子"到哪里验光', '20200422'),
            ('735', 'F0035D1BBA89DB1DDDA776D44F731A2947971C7C0OHBTRRPKDT', 1, '0_1', '', '2020/4/23 19:47', u'带孩子到哪里验光', '20200423'),
            ('735', '2FD9C9E40B0E0A9795FC5866C58941D0|925700230232768', 2, '26_211', '12_1,12_4,12_8,12_9,12_10,12_13,12_15,12_16,12_17,12_18,12_20,12_21,12_23,12_25,12_26,12_27,12_28,13_1,14_2,15_1,15_2,16_3,17_4,23_106497,23_106499,23_106500,23_106502', '2020/4/22 09:59', u'', '20200422'),
            ('735', '8C89341E1B6CFFAAF2BB4F4D57992578|0', 2, '32_280', '', '2020/4/22 09:52', u'带孩子到哪里验光,医保卡里的钱会清零吗,医保卡里的钱突然变少了', '20200422'),
            ('735', '35897B890EB09F0E77A4BA2EE772CB3C|454851830367568', 2, '2_408', '23_106499,23_106502', '2020/4/22 09:56', u'', '20200422'),
            ('735', '35897B890EB09F0E77A4BA2EE772CB3C|454851830367568', 2, '2_408', '23_106499,23_106502', '2020/4/22 19:56', u'', '20200422'),
            ('735', '40504BD32DE77815A994C17E36ED359C|VYMLIUGY4', 2, '5_66', '8_70120200,12_9,12_18,13_1,14_3,15_3,17_4,23_106452,23_106455', '2020/4/22 09:52', u'', '20200422'),
            ('735', 'DAD8E52FE8E22CC8C786C14923240806|0', 2, '31_483', '12_1,12_4,12_8,12_13,12_15,12_20,12_21,12_25,12_26,12_27,13_1,14_2,17_4', '2020/4/22 09:45', u'普通话证报考条件,将界2,夹盗高飞,普京收到15亿只中国口罩,,3d开奖结果,我还要来一次,我要再来一次', '20200422'),
            ('735', '5F30C867CA3309720E4DC7CA1FD8907A|353987230083868', 2, '19_62', '8_70120300,12_1,12_8,12_17,12_21,12_25,12_26,12_27,13_2,14_7,15_1,17_3,23_106470', '2020/4/22 09:55', u'高h古代辣文,正宗醉虾的做法,学鸡蛋灌饼,渣王作妃全文免费阅读', '20200422'),
            ('735', '340A4B588A065E5FDEC93A928BB7D97E|0', 2, '33_448', '12_1,12_8,12_17,12_20,12_21,12_25,12_26,13_2,14_2,17_2,23_105956', '05-11 07:30:07', u'', '20200511')
        ]
        df = self.spark.createDataFrame(data=arrayData, schema=['event_srcid', 'cuid', 'oi', 'rg', 'user_orient', 'log_time', 'query', 'event_day'])
        # df.printSchema()
        # df.show()

        # act
        data_transformed = transform_data(self.spark, df)
        # self.spark.sparkContext.show_profiles()

        # assert
        # data_transformed.show(truncate=False)
        import os
        output_path = 'file://' + os.path.realpath("./tests/test_data/gen_pv_data")
        (
            data_transformed
            .repartition(1)
            .write
            .mode('overwrite')
            .text(output_path)
        )


if __name__ == '__main__':
    unittest.main()
