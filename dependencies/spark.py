"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import __main__

from os import environ, listdir, path
import json
from py4j.java_gateway import java_import
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    This function also looks for a file ending in '.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for config.

    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set (e.g.
    setting `DEBUG=1` as an environment variable as part of a debug
    configuration within an IDE such as Visual Studio Code or PyCharm.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

    # general config
    spark_builder.config("spark.driver.memory", "10g")
    spark_builder.config("spark.driver.cores", "4")
    spark_builder.config("spark.driver.extraJavaOptions",
                        "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -XX:+UseG1GC")
    spark_builder.config("spark.executor.extraJavaOptions",
                        "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -XX:+UseG1GC")
    spark_builder.config("spark.yarn.maxAppAttempts", 1)
    spark_builder.config("spark.blacklist.enabled", "true")
    spark_builder.config("spark.network.timeout", 300)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.enableHiveSupport().getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict


def del_hdfs_path(spark_sess, path, recursively=True):
    """ Delete hdfs path
    Args:
        spark_sess: SparkSession
        path: the path to delete
        recursively: delete data recursively
    """
    sc = spark_sess.sparkContext
    java_import(sc._jvm, "org.apache.hadoop.fs.Path")
    hdfs_path = sc._jvm.Path(path)
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = hdfs_path.getFileSystem(hadoop_conf)
    fs.delete(hdfs_path, recursively)
