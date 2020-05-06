JOB=${1-"etl"}

${SPARK_HOME}/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --py-files packages.zip \
    --files configs/${JOB}.json \
    jobs/${JOB}.py ${@:2}
