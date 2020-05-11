JOB=${1-"etl"}

SPARK="${SPARK_HOME}/bin/spark-submit"
ARGS="--master yarn \
    --deploy-mode cluster \
    --py-files packages.zip"

if [ -f "configs/${JOB}.json" ]; then
    ARGS="${CLI} --files configs/${JOB}.json"
fi

${SPARK} ${ARGS} "jobs/${JOB}.py" "${@:2}"
