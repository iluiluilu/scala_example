#!/bin/bash

SPARK_APPLICATION_JAR_LOCATION=`find /target -iname '*_example_*.jar' | head -n1`
export SPARK_APPLICATION_JAR_LOCATION

if [ -z "$SPARK_APPLICATION_JAR_LOCATION" ]; then
	echo "Can't find a file *_example_*.jar in /app/target"
	exit 1
fi

/usr/spark/bin/spark-submit \
        --conf spark.driver.extraClassPath=/sparklib/mysql-connector-java-5.1.46.jar \
        --conf spark.driver.extraClassPath=/sparklib/mysql-connector-java-8.0.12.jar \
        --class ${SPARK_APPLICATION_MAIN_CLASS} \
        --master ${SPARK_MASTER_URL} \
        ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_APPLICATION_ARGS}
