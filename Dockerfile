FROM openjdk:8-alpine

ENV SPARK_VER=2.4.0
ENV HADOOP_VER=2.7
ENV SPARK_HOME=/usr/spark

RUN apk --update add wget tar bash \
&& wget http://mirror.downloadvn.com/apache/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-hadoop${HADOOP_VER}.tgz \
&& tar -xvzf spark-${SPARK_VER}-bin-hadoop${HADOOP_VER}.tgz \
&& mv spark-${SPARK_VER}-bin-hadoop${HADOOP_VER} $SPARK_HOME


COPY template.sh /

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_MAIN_CLASS Main
ENV SPARK_MASTER_URL local[*]

# Copy the build.sbt first, for separate dependency resolving and downloading
#ONBUILD COPY build.sbt /scala_example/
#ONBUILD COPY project /scala_example-mearuse/project
#ONBUILD RUN sbt update

# Copy the source code and build the application
#ONBUILD COPY . /scala_example
#ONBUILD RUN sbt clean package
COPY target/scala-2.11/scala_example_2.11-0.1.jar /target/

RUN chmod +x template.sh \
&& mkdir data \
&& chmod -R 777 data/ \
&& ls -la

CMD ["./template.sh"]