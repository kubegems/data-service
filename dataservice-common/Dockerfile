FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=dataserver-common

WORKDIR /dataservice/dataserver-common

COPY run.sh /run.sh

COPY target/bigdata-dataservice-quoto-config.jar /dataservice/dataserver-common/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/dataserver-common/dataserver-common.jar","--spring.profiles.active=prod"]
