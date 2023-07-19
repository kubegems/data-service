FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=dataservice-common

WORKDIR /dataservice/dataservice-common

COPY run.sh /run.sh

COPY target/dataservice-common.jar /dataservice/dataservice-common/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/dataservice-common/dataservice-common.jar","--spring.profiles.active=prod"]
