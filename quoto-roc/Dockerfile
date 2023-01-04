FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=quoto-roc

WORKDIR /dataservice/quoto-roc

COPY run.sh /run.sh

COPY target/bigdata-dataservice-quoto-roc.jar /dataservice/quoto-roc/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/quoto-roc/bigdata-dataservice-quoto-roc.jar","--spring.profiles.active=prod"]
