FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=quoto-config

WORKDIR /dataservice/quoto-config

COPY run.sh /run.sh

COPY target/bigdata-dataservice-quoto-config.jar /dataservice/quoto-config/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/quoto-config/bigdata-dataservice-quoto-config.jar","--spring.profiles.active=prod"]
