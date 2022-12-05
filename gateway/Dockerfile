FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=gateway

WORKDIR /dataservice/gateway

COPY run.sh /run.sh

COPY target/bigdata-dataservice-gateway.jar /dataservice/gateway/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/gateway/bigdata-dataservice-gateway.jar","--spring.profiles.active=prod"]
