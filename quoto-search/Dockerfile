FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=quoto-search

WORKDIR /dataservice/quoto-search/

COPY run.sh /run.sh

COPY target/bigdata-dataservice-quoto-search.jar /dataservice/quoto-search/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/quoto-search/bigdata-dataservice-quoto-search.jar","--spring.profiles.active=prod"]
