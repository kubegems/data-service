FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=quoto-manage

WORKDIR /dataservice/quoto-manage

COPY run.sh /run.sh

COPY target/bigdata-dataservice-quoto-manage.jar /dataservice/quoto-manage/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/quoto-manage/bigdata-dataservice-quoto-manage.jar","--spring.profiles.active=prod"]
