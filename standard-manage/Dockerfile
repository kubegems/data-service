FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=standard-manage

WORKDIR /dataservice/standard-manage

COPY run.sh /run.sh

COPY target/bigdata-dataservice-standard-manage.jar /dataservice/standard-manage/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/standard-manage/bigdata-dataservice-standard-manage.jar","--spring.profiles.active=prod"]
