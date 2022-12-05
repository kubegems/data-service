FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=label-manage

WORKDIR /dataservice/label-manage

COPY run.sh /run.sh

COPY target/bigdata-dataservice-label-manage.jar /dataservice/label-manage/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/label-manage/bigdata-dataservice-label-manage.jar","--spring.profiles.active=prod"]
