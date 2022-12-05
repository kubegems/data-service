FROM harbor.cloudminds.com/gitlab-ci/openjdk:jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=markdown-manage

WORKDIR /opt/bigdata-service/markdown-manage

COPY target/markdown-manage.jar /opt/bigdata-service/markdown-manage/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/opt/bigdata-service/markdown-manage.jar","--spring.profiles.active=prod"]
