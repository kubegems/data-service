FROM harbor.cloudminds.com/library/openjdk:15.0.2-jdk-oraclelinux8

ENV PROJECT=bigdata-service MODULE=quoto-chatbot

WORKDIR /dataservice/quoto-chatbot/

COPY run.sh /run.sh

COPY target/bigdata-dataservice-quoto-chatbot.jar /dataservice/quoto-chatbot/

CMD ["java","-jar","-Duser.timezone=Asia/Shanghai","/dataservice/quoto-chatbot/bigdata-dataservice-quoto-chatbot.jar","--spring.profiles.active=prod"]
