FROM maven:3-openjdk-8 as builder

ENV HOME=/home/usr/app
RUN mkdir -p $HOME
WORKDIR $HOME
ADD pom.xml $HOME
RUN mvn -T4 verify clean --fail-never
ADD . $HOME
RUN mvn -Dmaven.test.skip=true package

FROM openjdk:jdk-oraclelinux8
COPY run.sh /run.sh
COPY --from=builder /home/usr/app/gateway/target/bigdata-dataservice-gateway.jar /dataservice/gateway/
COPY --from=builder /home/usr/app/quoto-config/target/bigdata-dataservice-quoto-config.jar /dataservice/quoto-config/
COPY --from=builder /home/usr/app/quoto-roc/target/bigdata-dataservice-quoto-roc.jar /dataservice/quoto-roc/