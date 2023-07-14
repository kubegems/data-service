**安装前说明：**
* 确保已安装hadoop(推荐 cdh6.1.1 +) 相关集群环境包括（hdfs、Hive、Spark、Yarn、Oozie （5.0.0+） ）以及python2.7
* 确定已安装mysql （5.7+）、redis（5.0.12） 、clickhouse、nacos （2.2.3+）、JDK 1.8、MAVEN 3.5

## 编译安装数据服务

---

**编译部署：**

> git clone https://github.com/kubegems/data-service.git
>
> cd ./bigdata-dataservice
>
> mvn package
>
> mvn -X clean package assembly:single
>
> copy ./bin/bigdata-dataservice-bin.tar.gz  .
>
> tar xzvf  bigdata-dataservice-bin.tar.gz

**初始化mysql数据库：**

>`source /path/to/``mysql_bigdata_dataservice_ddl.sql``;`

初始化clickhouse数据库

>`clickhouse_ddl.sql`

**配置服务：**

cd ./conf

编辑 bigdata-dataservice-quoto-chatbot.yml  bigdata-dataservice-quoto-roc.yml  bigdata-dataservice-quoto-config.yml  bigdata-dataservice-quoto-search.yml

配置涉及到的 mysql 、ck、redis、nacos 相关的配置

**启动服务：**

./sbin/dataservice-start-all.sh


## bigdata-dataservice
 apijson  版本为4.6.0  开源代码最后一次的提交日期为2020-02-06
 apijson-framework 版本为4.6.0  开源代码最后一次的提交日期为2020-02-01  

port gateway-8001 roc-8070 config-8071 chatbot-8072 standardManage-8073 quotoManage-8074 
markdown-manage-8075 search-8076 label-manage 8077

