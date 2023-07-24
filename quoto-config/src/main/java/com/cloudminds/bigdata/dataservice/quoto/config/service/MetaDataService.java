package com.cloudminds.bigdata.dataservice.quoto.config.service;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.config.mapper.MetaDataTableMapper;
import com.linkedin.common.*;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.azkaban.AzkabanJobType;
import com.linkedin.dataset.*;
import com.linkedin.domain.Domains;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class MetaDataService {
    @Autowired
    private MetaDataTableMapper metaDataTableMapper;
    @Autowired
    private RestEmitter emitter;
    private String hdfsInstance = "prod_hdfs";
    private String workFlowPlat = "oozie";
    @Value("${hiveUrl}")
    private String hiveUrl;
    @Value("${hiveUser}")
    private String hiveUser;
    @Value("${hivePassword}")
    private String hivePassword;
    @Value("${hiveMetastoreUrl}")
    private String hiveMetastoreUrl;
    @Value("${hiveMetastoreUser}")
    private String hiveMetastoreUser;
    @Value("${hiveMetastorePassword}")
    private String hiveMetastorePassword;
    @Value("${ckUrl}")
    private String ckUrl;
    @Value("${ckUser}")
    private String ckUser;
    @Value("${ckPassword}")
    private String ckPassword;
    @Value("${datahubToken}")
    private String datahubToken;
    @Value("${datahubGraphqlUrl}")
    private String datahubGraphqlUrl;
    @Autowired
    RestTemplate restTemplate;

    public CommonResponse addTable(MetaDataTable metaDataTable) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (metaDataTable == null || StringUtils.isEmpty(metaDataTable.getName()) || StringUtils.isEmpty(metaDataTable.getDatabase_name()) || StringUtils.isEmpty(metaDataTable.getStorage_format())
                || StringUtils.isEmpty(metaDataTable.getModel_level()) || StringUtils.isEmpty(metaDataTable.getData_domain())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表名,库名,存储格式,模型层级,数据域");
            return commonResponse;
        }
        //判断名字是否重复
        if (metaDataTableMapper.findMetaDataTableByName(metaDataTable.getDatabase_name(), metaDataTable.getName(), metaDataTable.getTable_type()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表已经存在了,请不要重复添加");
            return commonResponse;
        }

        if (metaDataTable.getTable_type() == 1) {
            //非默认存储位置需要传入存储位置
            if (!metaDataTable.isSystem_storage_location()) {
                if (StringUtils.isEmpty(metaDataTable.getStorage_location())) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("非默认存储位置,存储位置不能为空");
                    return commonResponse;
                }
            }
            if (StringUtils.isEmpty(metaDataTable.getDdl())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("ddl不能为空");
                return commonResponse;
            }

            //非默认分隔符需要传入分隔符
            if (!metaDataTable.isSystem_delimiter()) {
                if (StringUtils.isEmpty(metaDataTable.getDelimiter())) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("非默认分隔符,分隔符不能为空");
                    return commonResponse;
                }
            }
        }
        //ck表的话,去ck创建表
        if (metaDataTable.getTable_type() == 2) {
            if (!metaDataTable.getStorage_format().equals("ReplicatedMergeTree")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("引擎只支持ReplicatedMergeTree");
                return commonResponse;
            }
            if (metaDataTable.getPartition_field() == null || metaDataTable.getPartition_field().size() == 0 || metaDataTable.getPartition_field().size() > 1) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("分区必须有值,且必须只有一个值");
                return commonResponse;
            }
            if (metaDataTable.getOrder_field() == null || metaDataTable.getOrder_field().length == 0) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("排序的字段不能未空");
                return commonResponse;
            }
            //去ck创建表
            String[] createSql = createCKTableSql(metaDataTable);
            metaDataTable.setMapping_instance_table(metaDataTable.getName() + "_instance");
            metaDataTable.setDdl(createSql[0] + ";\n" + createSql[1]);
            Connection conn = null;
            PreparedStatement pStemt = null;
            int execute = 0;
            try {
                conn = DriverManager.getConnection(ckUrl, ckUser, ckPassword);
                System.out.println("去ck执行sql:" + createSql[0]);
                pStemt = conn.prepareStatement(createSql[0]);
                pStemt.execute();
                execute++;
                System.out.println("去ck执行sql:" + createSql[1]);
                pStemt = conn.prepareStatement(createSql[1]);
                pStemt.execute();
                execute++;
            } catch (Exception e) {
                e.printStackTrace();
                commonResponse.setSuccess(false);
                commonResponse.setMessage("创建ck表失败：" + e.getMessage());
                if (execute > 0) {
                    try {
                        pStemt = conn.prepareStatement("drop table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + "_instance on cluster cm_ck_cluster");
                        pStemt.execute();
                        if (execute > 1) {
                            pStemt = conn.prepareStatement("drop table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " on cluster cm_ck_cluster");
                            pStemt.execute();
                        }
                    } catch (Exception ee) {
                        ee.printStackTrace();
                        commonResponse.setMessage("创建ck表失败：" + e.getMessage() + " drop table fail:" + ee.getMessage());
                    }
                }
                return commonResponse;
            } finally {
                try {
                    pStemt.close();
                    conn.close();
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        }
        //插入数据库
        if (metaDataTableMapper.insertMetaDataTable(metaDataTable) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表创建失败,请联系管理员");
            //删除ck表
            if (metaDataTable.getTable_type() == 2) {
                Connection conn = null;
                PreparedStatement pStemt = null;
                try {
                    pStemt = conn.prepareStatement("drop table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + "_instance on cluster cm_ck_cluster");
                    pStemt.execute();
                    pStemt = conn.prepareStatement("drop table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " on cluster cm_ck_cluster");
                    pStemt.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                    commonResponse.setMessage("mysql表创建失败" + " drop ck table fail:" + e.getMessage());
                } finally {
                    try {
                        pStemt.close();
                        conn.close();
                    } catch (Exception ee) {
                        ee.printStackTrace();
                    }

                }
            }
            return commonResponse;
        }
        try {
            //去datahub上创建节点
            DatasetUrn datasetUrn = null;
            StringMap customerPro = new StringMap();
            MetaDataSource metaDataSource = metaDataTableMapper.findMetaDataSource(metaDataTable.getTable_type());
            if (metaDataSource == null || StringUtils.isEmpty(metaDataSource.getDatahub_instance()) || StringUtils.isEmpty(metaDataSource.getDatahub_ingestion_source())) {
                System.out.println("数据源不存在或者数据源的datahub_instance或者datahub_ingestion_source为空");
                return commonResponse;
            }
            if (metaDataTable.getTable_type() == 1) {
                datasetUrn = new DatasetUrn(new DataPlatformUrn("hive"), metaDataSource.getDatahub_instance() + "." + metaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
                customerPro.put("CreateTime", new Date().toString());
                customerPro.put("Database", metaDataTable.getDatabase_name());
                customerPro.put("Owner", metaDataTable.getCreator());
                customerPro.put("OwnerType", "USER");
            } else if (metaDataTable.getTable_type() == 2) {
                datasetUrn = new DatasetUrn(new DataPlatformUrn("clickhouse"), metaDataSource.getDatahub_instance() + "." + metaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
                customerPro.put("engine", metaDataTable.getStorage_format());
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                customerPro.put("metadata_modification_time", dateFormat.format(new Date()));
                customerPro.put("partition_key", metaDataTable.getPartition_field().get(0).getName());
                customerPro.put("sorting_key", StringUtils.join(metaDataTable.getOrder_field(), ","));
            }
            DatasetProperties datasetProperties = new DatasetProperties().setName(metaDataTable.getName()).setCustomProperties(customerPro);
            addMetadata("dataset", datasetUrn, datasetProperties, emitter);
            //关联数据域
            Domains domains = new Domains();
            UrnArray urnArray = new UrnArray();
            urnArray.add(new Urn(metaDataTable.getData_domain()));
            domains.setDomains(urnArray);
            addMetadata("dataset", datasetUrn, domains, emitter);
            //设置表类型
            StringArray types = new StringArray();
            types.add("Table");
            SubTypes subTypes = new SubTypes();
            subTypes.setTypeNames(types);
            addMetadata("dataset", datasetUrn, subTypes, emitter);
            //入ownerShip
            Ownership ownership = new Ownership();
            OwnerArray ownerArray = new OwnerArray();
            ownerArray.add(new Owner().setOwner(new Urn("urn:li:corpuser:" + metaDataTable.getCreator())).setType(OwnershipType.DATA_STEWARD));
            ownership.setOwners(ownerArray);
            addMetadata("dataset", datasetUrn, ownership, emitter);
        } catch (Exception e) {
            e.printStackTrace();
        }
        executionIngestionDatahub(metaDataTable.getTable_type());
        return commonResponse;
    }

    public boolean addMetadata(String entityType, Urn urn, RecordTemplate recordTemplate, RestEmitter emitter) throws IOException, ExecutionException, InterruptedException {
        MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
                .entityType(entityType)
                .entityUrn(urn)
                .upsert()
                .aspect(recordTemplate)
                .build();
        // Blocking call using future
        Future<MetadataWriteResponse> requestFuture = emitter.emit(mcpw, null);
        MetadataWriteResponse mwr = requestFuture.get();
        if (!mwr.isSuccess()) {
            HttpResponse httpResponse = (HttpResponse) mwr.getUnderlyingResponse();
            System.out.println(String.format("失败发送元数据事件: %s, aspect: %s 状态码是: %d",
                    mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
            return false;
        }
        return true;
    }

    public CommonResponse precomputationDdl(MetaDataTable metaDataTable) {
        CommonResponse commonResponse = new CommonResponse();
        MetaDataTable oldMetaDataTable = null;
        if (metaDataTable.getId() > 0) {
            oldMetaDataTable = metaDataTableMapper.findMetaDataTableById(metaDataTable.getId());
            if (oldMetaDataTable == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("原始表不存在,请刷新后再查看ddl");
                return commonResponse;
            }
            if (StringUtils.isEmpty(oldMetaDataTable.getMapping_instance_table())) {
                oldMetaDataTable.setMapping_instance_table("");
            }
        }
        if (metaDataTable == null || StringUtils.isEmpty(metaDataTable.getName()) || StringUtils.isEmpty(metaDataTable.getDatabase_name()) || StringUtils.isEmpty(metaDataTable.getStorage_format())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表名,库名,存储格式");
            return commonResponse;
        }
        if (metaDataTable.getPartition_field() == null || metaDataTable.getPartition_field().size() == 0 || metaDataTable.getPartition_field().size() > 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("分区必须有值,且必须只有一个值");
            return commonResponse;
        }
        if (metaDataTable.getOrder_field() == null || metaDataTable.getOrder_field().length == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("排序的字段不能未空");
            return commonResponse;
        }
        String[] createSql = createCKTableSql(metaDataTable);
        String sql = "";
        if (oldMetaDataTable == null || oldMetaDataTable.getMapping_instance_table().equals(oldMetaDataTable.getName() + "_instance")) {
            sql = createSql[0] + ";\n" + createSql[1];
            if (!metaDataTable.getStorage_format().equals("ReplicatedMergeTree")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("引擎只支持ReplicatedMergeTree");
                return commonResponse;
            }
        } else if (StringUtils.isEmpty(oldMetaDataTable.getMapping_instance_table())) {
            sql = createSql[1].substring(0, createSql[1].indexOf("ENGINE")) + metaDataTable.getDdl().substring(metaDataTable.getDdl().indexOf("ENGINE"));
        } else {
            String[] oldSql = metaDataTable.getDdl().split(";\n");
            if (oldSql[0].contains("Distributed")) {
                String tmp = oldSql[0];
                oldSql[0] = oldSql[1];
                oldSql[1] = tmp;
            }
            sql = createSql[0].substring(0, createSql[0].indexOf("ENGINE")).replace(metaDataTable.getName() + "_instance", oldMetaDataTable.getMapping_instance_table()) + oldSql[1].substring(oldSql[1].indexOf("ENGINE")) + ";\n" + createSql[1].substring(0, createSql[1].indexOf("ENGINE")) + oldSql[0].substring(oldSql[0].indexOf("ENGINE"));
        }
        commonResponse.setData(sql);
        return commonResponse;
    }

    public String[] createCKTableSql(MetaDataTable metaDataTable) {
        String sql = "";
        for (int i = 0; i < metaDataTable.getColumns().size(); i++) {
            Column column = metaDataTable.getColumns().get(i);
            sql = sql + "`" + column.getName() + "` " + getCKType(column);
            if (!StringUtils.isEmpty(column.getDefault_value())) {
                sql = sql + " DEFAULT " + column.getDefault_value();
            }
            if (!StringUtils.isEmpty(column.getDesc())) {
                sql = sql + " COMMENT '" + column.getDesc() + "'";
            }
            if (i != metaDataTable.getColumns().size() - 1) {
                sql = sql + ",\n";
            }
        }
        String tableSql = "create table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + "_instance" + " ON CLUSTER cm_ck_cluster\n(\n" + sql + "\n)ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/" + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + "_instance', '{replica}')" + "\nPARTITION BY " + metaDataTable.getPartition_field().get(0).getName() + "\nORDER BY (" + StringUtils.join(metaDataTable.getOrder_field(), ",") + ")";
        String vSql = "create table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " ON CLUSTER cm_ck_cluster\n(\n" + sql + "\n)ENGINE = Distributed('cm_ck_cluster', '" + metaDataTable.getDatabase_name() + "', '" + metaDataTable.getName() + "_instance', rand())";
        if (!StringUtils.isEmpty(metaDataTable.getDescr())) {
            tableSql = tableSql + "\nCOMMENT '" + metaDataTable.getDescr() + "'";
            vSql = vSql + "\nCOMMENT '" + metaDataTable.getDescr() + "'";
        }
        return new String[]{tableSql, vSql};
    }

    public String getCKType(Column column) {
        String type = column.getType();
        if (type.toLowerCase().equals("decimal")) {
            JSONObject typeDetail = JSONArray.parseArray(column.getType_detail()).getJSONObject(0);
            type = type + "(" + typeDetail.getIntValue("precision") + "," + typeDetail.getIntValue("scale") + ")";
        } else if (type.toLowerCase().equals("enum")) {
            JSONObject typeDetail = JSONArray.parseArray(column.getType_detail()).getJSONObject(0);
            JSONArray enumValues = typeDetail.getJSONArray("enumValues");
            type = type + "(";
            for (int j = 0; j < enumValues.size(); j++) {
                JSONObject enumValue = enumValues.getJSONObject(j);
                type = type + "'" + enumValue.getString("key") + "'=" + enumValue.getIntValue("value");
                if (j != enumValues.size() - 1) {
                    type = type + ",";
                }
            }
            type = type + ")";
        } else if (type.toLowerCase().equals("array")) {
            JSONObject typeDetail = JSONArray.parseArray(column.getType_detail()).getJSONObject(0);
            JSONObject innerTypeDetail = typeDetail.getJSONArray("nested").getJSONObject(0);
            String innerType = innerTypeDetail.getString("type");
            type = type + "(" + innerType;
            if (innerType.toLowerCase().equals("decimal")) {
                type = type + "(" + innerTypeDetail.getIntValue("precision") + "," + innerTypeDetail.getIntValue("scale") + ")";
            } else if (innerType.toLowerCase().equals("enum")) {
                JSONArray enumValues = innerTypeDetail.getJSONArray("enumValues");
                type = type + "(";
                for (int j = 0; j < enumValues.size(); j++) {
                    JSONObject enumValue = enumValues.getJSONObject(j);
                    type = type + "'" + enumValue.getString("key") + "'=" + enumValue.getIntValue("value");
                    if (j != enumValues.size() - 1) {
                        type = type + ",";
                    }
                }
                type = type + ")";
            }
            type = type + ")";
        }
        return type;
    }


    public CommonResponse updateTable(UpdateMetaDataTableReq metaDataTable) {
        CommonResponse commonResponse = new CommonResponse();
        //校验参数是否为空
        if (metaDataTable == null || StringUtils.isEmpty(metaDataTable.getName()) || StringUtils.isEmpty(metaDataTable.getStorage_format())
                || StringUtils.isEmpty(metaDataTable.getData_domain())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("表名,存储格式,数据域,不能为空");
            return commonResponse;
        }
        //查询原始表
        MetaDataTable oldMetaDataTable = metaDataTableMapper.findMetaDataTableById(metaDataTable.getId());
        if (oldMetaDataTable == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("原始表不存在");
            return commonResponse;
        }
        if (metaDataTable.getTable_type() == 1) {
            if (StringUtils.isEmpty(metaDataTable.getDdl())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("生成的ddl不能为空");
                return commonResponse;
            }
            if (StringUtils.isEmpty(metaDataTable.getModel_level())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("模型层级不能为空");
                return commonResponse;
            }
        } else {
            if (metaDataTable.getUpdate_time() != null) {
                if (!oldMetaDataTable.getUpdate_time().equals(metaDataTable.getUpdate_time())) {
                    commonResponse.setSuccess(false);
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    commonResponse.setMessage("表在" + format.format(oldMetaDataTable.getUpdate_time()) + " 被改变,请刷新后再更新");
                    return commonResponse;
                }
            }
            if (metaDataTable.getUpdateColumn() != null && metaDataTable.getUpdateColumn().size() > 0) {
                if (metaDataTable.getOldUpdateColumn() == null || metaDataTable.getOldUpdateColumn().size() != metaDataTable.getUpdateColumn().size()) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("更新列不为空时,以前老的列需和它对应");
                    return commonResponse;
                }
            }
        }


        //判断名字是否重复
        if (!oldMetaDataTable.getName().equals(metaDataTable.getName())) {
            if (metaDataTableMapper.findMetaDataTableByName(oldMetaDataTable.getDatabase_name(), metaDataTable.getName(), oldMetaDataTable.getTable_type()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("表已经存在了,请不要重复添加");
                return commonResponse;
            }
        }

        //执行sql
        Map<String, Column> updateColumn = new HashMap<>();
        Set<String> deleteColumn = new HashSet<>();
        List<Column> insertColumn = new ArrayList<>();
        if (metaDataTable.getTable_type() == 1) {
            if (metaDataTable.getUpdateSql() != null && metaDataTable.getUpdateSql().length > 0) {
                Connection conn = null;
                int i = 0;
                String[] sqls = metaDataTable.getUpdateSql();
                PreparedStatement stmt = null;
                try {
                    Class.forName("org.apache.hive.jdbc.HiveDriver");
                    conn = DriverManager.getConnection(hiveUrl, hiveUser, hivePassword);
                    for (i = 0; i < sqls.length; i++) {
                        String sqlExecute = sqls[i].replaceAll("\\?", "").replaceAll("\\\\'", "").replaceAll("\\\\", "");
                        stmt = conn.prepareStatement(sqlExecute);
                        stmt.execute();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (i == 0) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("去hive执行sql：" + sqls[i] + " 失败:" + e.getMessage());
                        return commonResponse;
                    } else if (i == 1) {
                        //删除表成功,创建新表失败,回退
                        try {
                            String ddl = oldMetaDataTable.getDdl().replaceAll("\\?", "").replaceAll("\\\\'", "").replaceAll("\\\\", "");
                            if (ddl != null && ddl.endsWith(";")) {
                                ddl = ddl.substring(0, ddl.length() - 1);
                            }
                            stmt = conn.prepareStatement(ddl);
                            stmt.execute();
                            //执行加载分区的语句
                            stmt = conn.prepareStatement("MSCK REPAIR TABLE " + oldMetaDataTable.getDatabase_name() + "." + oldMetaDataTable.getName());
                            stmt.execute();
                        } catch (Exception ee) {
                            ee.printStackTrace();
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("去hive执行sql：" + sqls[i] + "失败:" + e.getMessage() + ",并且回退时失败：" + ee.getMessage());
                            return commonResponse;
                        }
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("去hive执行sql：" + sqls[i] + "失败:" + e.getMessage());
                        return commonResponse;
                    } else {
                        commonResponse.setMessage("分区重新加载失败：" + e.getMessage());
                    }
                } finally {
                    try {
                        stmt.close();
                        conn.close();
                    } catch (Exception tt) {
                        tt.printStackTrace();
                    }
                }
            }
        } else {
            Connection conn = null;
            PreparedStatement pStemt = null;
            try {
                conn = DriverManager.getConnection(ckUrl, ckUser, ckPassword);
                boolean haveInstance = false;
                if (!StringUtils.isEmpty(oldMetaDataTable.getMapping_instance_table())) {
                    haveInstance = true;
                }
                if (metaDataTable.getDeleteColumn() != null && metaDataTable.getDeleteColumn().size() > 0) {
                    for (Column column : metaDataTable.getDeleteColumn()) {
                        String sql = "alter table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " on cluster cm_ck_cluster " + " drop column `" + column.getName() + "`";
                        if (haveInstance) {
                            String instanceSql = sql.replace(metaDataTable.getName(), oldMetaDataTable.getMapping_instance_table());
                            System.out.println("去ck执行sql:" + instanceSql);
                            pStemt = conn.prepareStatement(instanceSql);
                            pStemt.execute();
                        }
                        System.out.println("去ck执行sql:" + sql);
                        pStemt = conn.prepareStatement(sql);
                        pStemt.execute();
                        deleteColumn.add(column.getName());
                    }

                }
                if (metaDataTable.getInsertColumn() != null && metaDataTable.getInsertColumn().size() > 0) {
                    for (Column column : metaDataTable.getInsertColumn()) {
                        String sql = "alter table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " on cluster cm_ck_cluster " + " add column `" + column.getName() + "` " + getCKType(column);
                        if (!StringUtils.isEmpty(column.getDefault_value())) {
                            sql = sql + " DEFAULT " + column.getDefault_value();
                        }
                        if (!StringUtils.isEmpty(column.getDesc())) {
                            sql = sql + " COMMENT '" + column.getDesc() + "'";
                        }
                        if (haveInstance) {
                            String instanceSql = sql.replace(metaDataTable.getName(), oldMetaDataTable.getMapping_instance_table());
                            System.out.println("去ck执行sql:" + instanceSql);
                            pStemt = conn.prepareStatement(instanceSql);
                            pStemt.execute();
                        }
                        System.out.println("去ck执行sql:" + sql);
                        pStemt = conn.prepareStatement(sql);
                        pStemt.execute();
                        insertColumn.add(column);
                    }

                }
                if (metaDataTable.getUpdateColumn() != null && metaDataTable.getUpdateColumn().size() > 0) {
                    for (int i = 0; i < metaDataTable.getUpdateColumn().size(); i++) {
                        Column oldColumn = metaDataTable.getOldUpdateColumn().get(i);
                        String oldColumnName = oldColumn.getName();
                        Column newColumn = metaDataTable.getUpdateColumn().get(i);
                        String sql = "";
                        String oldDefaultValue = oldColumn.getDefault_value();
                        String newDefaultValue = newColumn.getDefault_value();
                        if (StringUtils.isEmpty(oldDefaultValue)) {
                            oldDefaultValue = "";
                        }
                        if (StringUtils.isEmpty(newDefaultValue)) {
                            newDefaultValue = "";
                        }
                        if ((!getCKType(oldColumn).equals(getCKType(newColumn))) || (!oldDefaultValue.equals(newDefaultValue))) {
                            sql = "alter table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " on cluster cm_ck_cluster modify column `" + oldColumn.getName() + "` " + getCKType(newColumn);
                            oldColumn.setType(newColumn.getType());
                            if (!StringUtils.isEmpty(newColumn.getDefault_value())) {
                                if (StringUtils.isEmpty(oldColumn.getDefault_value()) || (!oldColumn.getDefault_value().equals(newColumn.getDefault_value()))) {
                                    sql = sql + " DEFAULT " + newColumn.getDefault_value();
                                    oldColumn.setDefault_value(newColumn.getDefault_value());
                                }
                            }
                            if (haveInstance) {
                                String instanceSql = sql.replace(metaDataTable.getName(), oldMetaDataTable.getMapping_instance_table());
                                System.out.println("去ck执行sql:" + instanceSql);
                                pStemt = conn.prepareStatement(instanceSql);
                                pStemt.execute();
                            }
                            System.out.println("去ck执行sql:" + sql);
                            pStemt = conn.prepareStatement(sql);
                            pStemt.execute();
                            updateColumn.put(oldColumnName, oldColumn);
                        }
                        String oldDesc = oldColumn.getDesc();
                        String newDesc = newColumn.getDesc();
                        if (StringUtils.isEmpty(oldDesc)) {
                            oldDesc = "";
                        }
                        if (StringUtils.isEmpty(newDesc)) {
                            newDesc = "";
                        }

                        if (!oldDesc.equals(newDesc)) {
                            sql = "alter table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " on cluster cm_ck_cluster comment column `" + oldColumn.getName() + "` '" + newDesc + "'";
                            if (haveInstance) {
                                String instanceSql = sql.replace(metaDataTable.getName(), oldMetaDataTable.getMapping_instance_table());
                                System.out.println("去ck执行sql:" + instanceSql);
                                pStemt = conn.prepareStatement(instanceSql);
                                pStemt.execute();
                            }
                            System.out.println("去ck执行sql:" + sql);
                            pStemt = conn.prepareStatement(sql);
                            pStemt.execute();
                            oldColumn.setDesc(newColumn.getDesc());
                            updateColumn.put(oldColumnName, oldColumn);
                        }

                        if (!oldColumn.getName().equals(newColumn.getName())) {
                            sql = "alter table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " on cluster cm_ck_cluster RENAME  column `" + oldColumn.getName() + "` to `" + newColumn.getName() + "`";
                            if (haveInstance) {
                                String instanceSql = sql.replace(metaDataTable.getName(), oldMetaDataTable.getMapping_instance_table());
                                System.out.println("去ck执行sql:" + instanceSql);
                                pStemt = conn.prepareStatement(instanceSql);
                                pStemt.execute();
                            }
                            System.out.println("去ck执行sql:" + sql);
                            pStemt = conn.prepareStatement(sql);
                            pStemt.execute();
                            oldColumn.setName(newColumn.getName());
                            updateColumn.put(oldColumnName, oldColumn);
                        }
                    }


                }
            } catch (Exception e) {
                e.printStackTrace();
                commonResponse.setMessage("列信息更新失败：" + e.getMessage());
                commonResponse.setSuccess(false);
            } finally {
                try {
                    pStemt.close();
                    conn.close();
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }

        }
        //更新列信息
        if (!commonResponse.isSuccess()) {
            List<Column> oldColumn = new ArrayList<>();
            List<Column> columns = oldMetaDataTable.getColumns();
            if (columns != null && columns.size() > 0) {
                for (int i = 0; i < columns.size(); i++) {
                    Map<String, Object> column = (Map<String, Object>) columns.get(i);
                    Column columnNew = new Column();
                    columnNew.setName((String) column.get("name"));
                    columnNew.setType((String) column.get("type"));
                    columnNew.setZh_name((String) column.get("zh_name"));
                    columnNew.setType_detail((String) column.get("type_detail"));
                    if (column.containsKey("length")) {
                        columnNew.setLength((int) column.get("length"));
                    } else {
                        columnNew.setLength(100);
                    }
                    columnNew.setDesc((String) column.get("desc"));
                    columnNew.setDefault_value((String) column.get("default_value"));
                    oldColumn.add(columnNew);
                }
            }
            if ((!deleteColumn.isEmpty()) || (!updateColumn.isEmpty())) {
                for (int j = 0; j < oldColumn.size(); j++) {
                    String columnName = oldColumn.get(j).getName();
                    if (deleteColumn.contains(columnName)) {
                        oldColumn.remove(j);
                        j--;
                    } else if (updateColumn.containsKey(columnName)) {
                        oldColumn.set(j, updateColumn.get(columnName));
                    }
                }

            }

            if (!insertColumn.isEmpty()) {
                oldColumn.addAll(insertColumn);
            }
            metaDataTable.setColumns(oldColumn);
        }
        //生成新的ddl
        if (metaDataTable.getTable_type() == 2) {
            metaDataTable.setDdl(precomputationDdl(metaDataTable).getData().toString());
        }

        //更新表
        if (metaDataTableMapper.updateMetaDataTable(metaDataTable) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("更新信息入库失败,请联系管理员" + commonResponse.getMessage());
            return commonResponse;
        }
        DatasetUrn datasetUrn = null;
        MetaDataSource metaDataSource = metaDataTableMapper.findMetaDataSource(metaDataTable.getTable_type());
        if (metaDataSource == null || StringUtils.isEmpty(metaDataSource.getDatahub_instance()) || StringUtils.isEmpty(metaDataSource.getDatahub_ingestion_source())) {
            System.out.println("数据源不存在或者数据源的datahub_instance或者datahub_ingestion_source为空");
            return commonResponse;
        }

        if (metaDataTable.getTable_type() == 1) {
            datasetUrn = new DatasetUrn(new DataPlatformUrn("hive"), metaDataSource.getDatahub_instance() + "." + oldMetaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
        } else {
            datasetUrn = new DatasetUrn(new DataPlatformUrn("clickhouse"), metaDataSource.getDatahub_instance() + "." + oldMetaDataTable.getDatabase_name() + "." + oldMetaDataTable.getName(), FabricType.PROD);
        }

        if (!metaDataTable.getName().equals(oldMetaDataTable.getName())) {
            try {
                //去datahub上创建节点
                //创建表
                StringMap customerPro = new StringMap();
                customerPro.put("CreateTime", new Date().toString());
                customerPro.put("Database", oldMetaDataTable.getDatabase_name());
                customerPro.put("Owner", metaDataTable.getCreator());
                customerPro.put("OwnerType", "USER");
                DatasetProperties datasetProperties = new DatasetProperties().setName(metaDataTable.getName()).setCustomProperties(customerPro);
                addMetadata("dataset", datasetUrn, datasetProperties, emitter);
                //关联数据域
                Domains domains = new Domains();
                UrnArray urnArray = new UrnArray();
                urnArray.add(new Urn(metaDataTable.getData_domain()));
                domains.setDomains(urnArray);
                addMetadata("dataset", datasetUrn, domains, emitter);
                //设置表类型
                StringArray types = new StringArray();
                types.add("Table");
                SubTypes subTypes = new SubTypes();
                subTypes.setTypeNames(types);
                addMetadata("dataset", datasetUrn, subTypes, emitter);
                //入ownerShip
                Ownership ownership = new Ownership();
                OwnerArray ownerArray = new OwnerArray();
                ownerArray.add(new Owner().setOwner(new Urn("urn:li:corpuser:" + metaDataTable.getCreator())).setType(OwnershipType.DATA_STEWARD));
                ownership.setOwners(ownerArray);
                addMetadata("dataset", datasetUrn, ownership, emitter);
            } catch (Exception e) {
                e.printStackTrace();
                return commonResponse;
            }
        } else {
            //更新datahub
            if (!metaDataTable.getData_domain().equals(oldMetaDataTable.getData_domain())) {
                try {
                    Domains domains = new Domains();
                    UrnArray urnArray = new UrnArray();
                    urnArray.add(new Urn(metaDataTable.getData_domain()));
                    domains.setDomains(urnArray);
                    addMetadata("dataset", datasetUrn, domains, emitter);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return commonResponse;
    }

    public CommonResponse deleteTable(DeleteReq deleteReq) {
        CommonResponse commonResponse = new CommonResponse();
        //查询原始表
        MetaDataTable metaDataTable = metaDataTableMapper.findMetaDataTableById(deleteReq.getId());
        if (metaDataTable == null) {
            if ((!StringUtils.isEmpty(deleteReq.getDatabase_name())) && (!StringUtils.isEmpty(deleteReq.getName()))) {
                metaDataTable = metaDataTableMapper.findMetaDataTableByName(deleteReq.getDatabase_name(), deleteReq.getName(), deleteReq.getTable_type());
            }
            if (metaDataTable == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("原始表不存在");
                return commonResponse;
            }
        }
        if (metaDataTableMapper.deleteMetaDataTableById(metaDataTable.getId()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("删除失败,请稍后再试");
            return commonResponse;
        }
        //删除ck里面的表
        if (metaDataTable.getTable_type() == 2) {
            //删除中间表
            Connection conn = null;
            PreparedStatement pStemt = null;
            try {
                conn = DriverManager.getConnection(ckUrl, ckUser, ckPassword);
                pStemt = conn.prepareStatement("drop table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + "_instance on cluster cm_ck_cluster sync");
                pStemt.execute();
                pStemt = conn.prepareStatement("drop table " + metaDataTable.getDatabase_name() + "." + metaDataTable.getName() + " on cluster cm_ck_cluster sync");
                pStemt.execute();
            } catch (Exception ee) {
                ee.printStackTrace();
            } finally {
                try {
                    pStemt.close();
                    conn.close();
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
        }
        return commonResponse;
    }

    public CommonResponse findTable(QueryMetaDataTableReq queryMetaDataTableReq) {
        CommonResponse commonResponse = new CommonResponse();
        String condition = "m.deleted=0";
        if (!StringUtils.isEmpty(queryMetaDataTableReq.getDatabase_name())) {
            condition = condition + " and m.database_name='" + queryMetaDataTableReq.getDatabase_name() + "'";
        }
        if (queryMetaDataTableReq.getTheme_id() > 0) {
            condition = condition + " and m.theme_id=" + queryMetaDataTableReq.getTheme_id();
        }
        if (queryMetaDataTableReq.getTable_type() > 0) {
            condition = condition + " and m.table_type=" + queryMetaDataTableReq.getTable_type();
        }
        if (!StringUtils.isEmpty(queryMetaDataTableReq.getModel_level())) {
            condition = condition + " and m.model_level='" + queryMetaDataTableReq.getModel_level() + "'";
        }
        if (!StringUtils.isEmpty(queryMetaDataTableReq.getData_domain())) {
            condition = condition + " and m.data_domain='" + queryMetaDataTableReq.getData_domain() + "'";
        }
        commonResponse.setData(metaDataTableMapper.findMetaDataTable(condition));
        return commonResponse;
    }

    public CommonResponse findTableByTableName(int table_type, String database_name, String table_name) {
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(database_name) || StringUtils.isEmpty((table_name))) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("库名和表名不能为空");
            return commonResponse;
        }
        if (table_type != 1 && table_type != 2) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("暂不支持的表类型");
            return commonResponse;
        }
        commonResponse.setData(metaDataTableMapper.findMetaDataTableByName(database_name, table_name, table_type));
        return commonResponse;
    }

    public CommonResponse analysisFile(MultipartFile file) {
        CommonResponse commonResponse = new CommonResponse();
        List<Column> columns = new ArrayList<>();
        try {
            CSVReader csvReader = new CSVReaderBuilder(
                    new BufferedReader(
                            new InputStreamReader(file.getInputStream(), "utf-8"))).build();
            Iterator<String[]> iterator = csvReader.iterator();
            while (iterator.hasNext()) {
                String[] next = iterator.next();
                Column column = new Column();
                if (next.length >= 1) {
                    column.setName(next[0]);
                }
                if (next.length >= 2) {
                    column.setZh_name(next[1]);
                }
                if (next.length >= 3) {
                    column.setType(next[2]);
                }
                if (next.length >= 4) {
                    column.setLength(Integer.parseInt(next[3]));
                }
                if (next.length >= 5) {
                    column.setDesc(next[4]);
                }
                columns.add(column);
            }
            commonResponse.setData(columns);
            return commonResponse;
        } catch (Exception e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
    }

    public CommonResponse historyDataAddDataBase(HistoryDataAddDataBase historyDataAddDataBase) {
        CommonResponse commonResponse = new CommonResponse();
        if (historyDataAddDataBase == null || StringUtils.isEmpty(historyDataAddDataBase.getDatabase_name())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("库名不能为空");
            return commonResponse;
        }
        if (historyDataAddDataBase.getTable_type() != 1 & historyDataAddDataBase.getTable_type() != 2) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("不支持的表类型");
            return commonResponse;
        }
        String datahubInstance = "";
        MetaDataSource metaDataSource = metaDataTableMapper.findMetaDataSource(historyDataAddDataBase.getTable_type());
        if (metaDataSource != null && (!StringUtils.isEmpty(metaDataSource.getDatahub_instance()))) {
            datahubInstance = metaDataSource.getDatahub_instance();
        }

        if (historyDataAddDataBase.getTable_type() == 1) {
            if (StringUtils.isEmpty(historyDataAddDataBase.getModel_level())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("层级不能为空");
                return commonResponse;
            }
            Connection conn = null;
            PreparedStatement pStemt = null;
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(hiveMetastoreUrl, hiveMetastoreUser, hiveMetastorePassword);
                String sql = "select * from DBS d left join TBLS t on d.DB_ID=t.DB_ID LEFT JOIN SDS s on t.SD_ID=s.SD_ID LEFT JOIN (select * from TABLE_PARAMS where PARAM_KEY='comment') p on t.TBL_ID=p.TBL_ID where d.name='" + historyDataAddDataBase.getDatabase_name() + "'";
                if (!StringUtils.isEmpty(historyDataAddDataBase.getTable_name())) {
                    sql = sql + " and t.TBL_NAME='" + historyDataAddDataBase.getTable_name() + "'";
                }
                pStemt = conn.prepareStatement(sql);
                ResultSet set = pStemt.executeQuery();
                while (set.next()) {
                    MetaDataTable metaDataTable = new MetaDataTable();
                    metaDataTable.setData_domain(historyDataAddDataBase.getData_domain());
                    metaDataTable.setLife_cycle(0);
                    metaDataTable.setDatabase_name(historyDataAddDataBase.getDatabase_name());
                    metaDataTable.setTable_type(1);
                    metaDataTable.setName(set.getString("TBL_NAME"));
                    metaDataTable.setModel_level(historyDataAddDataBase.getModel_level());
                    metaDataTable.setSystem_delimiter(true);
                    metaDataTable.setTheme_id(historyDataAddDataBase.getTheme_id());
                    String location = set.getString("LOCATION");
                    if (StringUtils.isEmpty(location)) {
                        metaDataTable.setSystem_storage_location(true);
                    } else {
                        if (location.contains(metaDataTable.getName())) {
                            metaDataTable.setSystem_storage_location(true);
                        } else {
                            metaDataTable.setSystem_storage_location(false);
                        }
                        location = location.replaceAll("hdfs://nameservice1", "");
                        metaDataTable.setStorage_location(location);
                    }
                    metaDataTable.setDescr(set.getString("PARAM_VALUE"));
                    String inputFormat = set.getString("INPUT_FORMAT");
                    if (!StringUtils.isEmpty(inputFormat)) {
                        if (inputFormat.equals("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")) {
                            metaDataTable.setStorage_format("parquet");
                        } else if (inputFormat.equals("org.apache.hadoop.mapred.TextInputFormat")) {
                            metaDataTable.setStorage_format("textfile");
                        } else {
                            metaDataTable.setStorage_format(inputFormat);
                        }
                    }
                    String owner = set.getString("OWNER");
                    if (StringUtils.isEmpty(owner)) {
                        owner = "unknown";
                    } else if (owner.equals("qiong.tan")) {
                        owner = "tanqiong";
                    } else if (owner.equals("datasource")) {
                        owner = "liuhao";
                    } else if (owner.startsWith("hive") || owner.startsWith("hdfs")) {
                        owner = "hive";
                    }
                    metaDataTable.setCreator(owner);
                    metaDataTable.setCreate_time(new Date(set.getLong("CREATE_TIME") * 1000));
                    //查询parquet信息
                    List<Partition_field> partition_fields = new ArrayList<>();
                    int tableId = set.getInt("TBL_ID");
                    sql = "select p.* from TBLS t left join PARTITION_KEYS p ON t.TBL_ID=p.TBL_ID where t.TBL_ID=" + tableId;
                    pStemt = conn.prepareStatement(sql);
                    ResultSet setTmp = pStemt.executeQuery();
                    int rows = setTmp.getRow();
                    boolean ispaquet = false;
                    if (rows > 0) {
                        while (setTmp.next()) {
                            ispaquet = true;
                            Partition_field partition_field = new Partition_field();
                            String comment = setTmp.getString("PKEY_COMMENT");
                            if (!StringUtils.isEmpty(comment)) {
                                comment = comment.replaceAll("\r\n", "");
                            }
                            partition_field.setDesc(comment);
                            partition_field.setName(setTmp.getString("PKEY_NAME"));
                            partition_field.setLength(100);
                            partition_field.setType(setTmp.getString("PKEY_TYPE"));
                            partition_field.setType_detail("[{\"operations\":[],\"comment\":\"" + partition_field.getDesc() + "\",\"nested\":[],\"name\":\"" + partition_field.getName() + "\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"" + partition_field.getType() + "\",\"showProperties\":false,\"scale\":0}]");
                            partition_field.setFormat("dt=yyyy-MM-dd");
                            partition_fields.add(partition_field);
                        }
                    }
                    if (ispaquet) {
                        metaDataTable.setPartition(true);
                        metaDataTable.setPartition_field(partition_fields);
                    } else {
                        metaDataTable.setPartition(false);
                    }
                    //查询列信息
                    List<Column> columns = new ArrayList<>();
                    sql = "select c.* from TBLS t left join SDS s on t.SD_ID=s.SD_ID LEFT JOIN COLUMNS_V2 c on s.CD_ID=c.CD_ID where t.TBL_ID=" + tableId;
                    pStemt = conn.prepareStatement(sql);
                    setTmp = pStemt.executeQuery();
                    while (setTmp.next()) {
                        Column column = new Column();
                        String comment = setTmp.getString("COMMENT");
                        if (!StringUtils.isEmpty(comment)) {
                            comment = comment.replaceAll("\r\n", "");
                        }
                        column.setDesc(comment);
                        column.setName(setTmp.getString("COLUMN_NAME"));
                        column.setZh_name(setTmp.getString("COLUMN_NAME"));
                        column.setLength(100);
                        String type = setTmp.getString("TYPE_NAME");
                        if (type.equals("array<map<string,string>>")) {
                            column.setType("array");
                            column.setType_detail("[{\"operations\":[],\"comment\":\"" + column.getDesc() + "\",\"nested\":[{\"operations\":[],\"comment\":\"\",\"nested\":[{\"operations\":[],\"comment\":\"\",\"nested\":[],\"name\":\"\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"string\",\"showProperties\":false,\"scale\":0}],\"name\":\"\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"map\",\"showProperties\":false,\"scale\":0}],\"name\":\"aa\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"array\",\"showProperties\":false,\"scale\":0}]".replaceAll("aa", column.getName()));
                        } else if (type.equals("map<string,string>")) {
                            column.setType("map");
                            column.setType_detail("[{\"operations\":[],\"comment\":\"" + column.getDesc() + "\",\"nested\":[{\"operations\":[],\"comment\":\"\",\"nested\":[],\"name\":\"\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"string\",\"showProperties\":false,\"scale\":0}],\"name\":\"bb\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"map\",\"showProperties\":false,\"scale\":0}]".replaceAll("bb", column.getName()));
                        } else if (type.equals("struct<system_service:map<string,string>,user_service:map<string,string>,error_message:string>")) {
                            column.setType("struct");
                            column.setType_detail("[{\"operations\":[],\"comment\":\"" + column.getDesc() + "\",\"nested\":[{\"operations\":[],\"comment\":\"\",\"nested\":[{\"operations\":[],\"comment\":\"\",\"nested\":[],\"name\":\"\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"string\",\"showProperties\":false,\"scale\":0}],\"name\":\"system_service\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"map\",\"showProperties\":false,\"scale\":0},{\"operations\":[],\"comment\":\"\",\"nested\":[{\"operations\":[],\"comment\":\"\",\"nested\":[],\"name\":\"\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"string\",\"showProperties\":false,\"scale\":0}],\"name\":\"user_service\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"map\",\"showProperties\":false,\"scale\":0},{\"operations\":[],\"comment\":\"\",\"nested\":[],\"name\":\",error_message\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"string\",\"showProperties\":false,\"scale\":0}],\"name\":\"cc\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"struct\",\"showProperties\":false,\"scale\":0}]".replaceAll("cc", column.getName()));
                        } else if (type.equals("array<string>")) {
                            column.setType("array");
                            column.setType_detail("[{\"operations\":[],\"comment\":\"" + column.getDesc() + "\",\"nested\":[{\"operations\":[],\"comment\":\"\",\"nested\":[],\"name\":\"\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"string\",\"showProperties\":false,\"scale\":0}],\"name\":\"dd\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"array\",\"showProperties\":false,\"scale\":0}]".replaceAll("dd", column.getName()));
                        } else if (type.equals("struct<nest_in_struct1:string>")) {
                            column.setType("struct");
                            column.setType_detail("[{\"operations\":[],\"comment\":\"" + column.getDesc() + "\",\"nested\":[{\"operations\":[],\"comment\":\"\",\"nested\":[],\"name\":\"nest_in_struct1\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"string\",\"showProperties\":false,\"scale\":0}],\"name\":\"ff\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"struct\",\"showProperties\":false,\"scale\":0}]".replaceAll("ff", column.getName()));
                        } else if (type.contains("decimal(")) {
                            column.setType("decimal");
                            column.setType_detail("[{\"operations\":[],\"comment\":\"" + column.getDesc() + "\",\"nested\":[],\"name\":\"ee\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":" + type.substring(type.indexOf("(") + 1, type.indexOf(",")) + ",\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"decimal\",\"showProperties\":false,\"scale\":" + type.substring(type.indexOf(",") + 1, type.indexOf(")")) + "}]");
                            column.setType_detail(column.getType_detail().replaceAll("ee", column.getName()));
                        } else {
                            column.setType(type);
                            column.setType_detail("[{\"operations\":[],\"comment\":\"" + column.getDesc() + "\",\"nested\":[],\"name\":\"" + column.getName() + "\",\"level\":0,\"keyType\":\"string\",\"required\":false,\"precision\":10,\"keep\":true,\"isPartition\":false,\"length\":100,\"partitionValue\":\"\",\"multiValued\":false,\"unique\":false,\"type\":\"" + column.getType() + "\",\"showProperties\":false,\"scale\":0}]");
                        }
                        columns.add(column);
                    }
                    metaDataTable.setColumns(columns);
                    metaDataTable.setDdl("");
                    metaDataTable.setExternal_table(true);
                    //判断这个表是否已经存在,存在就跳过
                    if (metaDataTableMapper.findMetaDataTableByName(metaDataTable.getDatabase_name(), metaDataTable.getName(), metaDataTable.getTable_type()) == null) {
                        if (metaDataTableMapper.insertMetaDataTableHaveCreateTime(metaDataTable) < 1) {
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("插入数据失败,请联系管理员");
                            return commonResponse;
                        }
                        //关联数据域
                        DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("hive"), datahubInstance + "." + metaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
                        if (!StringUtils.isEmpty(metaDataTable.getData_domain())) {
                            Domains domains = new Domains();
                            UrnArray urnArray = new UrnArray();
                            urnArray.add(new Urn(metaDataTable.getData_domain()));
                            domains.setDomains(urnArray);
                            addMetadata("dataset", datasetUrn, domains, emitter);
                        }
                        //关联用户
                        Ownership ownership = new Ownership();
                        OwnerArray ownerArray = new OwnerArray();
                        ownerArray.add(new Owner().setOwner(new Urn("urn:li:corpuser:" + metaDataTable.getCreator())).setType(OwnershipType.DATA_STEWARD));
                        ownership.setOwners(ownerArray);
                        addMetadata("dataset", datasetUrn, ownership, emitter);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    pStemt.close();
                    conn.close();
                } catch (Exception tt) {
                    tt.printStackTrace();
                }
            }
        } else {
            if (StringUtils.isEmpty(historyDataAddDataBase.getTable_name())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("拉ck的历史数据表名不能为空");
                return commonResponse;
            }
            Connection conn = null;
            PreparedStatement pStemt = null;
            try {
                int tableNum = 1;
                conn = DriverManager.getConnection(ckUrl, ckUser, ckPassword);
                String sql = "select * from `system`.tables where database='" + historyDataAddDataBase.getDatabase_name() + "' and name in ('" + historyDataAddDataBase.getTable_name() + "'";
                if (!StringUtils.isEmpty(historyDataAddDataBase.getMapping_instance_table())) {
                    sql = sql + ",'" + historyDataAddDataBase.getMapping_instance_table() + "')";
                    tableNum = 2;
                } else {
                    sql = sql + ")";
                }
                System.out.println("查询表信息sql：" + sql);
                pStemt = conn.prepareStatement(sql);
                ResultSet set = pStemt.executeQuery();
                MetaDataTable metaDataTable = new MetaDataTable();
                metaDataTable.setMapping_instance_table(historyDataAddDataBase.getMapping_instance_table());
                metaDataTable.setData_domain(historyDataAddDataBase.getData_domain());
                metaDataTable.setLife_cycle(0);
                metaDataTable.setDatabase_name(historyDataAddDataBase.getDatabase_name());
                metaDataTable.setTable_type(2);
                metaDataTable.setName(historyDataAddDataBase.getTable_name());
                metaDataTable.setTheme_id(historyDataAddDataBase.getTheme_id());
                metaDataTable.setCreator("hive");
                int realTable = 0;
                while (set.next()) {
                    realTable++;
                    if (tableNum == 1 || set.getString("name").equals(historyDataAddDataBase.getMapping_instance_table())) {
                        metaDataTable.setDescr(set.getString("comment"));
                        metaDataTable.setStorage_format(set.getString("engine"));
                        if (!StringUtils.isEmpty(set.getString("sorting_key"))) {
                            metaDataTable.setOrder_field(set.getString("sorting_key").split(", "));
                        }
                        List<Partition_field> partition_fields = new ArrayList<>();
                        Partition_field partition_field = new Partition_field();
                        partition_field.setName(set.getString("partition_key"));
                        partition_fields.add(partition_field);
                        metaDataTable.setPartition(true);
                        metaDataTable.setPartition_field(partition_fields);
                        metaDataTable.setUpdate_time(new Date(set.getLong("metadata_modification_time") * 1000));
                        metaDataTable.setCreate_time(new Date(915148800000L));
                    }
                    if (StringUtils.isEmpty(metaDataTable.getDdl())) {
                        metaDataTable.setDdl(set.getString("create_table_query"));
                    } else {
                        metaDataTable.setDdl(metaDataTable.getDdl() + ";\n" + set.getString("create_table_query"));
                    }
                }
                if (realTable != tableNum) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("需要查出" + tableNum + "个表,实际数量为" + realTable);
                    return commonResponse;
                }

                //查询列信息
                List<Column> columns = new ArrayList<>();
                sql = "select * from `system`.columns where database='" + historyDataAddDataBase.getDatabase_name() + "' and table='" + historyDataAddDataBase.getTable_name() + "'";
                pStemt = conn.prepareStatement(sql);
                ResultSet setTmp = pStemt.executeQuery();
                while (setTmp.next()) {
                    Column column = new Column();
                    String comment = setTmp.getString("COMMENT");
                    if (!StringUtils.isEmpty(comment)) {
                        comment = comment.replaceAll("\r\n", "");
                    }
                    column.setDesc(comment);
                    column.setDefault_value(setTmp.getString("default_expression"));
                    column.setName(setTmp.getString("name"));
                    column.setZh_name(setTmp.getString("name"));
                    column.setLength(100);
                    String detailType = setTmp.getString("type");
                    String type;
                    if (detailType.contains("(")) {
                        type = detailType.substring(0, detailType.indexOf("("));
                        if (type.toLowerCase().equals("nullable")) {
                            detailType = detailType.substring(detailType.indexOf("(") + 1, detailType.lastIndexOf(")"));
                            if (detailType.contains("(")) {
                                type = detailType.substring(0, detailType.indexOf("("));
                            } else {
                                type = detailType;
                            }
                        }
                    } else {
                        type = detailType;
                    }
                    column.setType(type);
                    String lowerType = type.toLowerCase();
                    JSONArray jsonArray = new JSONArray();
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("comment", column.getDesc());
                    jsonObject.put("name", column.getName());
                    jsonObject.put("type", column.getType());
                    jsonObject.put("length", 100);
                    if (lowerType.startsWith("int") || lowerType.startsWith("string") || lowerType.startsWith("date") || lowerType.startsWith("float") || lowerType.startsWith("uint")) {
                        jsonObject.put("nested", new JSONArray());
                        jsonObject.put("precision", 0);
                        jsonObject.put("scale", 0);
                        jsonObject.put("enumValues", new JSONArray());
                    } else if (lowerType.startsWith("decimal")) {
                        String[] keyValues = detailType.substring(detailType.indexOf("(") + 1, detailType.lastIndexOf(")")).replaceAll(" ", "").split(",");
                        jsonObject.put("nested", new JSONArray());
                        jsonObject.put("precision", Integer.parseInt(keyValues[0]));
                        jsonObject.put("scale", Integer.parseInt(keyValues[1]));
                        jsonObject.put("enumValues", new JSONArray());
                    } else if (lowerType.startsWith("enum")) {
                        String[] keyValues = detailType.substring(detailType.indexOf("(") + 1, detailType.lastIndexOf(")")).replaceAll(" ", "").split(",");
                        jsonObject.put("nested", new JSONArray());
                        jsonObject.put("precision", 0);
                        jsonObject.put("scale", 0);
                        JSONArray enumValues = new JSONArray();
                        for (String keyValue : keyValues) {
                            JSONObject enumValue = new JSONObject();
                            String[] keyValueTmp = keyValue.split("=");
                            enumValue.put("key", keyValueTmp[0].replaceAll("'", ""));
                            enumValue.put("value", Integer.parseInt(keyValueTmp[1]));
                            enumValues.add(enumValue);
                        }
                        jsonObject.put("enumValues", enumValues);
                    } else if (lowerType.startsWith("array")) {
                        jsonObject.put("precision", 0);
                        jsonObject.put("scale", 0);
                        jsonObject.put("enumValues", new JSONArray());
                        JSONArray innerJsonArray = new JSONArray();
                        JSONObject innerJsonObject = new JSONObject();
                        String innerType = "";
                        detailType = detailType.substring(detailType.indexOf("(") + 1, detailType.lastIndexOf(")"));
                        if (detailType.contains("(")) {
                            innerType = detailType.substring(0, detailType.indexOf("("));
                        } else {
                            innerType = detailType;
                        }
                        innerJsonObject.put("comment", "");
                        innerJsonObject.put("name", "");
                        innerJsonObject.put("type", innerType);
                        innerJsonObject.put("length", 100);
                        innerJsonObject.put("nested", new JSONArray());
                        String lowerInnerType = innerType.toLowerCase();
                        if (lowerInnerType.startsWith("int") || lowerInnerType.startsWith("string") || lowerInnerType.startsWith("date") || lowerInnerType.startsWith("float") || lowerInnerType.startsWith("uint")) {
                            innerJsonObject.put("enumValues", new JSONArray());
                            innerJsonObject.put("precision", 0);
                            innerJsonObject.put("scale", 0);
                        } else if (lowerInnerType.startsWith("decimal")) {
                            String[] keyValues = detailType.substring(detailType.indexOf("(") + 1, detailType.lastIndexOf(")")).replaceAll(" ", "").split(",");
                            innerJsonObject.put("precision", Integer.parseInt(keyValues[0]));
                            innerJsonObject.put("scale", Integer.parseInt(keyValues[1]));
                            innerJsonObject.put("enumValues", new JSONArray());
                        } else if (lowerInnerType.startsWith("enum")) {
                            String[] keyValues = detailType.substring(detailType.indexOf("(") + 1, detailType.lastIndexOf(")")).replaceAll(" ", "").split(",");
                            innerJsonObject.put("precision", 0);
                            innerJsonObject.put("scale", 0);
                            JSONArray enumValues = new JSONArray();
                            for (String keyValue : keyValues) {
                                JSONObject enumValue = new JSONObject();
                                String[] keyValueTmp = keyValue.split("=");
                                enumValue.put("key", keyValueTmp[0].replaceAll("'", ""));
                                enumValue.put("value", Integer.parseInt(keyValueTmp[1]));
                                enumValues.add(enumValue);
                            }
                            innerJsonObject.put("enumValues", enumValues);
                        } else {
                            commonResponse.setSuccess(false);
                            commonResponse.setMessage("array里含有不支持的数据类型" + innerType);
                            return commonResponse;
                        }
                        innerJsonArray.add(innerJsonObject);
                        jsonObject.put("nested", innerJsonArray);
                    } else {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("不支持的数据类型" + type);
                        return commonResponse;
                    }
                    jsonArray.add(jsonObject);
                    column.setType_detail(jsonArray.toJSONString());
                    columns.add(column);
                }
                metaDataTable.setColumns(columns);
                //判断这个表是否已经存在,存在就跳过
                if (metaDataTableMapper.findMetaDataTableByName(metaDataTable.getDatabase_name(), metaDataTable.getName(), metaDataTable.getTable_type()) == null) {
                    if (metaDataTableMapper.insertMetaDataTableHistoryCk(metaDataTable) < 1) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("插入数据失败,请联系管理员");
                        return commonResponse;
                    }
                    //关联数据域
                    DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("clickhouse"), metaDataSource.getDatahub_instance() + "." + metaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
                    if (!StringUtils.isEmpty(metaDataTable.getData_domain())) {
                        Domains domains = new Domains();
                        UrnArray urnArray = new UrnArray();
                        urnArray.add(new Urn(metaDataTable.getData_domain()));
                        domains.setDomains(urnArray);
                        addMetadata("dataset", datasetUrn, domains, emitter);
                    }
                    //关联用户
                    Ownership ownership = new Ownership();
                    OwnerArray ownerArray = new OwnerArray();
                    ownerArray.add(new Owner().setOwner(new Urn("urn:li:corpuser:" + metaDataTable.getCreator())).setType(OwnershipType.DATA_STEWARD));
                    ownership.setOwners(ownerArray);
                    addMetadata("dataset", datasetUrn, ownership, emitter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                commonResponse.setMessage(e.getMessage());
                commonResponse.setSuccess(false);
                return commonResponse;
            } finally {
                try {
                    pStemt.close();
                    conn.close();
                } catch (Exception tt) {
                    tt.printStackTrace();
                }
            }
        }
        return commonResponse;
    }

    public void executionIngestionDatahub(int tableType) {
        MetaDataSource metaDataSource = metaDataTableMapper.findMetaDataSource(tableType);
        if (metaDataSource == null || StringUtils.isEmpty(metaDataSource.getDatahub_instance()) || StringUtils.isEmpty(metaDataSource.getDatahub_ingestion_source())) {
            System.out.println("数据源不存在或者数据源的datahub_instance或者datahub_ingestion_source为空");
            return;
        }
        String dataHubIngestionSource = metaDataSource.getDatahub_ingestion_source();
        String bodyRequest = "{\"operationName\":\"createIngestionExecutionRequest\",\"variables\":{\"input\":{\"ingestionSourceUrn\":\"urn:li:dataHubIngestionSource:" + dataHubIngestionSource + "\"}},\"query\":\"mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {\\n  createIngestionExecutionRequest(input: $input)\\n}\\n\"}";
        System.out.println(bodyRequest);
        // 请求数据服务
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("Authorization", "Bearer " + datahubToken);
        // 将请求头部和参数合成一个请求
        HttpEntity<String> requestEntity = new HttpEntity<>(bodyRequest, headers);
        ResponseEntity<String> responseEntity = restTemplate.postForEntity(datahubGraphqlUrl, requestEntity, String.class);
        if (!responseEntity.getStatusCode().is2xxSuccessful()) {
            System.out.println("datahub ingestion不可用");
        }
    }

    public CommonResponse getDatahubUrn(HueDataSource hueDataSource) {
        CommonResponse commonResponse = new CommonResponse();
        return commonResponse;
    }

    public CommonResponse hdfsToHiveTableTransformed(HdfsToHiveTableLineReq hdfsToHiveTableLineReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (hdfsToHiveTableLineReq == null || hdfsToHiveTableLineReq.getHdfs() == null || hdfsToHiveTableLineReq.getHdfs().isEmpty() || StringUtils.isEmpty(hdfsToHiveTableLineReq.getDatabase()) || StringUtils.isEmpty(hdfsToHiveTableLineReq.getTable())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("hdfs和hive的库和表不能为空");
            return commonResponse;
        }
        MetaDataSource metaDataSource = metaDataTableMapper.findMetaDataSource(1);
        if (metaDataSource == null || StringUtils.isEmpty(metaDataSource.getDatahub_instance())) {
            System.out.println("hive数据源不存在或者数据源的datahub_instance为空");
            commonResponse.setSuccess(false);
            commonResponse.setMessage("hive数据源不存在或者数据源的datahub_instance为空");
            return commonResponse;
        }
        try {
            String tableName = hdfsToHiveTableLineReq.getDatabase() + "." + hdfsToHiveTableLineReq.getTable();
            UpstreamArray srcUpstreams = new UpstreamArray();
            for (String hdfsLocation : hdfsToHiveTableLineReq.getHdfs()) {
                //入hdfs节点
                hdfsLocation = getHdfsUrn(hdfsLocation);
                DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn("hdfs"), hdfsInstance + "/" + hdfsLocation, FabricType.PROD);
                addMetadata("dataset", datasetUrn, new DatasetProperties().setName("hdfs://nameservice1/" + hdfsLocation), emitter);
                //加入transformed集合
                Upstream upstream = new Upstream();
                upstream.setDataset(datasetUrn);
                upstream.setType(DatasetLineageType.TRANSFORMED);
                srcUpstreams.add(upstream);
            }
            UpstreamLineage upstreamLineage = new UpstreamLineage();
            upstreamLineage.setUpstreams(srcUpstreams);
            MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
                    .entityType("dataset")
                    .entityUrn(new DatasetUrn(new DataPlatformUrn("hive"), metaDataSource.getDatahub_instance() + "." + tableName, FabricType.PROD))
                    .upsert()
                    .aspect(upstreamLineage)
                    .build();
            // Blocking call using future
            Future<MetadataWriteResponse> requestFuture = emitter.emit(mcpw, null);
            MetadataWriteResponse mwr = requestFuture.get();
            if (!mwr.isSuccess()) {
                HttpResponse httpResponse = (HttpResponse) mwr.getUnderlyingResponse();
                System.out.println(String.format("失败发送元数据事件: %s, aspect: %s 状态码是: %d",
                        mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
                commonResponse.setSuccess(false);
                commonResponse.setMessage(String.format("失败发送元数据事件: %s, aspect: %s 状态码是: %d",
                        mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
        }
        return commonResponse;
    }

    public String getHdfsUrn(String hdfsPath) {
        if (hdfsPath.indexOf("hdfs:") >= 0) {
            hdfsPath = hdfsPath.substring(hdfsPath.indexOf("hdfs:") + 5);
        }
        if (hdfsPath.indexOf("${") > 0) {
            hdfsPath = hdfsPath.substring(0, hdfsPath.indexOf("${"));
            if (hdfsPath.lastIndexOf("/") > 0) {
                hdfsPath = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
            }
        }
        if (hdfsPath.length() == 0) {
            return "";
        }
        hdfsPath = hdfsPath.replaceAll("nameservice1", "");
        while (hdfsPath.charAt(0) == '/') {
            hdfsPath = hdfsPath.substring(1);
            if (hdfsPath.length() == 0) {
                return "";
            }
        }
        if (hdfsPath.length() == 0) {
            return "";
        }
        while (hdfsPath.charAt(hdfsPath.length() - 1) == '/') {
            hdfsPath = hdfsPath.substring(0, hdfsPath.length() - 1);
            if (hdfsPath.length() == 0) {
                return "";
            }
        }
        hdfsPath = hdfsPath.toLowerCase();
        return hdfsPath;
    }

    public CommonResponse addDataJobInputAndOutput(JobBloodLineReq jobBloodLineReq) {
        CommonResponse commonResponse = new CommonResponse();
        //创建dataJobUrn
        if (jobBloodLineReq == null || jobBloodLineReq.getHueDataSourceSrc() == null || jobBloodLineReq.getHueDataSourceSrc().isEmpty() || jobBloodLineReq.getHueDataSourceDest() == null
                || jobBloodLineReq.getHueDataSourceDest().isEmpty() || StringUtils.isEmpty(jobBloodLineReq.getName()) || StringUtils.isEmpty(jobBloodLineReq.getCreator()) || StringUtils.isEmpty(jobBloodLineReq.getType())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("job的src和dest,名字,创建者,类型不能为空");
            return commonResponse;
        }
        try {
            //创建源和目的地的节点
            DatasetUrnArray srcUrns = new DatasetUrnArray();
            DatasetUrnArray destUrns = new DatasetUrnArray();
            for (HueDataSource hueDataSource : jobBloodLineReq.getHueDataSourceSrc()) {
                DatasetUrn datasetUrn = addDataSourceMetaData(hueDataSource);
                if (datasetUrn == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(hueDataSource.toString() + ":未生成urn");
                    return commonResponse;
                }
                srcUrns.add(datasetUrn);
            }
            for (HueDataSource hueDataSource : jobBloodLineReq.getHueDataSourceDest()) {
                DatasetUrn datasetUrn = addDataSourceMetaData(hueDataSource);
                if (datasetUrn == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(hueDataSource.toString() + ":未生成urn");
                    return commonResponse;
                }
                destUrns.add(datasetUrn);
            }
            //创建flow
            StringMap customerPro = new StringMap();
            StringMap customerProJob = new StringMap();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar calendar = java.util.Calendar.getInstance();
            String nowDayDate = dateFormat.format(calendar.getTime());
            customerPro.put("name", jobBloodLineReq.getName());
            customerPro.put("owner", jobBloodLineReq.getCreator());
            customerPro.put("description", jobBloodLineReq.getDescription());
            customerPro.put("status", "manual");
            customerPro.put("scheduleTask", jobBloodLineReq.isScheduleTask() + "");
            customerPro.put("updateTime", nowDayDate);

            customerProJob.put("name", jobBloodLineReq.getName());
            customerProJob.put("owner", jobBloodLineReq.getCreator());
            customerProJob.put("description", jobBloodLineReq.getDescription());
            customerProJob.put("status", "manual");
            customerProJob.put("scheduleTask", jobBloodLineReq.isScheduleTask() + "");
            customerProJob.put("updateTime", nowDayDate);

            com.linkedin.common.urn.DataFlowUrn dataFlowUrn = new DataFlowUrn(workFlowPlat, "workflowManualId" + jobBloodLineReq.getId(), "prod");
            if (!addMetadata("dataFlow", dataFlowUrn, new DataFlowInfo().setDescription(jobBloodLineReq.getDescription()).setCustomProperties(customerPro).setName(jobBloodLineReq.getName()), emitter)) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("Pipeline入数据失败,请联系后端开发");
                return commonResponse;
            }

            //创建job
            customerProJob.put("type", jobBloodLineReq.getType());
            customerProJob.put("realTimeTask", jobBloodLineReq.isRealTimeTask() + "");
            if (!StringUtils.isEmpty(jobBloodLineReq.getLastRunTime())) {
                customerProJob.put("lastRunTime", jobBloodLineReq.getLastRunTime());
            }

            DataJobInfo.Type type = DataJobInfo.Type.create(AzkabanJobType.HADOOP_JAVA);
            com.linkedin.common.urn.DataJobUrn dataJobUrn = new DataJobUrn(dataFlowUrn, "taskManualId" + jobBloodLineReq.getId());
            addMetadata("dataJob", dataJobUrn, new DataJobInfo().setDescription(jobBloodLineReq.getDescription()).setCustomProperties(customerProJob).setName(jobBloodLineReq.getName()).setType(type), emitter);

            Ownership ownership = new Ownership();
            OwnerArray ownerArray = new OwnerArray();
            ownerArray.add(new Owner().setOwner(new Urn("urn:li:corpuser:" + jobBloodLineReq.getCreator())).setType(OwnershipType.DATA_STEWARD));
            ownership.setOwners(ownerArray);
            addMetadata("dataJob", dataJobUrn, ownership, emitter);


            //给datajob入关系
            DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();
            dataJobInputOutput.setInputDatasets(srcUrns);
            dataJobInputOutput.setOutputDatasets(destUrns);
            MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
                    .entityType("dataJob")
                    .entityUrn(dataJobUrn)
                    .upsert()
                    .aspect(dataJobInputOutput)
                    .aspectName("dataJobInputOutput")
                    .build();
            // Blocking call using future
            Future<MetadataWriteResponse> requestFuture = emitter.emit(mcpw, null);
            MetadataWriteResponse mwr = requestFuture.get();
            if (!mwr.isSuccess()) {
                HttpResponse httpResponse = (HttpResponse) mwr.getUnderlyingResponse();
                System.out.println(String.format("失败发送元数据事件: %s, aspect: %s 状态码是: %d",
                        mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
                commonResponse.setSuccess(false);
                commonResponse.setMessage(String.format("失败发送元数据事件: %s, aspect: %s 状态码是: %d",
                        mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
        }
        return commonResponse;
    }

    public DatasetUrn addDataSourceMetaData(HueDataSource hueDataSource) throws IOException, ExecutionException, InterruptedException {
        //根据url查询出来模块model,env,instance,
        FabricType fabricType = FabricType.PROD;
        String model = hueDataSource.getModel();
        String env = hueDataSource.getEnv();
        String instanceName = hueDataSource.getInstanceName();
        boolean isExistDataHub = hueDataSource.isJoinDatahub();
        if (StringUtils.isEmpty(env)) {
            return null;
        }
        if (env.contains("prod")) {
            fabricType = FabricType.PROD;
        } else if (env.contains("sit")) {
            fabricType = FabricType.TEST;
        } else if (env.contains("fit") || env.contains("uit")) {
            fabricType = FabricType.DEV;
        } else {
            fabricType = FabricType.$UNKNOWN;
        }
        String platform = hueDataSource.getType();
        if (platform.equals("mqtt")) {
            platform = "Mqtt";
        } else if (platform.equals("hive")) {
            isExistDataHub = true;
            MetaDataSource metaDataSource = metaDataTableMapper.findMetaDataSource(1);
            if (metaDataSource == null || StringUtils.isEmpty(metaDataSource.getDatahub_instance())) {
                System.out.println("hive数据源不存在或者数据源的datahub_instance为空");
                return null;
            }
            instanceName = metaDataSource.getDatahub_instance();
        } else if (platform.equals("clickhouse")) {
            isExistDataHub = true;
            MetaDataSource metaDataSource = metaDataTableMapper.findMetaDataSource(2);
            if (metaDataSource == null || StringUtils.isEmpty(metaDataSource.getDatahub_instance())) {
                System.out.println("clickhouse数据源不存在或者数据源的datahub_instance为空");
                return null;
            }
            instanceName = metaDataSource.getDatahub_instance();
        } else if (platform.equals("hdfs")) {
            isExistDataHub = true;
            instanceName = hdfsInstance;
        }
        String srcUrn = "";
        if ((!StringUtils.isEmpty(hueDataSource.getDatabase())) && (!StringUtils.isEmpty(hueDataSource.getTable()))) {
            srcUrn = hueDataSource.getDatabase() + "." + hueDataSource.getTable();
        } else if (!StringUtils.isEmpty(hueDataSource.getTopic())) {
            srcUrn = hueDataSource.getTopic();
        } else if (!StringUtils.isEmpty(hueDataSource.getHdfsPath())) {
            srcUrn = getHdfsUrn(hueDataSource.getHdfsPath());
        } else {
            return null;
        }

        if (!isExistDataHub) {
            instanceName = model + "_" + env;
        }
        DatasetUrn datasetUrn = new DatasetUrn(new DataPlatformUrn(platform), instanceName + "." + srcUrn, fabricType);
        if (platform.equals("hdfs")) {
            datasetUrn = new DatasetUrn(new DataPlatformUrn("hdfs"), hdfsInstance + "/" + srcUrn, FabricType.PROD);
            addMetadata("dataset", datasetUrn, new DatasetProperties().setName("hdfs://nameservice1/" + srcUrn), emitter);
        }
        if (!isExistDataHub) {
            //先入datahub节点
            String name = srcUrn;
            if (srcUrn.lastIndexOf(".") > -1) {
                name = srcUrn.substring(srcUrn.lastIndexOf(".") + 1);
            }
            addMetadata("dataset", datasetUrn, new DatasetProperties().setDescription(hueDataSource.getHost() + ":" + hueDataSource.getPort()).setName(name), emitter);
            StringArray types = new StringArray();
            if (platform.equals("mysql")) {
                types.add("Table");
            } else if (platform.equals("mongodb")) {
                types.add("Collections");
            } else if (platform.equals("kafka") || platform.equals("Mqtt")) {
                types.add("Topic");
            }
            SubTypes subTypes = new SubTypes();
            subTypes.setTypeNames(types);
            if (!types.isEmpty()) {
                addMetadata("dataset", datasetUrn, subTypes, emitter);
            }
        }
        return datasetUrn;
    }

    public CommonResponse hiveScriptBloodLine(HiveScriptBloodLineReq hiveScriptBloodLineReq) {
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(hiveScriptBloodLineReq.getHiveScript())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("hive脚本不能为空");
            return commonResponse;
        }
        Map<String, Set<String>> hdfsToHiveTable = new HashMap<>();
        String sql = hiveScriptBloodLineReq.getHiveScript();
        sql = sql.toLowerCase();
        if (sql.contains("alter table") && sql.contains("partition") && sql.contains("location")) {
            String[] alterSqls = sql.split(";");
            for (String alterSql : alterSqls) {
                alterSql = alterSql.trim();
                if ((!StringUtils.isEmpty(alterSql)) && (!alterSql.contains("--alter table")) && alterSql.indexOf("alter table") > -1 && alterSql.indexOf("location ") > -1 && alterSql.indexOf("partition") > -1 && alterSql.indexOf(")") > -1) {
                    String tableName = alterSql.substring(alterSql.indexOf("alter table") + 11);
                    String location = alterSql.substring(alterSql.indexOf("location ") + 10).replaceAll("\"", "'");
                    String partion = alterSql.substring(alterSql.indexOf("partition") + 9, alterSql.indexOf(")")).trim();
                    partion = partion.substring(1).replaceAll(" ", "");
                    int partionRuleNum = partion.split(",").length;
                    if (tableName.indexOf(" add") > -1 && location.indexOf("'") > -1) {
                        tableName = tableName.substring(0, tableName.indexOf(" add")).replaceAll("`", "").trim();
                        location = location.substring(0, location.indexOf("'"));
                        if (location.charAt(location.length() - 1) == '/') {
                            location = location.substring(0, location.length() - 1);
                        }
                        if (partionRuleNum == 1) {
                            location = getHdfsUrn(location);
                        } else if (partionRuleNum == 2 || tableName.equals("sv.asr_ctrl") || tableName.equals("cdmods.ods_rcu_event_02_i_d")) {
                            //移除后两个
                            location = getHdfsUrn(removeTailPath(2, location));
                        } else {
                            location = getHdfsUrn(removeTailPath(partionRuleNum, location));
                        }
                        if (hdfsToHiveTable.containsKey(tableName)) {
                            Set<String> hdfs = hdfsToHiveTable.get(tableName);
                            hdfs.add(location);
                            hdfsToHiveTable.put(tableName, hdfs);
                        } else {
                            Set<String> hdfs = new HashSet<>();
                            hdfs.add(location);
                            hdfsToHiveTable.put(tableName, hdfs);
                        }
                    }
                }
            }
            //hdfsToHiveTable 入datahub
            System.out.println("\nhdfs加载到hive表的血缘关联");
            for (Map.Entry<String, Set<String>> entry : hdfsToHiveTable.entrySet()) {
                String mapKey = entry.getKey();
                Set<String> mapValue = entry.getValue();
                System.out.println("hdfs:" + mapValue + "     to     hive:" + mapKey);
                HdfsToHiveTableLineReq hdfsToHiveTableLineReq = new HdfsToHiveTableLineReq();
                hdfsToHiveTableLineReq.setHdfs(mapValue);
                String[] tableInfo = mapKey.split("\\.");
                hdfsToHiveTableLineReq.setDatabase(tableInfo[0]);
                hdfsToHiveTableLineReq.setTable(tableInfo[1]);
                CommonResponse tmpCommonResponse = hdfsToHiveTableTransformed(hdfsToHiveTableLineReq);
                if (!tmpCommonResponse.isSuccess()) {
                    return tmpCommonResponse;
                }
            }
        } else {
            //解析输入
            JobBloodLineReq jobBloodLineReq = new JobBloodLineReq();
            jobBloodLineReq.setName("手动执行hive脚本");
            jobBloodLineReq.setCreator(hiveScriptBloodLineReq.getCreator());
            jobBloodLineReq.setType("hive");
            jobBloodLineReq.setRealTimeTask(false);
            jobBloodLineReq.setScheduleTask(false);
            jobBloodLineReq.setDescription("手动执行hive脚本");
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar calendar = java.util.Calendar.getInstance();
            String nowDayDate = dateFormat.format(calendar.getTime());
            jobBloodLineReq.setLastRunTime(nowDayDate);
            jobBloodLineReq.setId(System.currentTimeMillis() + "");
            List<HueDataSource> hueDataSourceSrc = new ArrayList<>();
            List<HueDataSource> hueDataSourceDest = new ArrayList<>();
            sql = sql.toLowerCase();
            String[] sqlArray = sql.split(";");
            sql = "";
            for (String sqlRow : sqlArray) {
                sqlRow = sqlRow.trim();
                if (!(sqlRow.startsWith("set ") || (sqlRow.startsWith("--")) && sqlRow.contains("\r\nset "))) {
                    sql = sql + sqlRow + ";";
                }
            }
            HiveSchemaStatVisitor visitor = null;
            try {
                SQLStatementParser hive = SQLParserUtils.createSQLStatementParser(sql, DbType.hive);
                SQLStatement statement = hive.parseStatement();
                visitor = new HiveSchemaStatVisitor();
                statement.accept(visitor);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("错误,sql解析异常");
            }
            Map<TableStat.Name, TableStat> tables = visitor.getTables();
            Set<TableStat.Name> tableNameSet = tables.keySet();
            for (Map.Entry<TableStat.Name, TableStat> entry : tables.entrySet()) {
                String name = entry.getKey().getName();
                TableStat tableStat = entry.getValue();
                HueDataSource hueDataSource = new HueDataSource();
                hueDataSource.setEnv("prod");
                hueDataSource.setType("hive");
                hueDataSource.setJoinDatahub(true);
                if (tableStat.getInsertCount() > 0) {
                    if (name.contains(".")) {
                        String[] tableInfo = name.split("\\.");
                        hueDataSource.setDatabase(tableInfo[0]);
                        hueDataSource.setTable(tableInfo[1]);
                        hueDataSourceDest.add(hueDataSource);
                    } else {
                        System.out.println("表名不合法：" + name);
                        commonResponse.setMessage("表名不合法：" + name);
                        commonResponse.setSuccess(false);
                        return commonResponse;
                    }
                } else {
                    if (name.contains(".")) {
                        String[] tableInfo = name.split("\\.");
                        hueDataSource.setDatabase(tableInfo[0]);
                        hueDataSource.setTable(tableInfo[1]);
                        hueDataSourceSrc.add(hueDataSource);
                    } else {
                        System.out.println("表名不合法：" + name);
                        commonResponse.setMessage("表名不合法：" + name);
                        commonResponse.setSuccess(false);
                        return commonResponse;
                    }
                }
            }
            if (hueDataSourceDest.isEmpty() || hueDataSourceSrc.isEmpty()) {
                commonResponse.setMessage("此脚本没有血缘信息");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
            jobBloodLineReq.setHueDataSourceSrc(hueDataSourceSrc);
            jobBloodLineReq.setHueDataSourceDest(hueDataSourceDest);
            //入datahub
            return addDataJobInputAndOutput(jobBloodLineReq);
        }

        return commonResponse;
    }

    public String removeTailPath(int num, String path) {
        if (num <= 0) {
            return path;
        }
        while (num > 0) {
            int lastLocation = path.lastIndexOf("/");
            if (lastLocation > -1) {
                path = path.substring(0, lastLocation);
                num--;
            } else {
                break;
            }

        }
        return path;
    }

    public CommonResponse syncMetaDataDomainAndOwner(Integer table_type, String database_name, String name) {
        CommonResponse commonResponse = new CommonResponse();
        String condition = "m.deleted=0";
        if(table_type!=null){
            condition = condition + " and m.table_type=" + table_type;
        }
        if(!StringUtils.isEmpty(database_name)){
            condition = condition + " and m.database_name='" + database_name + "'";
        }
        if(!StringUtils.isEmpty(name)){
            condition = condition + " and m.name='" + name + "'";
        }
        List<MetaDataTableExtendInfo> tables = metaDataTableMapper.findMetaDataTable(condition);
        for (MetaDataTableExtendInfo metaDataTable : tables) {
            try {
                DatasetUrn datasetUrn = null;
                MetaDataSource metaDataSource = metaDataTableMapper.findMetaDataSource(metaDataTable.getTable_type());
                if (metaDataSource == null || StringUtils.isEmpty(metaDataSource.getDatahub_instance()) || StringUtils.isEmpty(metaDataSource.getDatahub_ingestion_source())) {
                    System.out.println("数据源不存在或者数据源的datahub_instance或者datahub_ingestion_source为空");
                    return commonResponse;
                }
                if (metaDataTable.getTable_type() == 1) {
                    datasetUrn = new DatasetUrn(new DataPlatformUrn("hive"), metaDataSource.getDatahub_instance() + "." + metaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
                } else if (metaDataTable.getTable_type() == 2) {
                    datasetUrn = new DatasetUrn(new DataPlatformUrn("clickhouse"), metaDataSource.getDatahub_instance() + "." + metaDataTable.getDatabase_name() + "." + metaDataTable.getName(), FabricType.PROD);
                }
                //关联数据域
                if(!StringUtils.isEmpty(metaDataTable.getData_domain())){
                    Domains domains = new Domains();
                    UrnArray urnArray = new UrnArray();
                    urnArray.add(new Urn(metaDataTable.getData_domain()));
                    domains.setDomains(urnArray);
                    addMetadata("dataset", datasetUrn, domains, emitter);
                }
                //入ownerShip
                if(!StringUtils.isEmpty(metaDataTable.getCreator())) {
                    Ownership ownership = new Ownership();
                    OwnerArray ownerArray = new OwnerArray();
                    ownerArray.add(new Owner().setOwner(new Urn("urn:li:corpuser:" + metaDataTable.getCreator())).setType(OwnershipType.DATA_STEWARD));
                    ownership.setOwners(ownerArray);
                    addMetadata("dataset", datasetUrn, ownership, emitter);
                }
            } catch (Exception e) {
                e.printStackTrace();
                commonResponse.setSuccess(false);
                commonResponse.setMessage("同步数据域和owner失败");
                return commonResponse;
            }
        }
        return commonResponse;
    }
}
