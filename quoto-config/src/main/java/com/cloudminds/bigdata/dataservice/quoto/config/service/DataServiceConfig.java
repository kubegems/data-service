package com.cloudminds.bigdata.dataservice.quoto.config.service;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.utils.StringUtils;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.cloudminds.bigdata.dataservice.quoto.config.amazons3.OssUtils;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.config.mapper.*;
import com.cloudminds.bigdata.dataservice.quoto.config.redis.RedisUtil;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DataServiceConfig {
    @Autowired
    private ColumnAliasMapper columnAliasMapper;
    @Autowired
    private DatabaseInfoMapper databaseInfoMapper;
    @Autowired
    private QuotoInfoMapper quotoInfoMapper;
    @Autowired
    private TableInfoMapper tableInfoMapper;
    @Autowired
    private ApiDocMapper apiDocMapper;
    @Autowired
    private UserTokenMapper userTokenMapper;
    @Autowired
    private RedisUtil redisUtil;
    @Autowired
    RestTemplate restTemplate;
    @Value("${dataServiceUrl}")
    private String dataServiceUrl;
    @Value("${accessKey}")
    private String accessKey;
    @Value("${secretKey}")
    private String secretKey;
    @Value("${endpoint}")
    private String endpoint;
    @Value("${bucketName}")
    private String bucketName;
    @Value("${commonDataserviceUrl}")
    private String commonDataserviceUrl;
    @Value("${commonDataserviceName}")
    private String commonDataserviceName;

    // columnAlias
    public CommonResponse getColumnAlias(int tableId) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(columnAliasMapper.getColumnAliasByTableId(tableId));
        return commonResponse;
    }

    public CommonResponse updateColumnAliasStatus(int id, int status) {
        CommonResponse commonResponse = new CommonResponse();
        ColumnAlias columnAlias = columnAliasMapper.getColumnAliasById(id);
        if (columnAlias == null) {
            commonResponse.setMessage("列不存在");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (columnAlias.isMetric()) {
            if (status == 0 && columnAlias.getState() == 1) {
                String name = quotoInfoMapper.getQuotoByMetric(columnAlias.getColumn_alias(), columnAlias.getTable_id());
                if (!StringUtils.isEmpty(name)) {
                    commonResponse.setMessage("有激活的指标(" + name + ")运行,不能禁用！");
                    commonResponse.setSuccess(false);
                    return commonResponse;
                }
            }
        }
        if (columnAliasMapper.updateColumnAliasStatus(id, status) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        refreshDataService(columnAlias.getTable_id());
        return commonResponse;
    }

    public CommonResponse deleteColumnAlias(int id) {
        CommonResponse commonResponse = new CommonResponse();
        ColumnAlias columnAlias = columnAliasMapper.getColumnAliasById(id);
        if (columnAlias == null) {
            commonResponse.setMessage("列不存在");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (columnAlias.isMetric()) {
            //判断是否有原址指标在使用
            String name = quotoInfoMapper.getQuotoByMetric(columnAlias.getColumn_alias(), columnAlias.getTable_id());
            if (!StringUtils.isEmpty(name)) {
                commonResponse.setMessage("有激活的指标(" + name + ")运行,不能取消！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (columnAliasMapper.updateColumnAliasDelete(id, 1) != 1) {
            commonResponse.setMessage("删除失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        refreshDataService(columnAlias.getTable_id());
        return commonResponse;
    }

    public CommonResponse insertColumnAlias(ColumnAlias columnAlias) {
        if (columnAlias == null || StringUtils.isEmpty(columnAlias.getColumn_name()) || StringUtils.isEmpty(columnAlias.getColumn_alias())) {
            CommonResponse commonResponse = new CommonResponse();
            commonResponse.setMessage("列名和列别名不能为空");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        CommonResponse commonResponse = new CommonResponse();
        ColumnAlias columnAliasOld = columnAliasMapper.getColumnAlias(columnAlias);
        if (columnAliasOld != null) {
            if (columnAliasOld.getIs_delete() == 0) {
                commonResponse.setMessage("数据已存在,请不要重复新增！");
                commonResponse.setSuccess(false);
                return commonResponse;
            } else {
                columnAliasOld.setDes(columnAlias.getDes());
                columnAliasOld.setColumn_alias(columnAlias.getColumn_alias());
                String dataType = getColunmType(columnAlias.getTable_id(), columnAlias.getColumn_name());
                if (dataType == null) {
                    commonResponse.setMessage("列不存在或者查询列的数据类型失败！");
                    commonResponse.setSuccess(false);
                    return commonResponse;
                }
                columnAliasOld.setData_type(dataType);
                if (columnAliasMapper.updateColumnAlias(columnAliasOld) != 1) {
                    commonResponse.setMessage("新增数据失败,请稍后再试！");
                    commonResponse.setSuccess(false);
                    return commonResponse;
                }
            }
        } else {
            String dataType = getColunmType(columnAlias.getTable_id(), columnAlias.getColumn_name());
            if (dataType == null) {
                commonResponse.setMessage("列不存在或者查询列的数据类型失败！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
            columnAlias.setData_type(dataType);
            if (columnAliasMapper.insertColumnAlias(columnAlias) != 1) {
                commonResponse.setMessage("新增数据失败,请稍后再试！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        refreshDataService(columnAlias.getTable_id());
        return commonResponse;
    }

    public CommonResponse updateColumnAlias(ColumnAlias columnAlias) {
        CommonResponse commonResponse = new CommonResponse();
        // 查询列信息
        ColumnAlias oldColumnAlias = columnAliasMapper.getColumnAliasById(columnAlias.getId());
        if (oldColumnAlias == null) {
            commonResponse.setMessage("列不存！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        // 如果列名没有发生变化,就不用去查询数据类型
        if (columnAlias.getColumn_name().equals(oldColumnAlias.getColumn_name())) {
            columnAlias.setData_type(oldColumnAlias.getData_type());
        } else {
            String dataType = getColunmType(columnAliasMapper.getColumnAliasById(columnAlias.getId()).getTable_id(),
                    columnAlias.getColumn_name());
            if (dataType == null) {
                commonResponse.setMessage("列不存在或者查询列的数据类型失败！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
            columnAlias.setData_type(dataType);
        }

        if (columnAliasMapper.updateColumnAlias(columnAlias) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (oldColumnAlias.isMetric() && (!columnAlias.getColumn_alias().equals(oldColumnAlias.getColumn_alias()))) {
            //更新原子指标里面的字段
            quotoInfoMapper.updateAtomQuotoMetric(oldColumnAlias.getColumn_alias(), columnAlias.getColumn_alias(), columnAlias.getTable_id());
        }
        refreshDataService(oldColumnAlias.getTable_id());
        return commonResponse;
    }

    // datainfo
    public CommonResponse getdbInfo(Integer common_service) {
        CommonResponse commonResponse = new CommonResponse();
        if (common_service == null) {
            common_service = 0;
        }
        commonResponse.setData(databaseInfoMapper.getdbInfo(common_service));
        return commonResponse;
    }

    // database
    public CommonResponse getDataBase() {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(databaseInfoMapper.getDataBase());
        return commonResponse;
    }

    public CommonResponse getDataBaseBydbId(int dbId) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(databaseInfoMapper.getDataBaseByDbid(dbId));
        return commonResponse;
    }

    public CommonResponse updateDatabaseInfoStatus(int id, int status) {
        CommonResponse commonResponse = new CommonResponse();
        if (databaseInfoMapper.updateDatabaseInfoStatus(id, status) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse deleteDatabaseInfo(int id) {
        CommonResponse commonResponse = new CommonResponse();
        //查询库下面是否有表
        List<TableInfo> tableInfos = tableInfoMapper.getTableInfoByDataBaseId(id);
        if (tableInfos != null && tableInfos.size() > 0) {
            commonResponse.setMessage("该库下面有表,请先删除表！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (databaseInfoMapper.updateDatabaseInfoDelete(id, 1) != 1) {
            commonResponse.setMessage("删除失败,请稍后再试！");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse insertDatabaseInfo(DatabaseInfo databaseInfo) {
        CommonResponse commonResponse = new CommonResponse();
        DatabaseInfo databaseInfoOld = databaseInfoMapper.getDatabaseInfo(databaseInfo);
        if (databaseInfoOld != null) {
            if (databaseInfoOld.getIs_delete() == 0) {
                commonResponse.setMessage("数据已存在,请不要重复新增！");
                commonResponse.setSuccess(false);
                return commonResponse;
            } else {
                if (databaseInfoMapper.updateDatabaseInfoDelete(databaseInfoOld.getId(), 0) != 1) {
                    commonResponse.setMessage("新增数据失败,请稍后再试！");
                    commonResponse.setSuccess(false);
                    return commonResponse;
                }
                databaseInfo.setId(databaseInfoOld.getId());
                databaseInfoMapper.updateDataBaseInfo(databaseInfo);
                return commonResponse;
            }
        } else {
            if (databaseInfoMapper.insertDatabaseInfo(databaseInfo) != 1) {
                commonResponse.setMessage("新增数据失败,请稍后再试！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        return commonResponse;
    }

    // quotoInfo
    public CommonResponse getQuotoInfo(int tableId) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(quotoInfoMapper.getQuotoInfoByTableId(tableId));
        return commonResponse;
    }

    public CommonResponse updateQuotoInfoStatus(int id, int status) {
        CommonResponse commonResponse = new CommonResponse();
        QuotoInfo quotoInfo = quotoInfoMapper.getQuotoInfoById(id);
        if (quotoInfo == null) {
            commonResponse.setMessage("指标信息不存在！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (status == 0 && quotoInfo.getState() == 1) {
            String name = quotoInfoMapper.getQuotoByMetric(quotoInfo.getQuoto_name(), quotoInfo.getTable_id());
            if (!StringUtils.isEmpty(name)) {
                commonResponse.setMessage("有激活的指标(" + name + ")运行,不能禁用！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (quotoInfoMapper.updateQuotoInfoStatus(id, status) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        refreshDataService(quotoInfo.getTable_id());
        return commonResponse;
    }

    public CommonResponse deleteQuotoInfo(int id) {
        CommonResponse commonResponse = new CommonResponse();
        // 查询指标信息
        QuotoInfo quotoInfo = quotoInfoMapper.getQuotoInfoById(id);
        if (quotoInfo == null) {
            commonResponse.setMessage("指标信息不存在！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (quotoInfo.getState() == 1) {
            String name = quotoInfoMapper.getQuotoByMetric(quotoInfo.getQuoto_name(), quotoInfo.getTable_id());
            if (!StringUtils.isEmpty(name)) {
                commonResponse.setMessage("有激活的指标(" + name + ")运行,不能删除！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (quotoInfoMapper.updateQuotoInfoDelete(id, 1) != 1) {
            commonResponse.setMessage("删除失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        refreshDataService(quotoInfo.getTable_id());
        return commonResponse;
    }

    @SuppressWarnings("deprecation")
    public CommonResponse insertQuotoInfo(QuotoInfo quotoInfo) {
        CommonResponse commonResponse = new CommonResponse();
        QuotoInfo quotoInfoOld = quotoInfoMapper.getQuotoInfo(quotoInfo);
        if (quotoInfoOld != null) {
            if (quotoInfoOld.getIs_delete() == 0) {
                commonResponse.setMessage("指标名已存在,请不要重复新增！");
                commonResponse.setSuccess(false);
            } else {
                quotoInfoOld.setDes(quotoInfo.getDes());
                quotoInfoOld.setQuoto_sql(quotoInfo.getQuoto_sql());
                if (quotoInfoMapper.updateQuotoInfo(quotoInfoOld) != 1) {
                    commonResponse.setMessage("新增数据失败,请稍后再试！");
                    commonResponse.setSuccess(false);
                }
            }
        } else {
            // 判断是否符合规则
            if (NumberUtils.isNumber(quotoInfo.getQuoto_name()) || quotoInfo.getQuoto_name().contains("(")
                    || quotoInfo.getQuoto_name().contains(")") || quotoInfo.getQuoto_name().contains("+")
                    || quotoInfo.getQuoto_name().contains("-") || quotoInfo.getQuoto_name().contains("*")
                    || quotoInfo.getQuoto_name().contains("/") || quotoInfo.getQuoto_name().contains("#")
                    || quotoInfo.getQuoto_name().contains("&") || isContainChinese(quotoInfo.getQuoto_name())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("指标名不能是数、中文或者含有()+-*/&#特殊符号");
                return commonResponse;
            }
            //判断是否和列名重复
            if (columnAliasMapper.getColumnAliasByColumnAlias(quotoInfo.getTable_id(), quotoInfo.getQuoto_name()) != null) {
                commonResponse.setMessage("不能和列别名相同!");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
            if (quotoInfoMapper.insertQuotoInfo(quotoInfo) != 1) {
                commonResponse.setMessage("新增数据失败,请稍后再试！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
            refreshDataService(quotoInfo.getTable_id());
        }
        return commonResponse;
    }

    @SuppressWarnings("deprecation")
    public CommonResponse updateQuotoInfo(QuotoInfo quotoInfo) {
        CommonResponse commonResponse = new CommonResponse();
        QuotoInfo oldQuotoInfo = quotoInfoMapper.getQuotoInfoById(quotoInfo.getId());
        if (oldQuotoInfo == null) {
            commonResponse.setMessage("指标不存在！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        // 判断是否有同名的指标
        if (!oldQuotoInfo.getQuoto_name().equals(quotoInfo.getQuoto_name())) {
            // 判断是否符合规则
            if (NumberUtils.isNumber(quotoInfo.getQuoto_name()) || quotoInfo.getQuoto_name().contains("(")
                    || quotoInfo.getQuoto_name().contains(")") || quotoInfo.getQuoto_name().contains("+")
                    || quotoInfo.getQuoto_name().contains("-") || quotoInfo.getQuoto_name().contains("*")
                    || quotoInfo.getQuoto_name().contains("/") || quotoInfo.getQuoto_name().contains("#")
                    || quotoInfo.getQuoto_name().contains("&") || isContainChinese(quotoInfo.getQuoto_name())) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("指标名不能是数、中文或者含有()+-*/&#特殊符号");
                return commonResponse;
            }
            if (quotoInfoMapper.getQuotoInfoByQuotoName(quotoInfo.getQuoto_name(), oldQuotoInfo.getTable_id()) != null) {
                commonResponse.setMessage("指标名已存在！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
            if (columnAliasMapper.getColumnAliasByColumnAlias(quotoInfo.getTable_id(), quotoInfo.getQuoto_name()) != null) {
                commonResponse.setMessage("不能和列别名相同!");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (quotoInfoMapper.updateQuotoInfo(quotoInfo) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (!oldQuotoInfo.getQuoto_name().equals(quotoInfo.getQuoto_name())) {
            //更新原子指标里面的字段
            quotoInfoMapper.updateAtomQuotoMetric(oldQuotoInfo.getQuoto_name(), quotoInfo.getQuoto_name(), oldQuotoInfo.getTable_id());
        }
        refreshDataService(oldQuotoInfo.getTable_id());
        return commonResponse;
    }

    // tableInfo
    public CommonResponse getTableInfo(int databaseId) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tableInfoMapper.getTableInfoByDataBaseId(databaseId));
        return commonResponse;
    }

    public CommonResponse updateTableInfoStatus(int id, int status) {
        CommonResponse commonResponse = new CommonResponse();
        // 表禁用需要表下面没有列别名和指标
        if (status == 0 && tableInfoMapper.relateQuotoOrColumnNum(id) > 0) {
            commonResponse.setMessage("表禁用需要表下面没有列别名和指标！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (tableInfoMapper.updateTableInfoStatus(id, status) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        refreshDataService(id);
        return commonResponse;
    }

    public CommonResponse deleteTableInfo(int id) {
        CommonResponse commonResponse = new CommonResponse();
        //查询表是否有指标在用
        String quotoName = tableInfoMapper.getRelationQuotoName(id);
        if (quotoName != null) {
            commonResponse.setMessage("表关联了指标(" + quotoName + "),请删除或禁用后再操作!");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        //删除表下的列和quotoinfo
        try {
            columnAliasMapper.updateColumnAliasDeleteByTableId(id, 1);
            quotoInfoMapper.updateQuotoInfoDeleteByTableId(id, 1);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("列别名或指标信息数据库删除失败,请联系管理员手动执行");
            return commonResponse;
        }
        // 表删除需要表下面没有列别名和指标
        if (tableInfoMapper.relateQuotoOrColumnNum(id) > 0) {
            commonResponse.setMessage("表删除需要表下面没有列别名和指标！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }

        if (tableInfoMapper.updateTableInfoDelete(id, 1) != 1) {
            commonResponse.setMessage("删除失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        refreshDataService(id);
        return commonResponse;
    }

    public CommonResponse insertTableInfo(TableInfo tableInfo) {
        CommonResponse commonResponse = new CommonResponse();
        if (tableInfo == null || StringUtils.isEmpty(tableInfo.getTable_name()) || StringUtils.isEmpty(tableInfo.getTable_alias())) {
            commonResponse.setMessage("表名和表别名不能为空！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (tableInfo.getTable_alias().contains(".")) {
            commonResponse.setMessage("表别名不能带.符号！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        TableInfo existTable = tableInfoMapper.getTableInfoByTableAlias(tableInfo);
        if (existTable != null && existTable.getIs_delete() == 0) {
            commonResponse.setMessage("此别名已存在,请重新命名！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        TableInfo tableInfoOld = tableInfoMapper.getTableInfo(tableInfo);
        if (tableInfoOld != null) {
            if (tableInfoOld.getIs_delete() == 0) {
                commonResponse.setMessage("表名已经存在,请不要重复新增！");
                commonResponse.setSuccess(false);
            } else {
                tableInfoOld.setTable_alias(tableInfo.getTable_alias());
                tableInfoOld.setDes(tableInfo.getDes());
                tableInfoOld.setTheme_id(tableInfo.getTheme_id());
                if (tableInfoMapper.updateTableInfo(tableInfoOld) != 1) {
                    commonResponse.setMessage("新增数据失败,请稍后再试！");
                    commonResponse.setSuccess(false);
                }
                tableInfo.setId(tableInfoOld.getId());
            }
        } else {
            if (tableInfoMapper.insertTableInfo(tableInfo) != 1) {
                commonResponse.setMessage("新增数据失败,请稍后再试！");
                commonResponse.setSuccess(false);
            }
        }
        if (commonResponse.isSuccess()) {
            commonResponse.setMessage(null);
            CommonResponse commonResponse1 = insertColumnAlias(tableInfo.getId());
            if (!commonResponse1.isSuccess()) {
                commonResponse.setMessage(commonResponse1.getMessage());
            }
            QuotoInfo quotoInfo = new QuotoInfo();
            quotoInfo.setTable_id(tableInfo.getId());
            quotoInfo.setQuoto_sql("count(*)");
            quotoInfo.setQuoto_name("cnt");
            quotoInfo.setDes("总数");
            insertQuotoInfo(quotoInfo);
            refreshDataService(tableInfo.getId());
        }
        return commonResponse;
    }

    public CommonResponse refreshTableCloumn(int id) {
        CommonResponse commonResponse = new CommonResponse();
        //查询表
        TableInfo tableInfo = tableInfoMapper.getTableInfoById(id);
        if (tableInfo == null) {
            commonResponse.setMessage("表不存在,请刷新界面后再操作！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (tableInfo.getState() == 0) {
            commonResponse.setMessage("表不可用，请先启用表");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        //删除表下的列
        try {
            columnAliasMapper.updateColumnAliasDeleteByTableId(id, 1);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage("列别名数据库删除失败,请联系管理员");
            return commonResponse;
        }
        commonResponse = insertColumnAlias(tableInfo.getId());
        refreshDataService(id);
        return commonResponse;
    }

    public CommonResponse updateTableInfo(TableInfo tableInfo) {
        CommonResponse commonResponse = new CommonResponse();
        TableInfo oldTableInfo = tableInfoMapper.getTableInfoById(tableInfo.getId());
        if (oldTableInfo == null) {
            commonResponse.setMessage("原始表不存在！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (!tableInfo.getTable_alias().equals(oldTableInfo.getTable_alias())) {
            TableInfo existTable = tableInfoMapper.getTableInfoByTableAlias(tableInfo);
            if (existTable != null && existTable.getIs_delete() == 0) {
                commonResponse.setMessage("此别名已存在,请重新命名！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (!tableInfo.getTable_name().equals(oldTableInfo.getTable_name())) {
            TableInfo existTable = tableInfoMapper.getTableInfo(tableInfo);
            if (existTable != null && existTable.getIs_delete() == 0) {
                commonResponse.setMessage("此表已经存在了,请不要重复添加！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (tableInfoMapper.updateTableInfo(tableInfo) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        refreshDataService(tableInfo.getId());
        return commonResponse;
    }

    public String getColunmType(int tableId, String columnName) {
        DbConnInfo dbConnInfo = databaseInfoMapper.getdbConnInfoByTableId(tableId);
        Connection conn = null;
        // 与数据库的连接
        PreparedStatement pStemt = null;
        try {
            String url = dbConnInfo.getDb_url();
            String dbType = url.substring(5, url.indexOf(":", 5));
            String sql = "SELECT \"" + columnName + "\" FROM \"" + dbConnInfo.getDatabase() + "\".\""
                    + dbConnInfo.getTable_name() + "\" limit 1";
            if (dbType.equals("postgresql")) {
                url = url + "/" + dbConnInfo.getDatabase();
                sql = "SELECT " + columnName + " FROM " + dbConnInfo.getDatabase() + "."
                        + dbConnInfo.getTable_name() + " limit 1";
            } else if (dbType.equals("sqlserver")) {
                sql = "SELECT top 1 " + columnName + " FROM " + dbConnInfo.getDatabase() + "."
                        + dbConnInfo.getTable_name();
            } else if (dbType.equals("oracle")) {
                sql = "SELECT " + columnName + " FROM " + dbConnInfo.getDatabase() + "."
                        + dbConnInfo.getTable_name() + " where rownum=1";
            }
            conn = DriverManager.getConnection(url, dbConnInfo.getUserName(),
                    dbConnInfo.getPassword());
            pStemt = conn.prepareStatement(sql);
            ResultSet set = pStemt.executeQuery();
            // 结果集元数据
            ResultSetMetaData rsmd = set.getMetaData();
            return rsmd.getColumnTypeName(1);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
        return null;
    }

    public CommonResponse getApiDoc() {
        // TODO Auto-generated method stub
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(apiDocMapper.getApiDoc());
        return commonResponse;
    }

    public CommonResponse insertColumnAlias(int tableId) {
        CommonResponse commonResponse = new CommonResponse();
        DbConnInfo dbConnInfo = databaseInfoMapper.getdbConnInfoByTableId(tableId);
        if (dbConnInfo == null) {
            commonResponse.setMessage("tableId:" + tableId + " 不存在");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        String url = dbConnInfo.getDb_url();
        String dbType = url.substring(5, url.indexOf(":", 5));
        if (dbType.equals("clickhouse")) {
            Connection conn = null;
            // 与数据库的连接
            PreparedStatement pStemt = null;
            try {
                conn = DriverManager.getConnection(url, dbConnInfo.getUserName(), dbConnInfo.getPassword());
                pStemt = conn.prepareStatement("SELECT name,type,comment FROM system.columns where database='"
                        + dbConnInfo.getDatabase() + "' and table='" + dbConnInfo.getTable_name() + "'");
                ResultSet set = pStemt.executeQuery();
                while (set.next()) {
                    ColumnAlias columnAlias = new ColumnAlias();
                    columnAlias.setColumn_name(set.getString("name"));
                    columnAlias.setColumn_alias(set.getString("name"));
                    columnAlias.setData_type(set.getString("type"));
                    columnAlias.setDes(set.getString("comment"));
                    columnAlias.setTable_id(tableId);
                    if (columnAliasMapper.insertColumnAlias(columnAlias) != 1) {
                        commonResponse.setMessage("插入列" + columnAlias.getColumn_name() + "失败,请稍后再试！");
                        commonResponse.setSuccess(false);
                        return commonResponse;
                    }
                }

            } catch (
                    SQLException e) {
                e.printStackTrace();
                commonResponse.setSuccess(false);
                commonResponse.setMessage(e.getMessage());
                return commonResponse;
            } finally {
                if (pStemt != null) {
                    try {
                        pStemt.close();
                        conn.close();
                    } catch (SQLException e) {
                    }
                }
            }
        } else if (dbType.equals("kylin")) {
            String user = dbConnInfo.getUserName();
            String password = dbConnInfo.getPassword();
            int projectLocation = url.lastIndexOf("/");
            String projectName = url.substring(projectLocation + 1);
            String ipAddress = url.substring(url.indexOf(":", 5), projectLocation);
            url = "http" + ipAddress + "/kylin/api/tables?ext=true&project=" + projectName;
            System.out.println("kylin访问地址:" + url);
            HttpHost target = new HttpHost(ipAddress.substring(3, ipAddress.lastIndexOf(":")),
                    Integer.parseInt(ipAddress.substring(ipAddress.lastIndexOf(":") + 1)), "http");
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(new AuthScope(target.getHostName(), target.getPort()),
                    new UsernamePasswordCredentials(user, password));
            CloseableHttpClient httpclient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
            try {
                AuthCache authCache = new BasicAuthCache();
                BasicScheme basicAuth = new BasicScheme();
                authCache.put(target, basicAuth);
                HttpClientContext localContext = HttpClientContext.create();
                localContext.setAuthCache(authCache);
                HttpGet httpget = new HttpGet(url);
                CloseableHttpResponse response = null;
                try {
                    response = httpclient.execute(target, httpget, localContext);
                    // 失败返回
                    if (response.getStatusLine().getStatusCode() != 200) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("kylin访问失败");
                        return commonResponse;
                    }
                    List<JSONObject> result = JSONObject.parseArray(EntityUtils.toString(response.getEntity()),
                            JSONObject.class);
                    for (JSONObject jsonObject : result) {
                        if (jsonObject.getString("database").equals(dbConnInfo.getDatabase())
                                && jsonObject.getString("name").equals(dbConnInfo.getTable_name())) {
                            List<JSONObject> columns = JSONObject.parseArray(jsonObject.getString("columns"),
                                    JSONObject.class);
                            for (JSONObject column : columns) {
                                ColumnAlias columnAlias = new ColumnAlias();
                                columnAlias.setColumn_name(column.getString("name"));
                                columnAlias.setColumn_alias(column.getString("name"));
                                columnAlias.setData_type(column.getString("datatype"));
                                columnAlias.setDes(column.getString("comment"));
                                columnAlias.setTable_id(tableId);
                                if (columnAliasMapper.insertColumnAlias(columnAlias) != 1) {
                                    commonResponse.setMessage("插入列" + columnAlias.getColumn_name() + "失败,请稍后再试！");
                                    commonResponse.setSuccess(false);
                                    return commonResponse;
                                }
                            }
                            return commonResponse;
                        }
                    }
                    System.out.println("kylin查询表信息的接口返回数据有问题");
                    commonResponse.setMessage("kylin查询表信息的接口返回数据有问题");
                    commonResponse.setSuccess(false);
                    return commonResponse;
                } catch (IOException e) {
                    System.out.println(e);
                } finally {
                    response.close();
                }
            } catch (Exception e) {
                System.out.println(e);
            } finally {
                try {
                    httpclient.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return commonResponse;
                }
            }

        } else if (dbType.equals("mysql")) {
            Connection conn = null;
            // 与数据库的连接
            PreparedStatement pStemt = null;
            try {
                conn = DriverManager.getConnection(url, dbConnInfo.getUserName(), dbConnInfo.getPassword());
                pStemt = conn.prepareStatement("SELECT column_name as name,data_type as type,column_comment as comment FROM information_schema.columns where TABLE_SCHEMA='"
                        + dbConnInfo.getDatabase() + "' and TABLE_NAME='" + dbConnInfo.getTable_name() + "'");
                ResultSet set = pStemt.executeQuery();
                while (set.next()) {
                    ColumnAlias columnAlias = new ColumnAlias();
                    columnAlias.setColumn_name(set.getString("name"));
                    columnAlias.setColumn_alias(set.getString("name"));
                    columnAlias.setData_type(set.getString("type"));
                    columnAlias.setDes(set.getString("comment"));
                    columnAlias.setTable_id(tableId);
                    if (columnAliasMapper.insertColumnAlias(columnAlias) != 1) {
                        commonResponse.setMessage("插入列" + columnAlias.getColumn_name() + "失败,请稍后再试！");
                        commonResponse.setSuccess(false);
                        return commonResponse;
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
                commonResponse.setMessage(e.getMessage());
                commonResponse.setSuccess(false);
                return commonResponse;
            } finally {
                if (pStemt != null) {
                    try {
                        pStemt.close();
                        conn.close();
                    } catch (SQLException e) {
                    }
                }
            }
        } else if (dbType.equals("postgresql")) {
            Connection conn = null;
            // 与数据库的连接
            PreparedStatement pStemt = null;
            try {
                String table_name = dbConnInfo.getTable_name();
                String db_name = "public";
                if (dbConnInfo.getTable_name().contains(".")) {
                    table_name = dbConnInfo.getTable_name().substring(dbConnInfo.getTable_name().indexOf(".") + 1);
                    db_name = dbConnInfo.getTable_name().substring(0, dbConnInfo.getTable_name().indexOf("."));
                }
                conn = DriverManager.getConnection(url + "/" + dbConnInfo.getDatabase(), dbConnInfo.getUserName(), dbConnInfo.getPassword());
                pStemt = conn.prepareStatement("select tmp.*,columns.udt_name as type from (SELECT a.attname AS name,d.description AS comment FROM pg_attribute a LEFT JOIN pg_description d ON d.objoid  = a.attrelid AND d.objsubid = a.attnum WHERE  a.attnum > 0 AND    NOT a.attisdropped AND a.attrelid = '" + dbConnInfo.getTable_name() + "'::regclass) tmp left join (SELECT column_name, udt_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_name='" + table_name + "' and table_schema='" + db_name + "') columns on tmp.name=columns.column_name");
                ResultSet set = pStemt.executeQuery();
                while (set.next()) {
                    ColumnAlias columnAlias = new ColumnAlias();
                    columnAlias.setColumn_name(set.getString("name"));
                    columnAlias.setColumn_alias(set.getString("name"));
                    columnAlias.setData_type(set.getString("type"));
                    columnAlias.setDes(set.getString("comment"));
                    columnAlias.setTable_id(tableId);
                    if (columnAliasMapper.insertColumnAlias(columnAlias) != 1) {
                        commonResponse.setMessage("插入列" + columnAlias.getColumn_name() + "失败,请稍后再试！");
                        commonResponse.setSuccess(false);
                        return commonResponse;
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
                commonResponse.setMessage(e.getMessage());
                commonResponse.setSuccess(false);
                return commonResponse;
            } finally {
                if (pStemt != null) {
                    try {
                        pStemt.close();
                        conn.close();
                    } catch (SQLException e) {
                    }
                }
            }
        } else if (dbType.equals("sqlserver")) {
            Connection conn = null;
            // 与数据库的连接
            PreparedStatement pStemt = null;
            try {
                String table_name = dbConnInfo.getTable_name();
                String db_name = "public";
                if (dbConnInfo.getTable_name().contains(".")) {
                    table_name = dbConnInfo.getTable_name().substring(dbConnInfo.getTable_name().indexOf(".") + 1);
                    db_name = dbConnInfo.getTable_name().substring(0, dbConnInfo.getTable_name().indexOf("."));
                }
                conn = DriverManager.getConnection(url, dbConnInfo.getUserName(), dbConnInfo.getPassword());
                pStemt = conn.prepareStatement("select tmp.*,cc.data_type from (SELECT\n" +
                        "B.name AS column_name,\n" +
                        "C.value AS comment\n" +
                        "FROM " + dbConnInfo.getDatabase() + ".sys.tables A\n" +
                        "INNER JOIN " + dbConnInfo.getDatabase() + ".sys.columns B ON B.object_id = A.object_id\n" +
                        "LEFT JOIN " + dbConnInfo.getDatabase() + ".sys.extended_properties C ON C.major_id = B.object_id AND C.minor_id = B.column_id\n" +
                        "WHERE a.object_id=Object_Id('" + dbConnInfo.getDatabase() + "." + dbConnInfo.getTable_name() + "')) tmp left join (SELECT column_name,data_type FROM " + dbConnInfo.getDatabase() + ".information_schema.columns WHERE table_name='" + table_name + "' and table_schema='" + db_name + "' and table_catalog='" + dbConnInfo.getDatabase() + "') cc on tmp.column_name=cc.column_name");
                ResultSet set = pStemt.executeQuery();
                while (set.next()) {
                    ColumnAlias columnAlias = new ColumnAlias();
                    columnAlias.setColumn_name(set.getString("column_name"));
                    columnAlias.setColumn_alias(set.getString("column_name"));
                    columnAlias.setData_type(set.getString("data_type"));
                    columnAlias.setDes(set.getString("comment"));
                    columnAlias.setTable_id(tableId);
                    if (columnAliasMapper.insertColumnAlias(columnAlias) != 1) {
                        commonResponse.setMessage("插入列" + columnAlias.getColumn_name() + "失败,请稍后再试！");
                        commonResponse.setSuccess(false);
                        return commonResponse;
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
                commonResponse.setMessage(e.getMessage());
                commonResponse.setSuccess(false);
                return commonResponse;
            } finally {
                if (pStemt != null) {
                    try {
                        pStemt.close();
                        conn.close();
                    } catch (SQLException e) {
                    }
                }
            }
        } else if (dbType.equals("oracle")) {
            Connection conn = null;
            // 与数据库的连接
            PreparedStatement pStemt = null;
            try {
                conn = DriverManager.getConnection(url, dbConnInfo.getUserName(), dbConnInfo.getPassword());
                pStemt = conn.prepareStatement("select a.*,b.comments from (select column_name,data_type from all_tab_columns where table_name=upper('" + dbConnInfo.getTable_name() + "') and owner=upper('" + dbConnInfo.getDatabase() + "')) a left join (select column_name,comments from all_col_comments where table_name=upper('" + dbConnInfo.getTable_name() + "') and owner=upper('" + dbConnInfo.getDatabase() + "')) b on a.column_name=b.column_name");
                ResultSet set = pStemt.executeQuery();
                while (set.next()) {
                    ColumnAlias columnAlias = new ColumnAlias();
                    columnAlias.setColumn_name(set.getString("column_name"));
                    columnAlias.setColumn_alias(set.getString("column_name"));
                    columnAlias.setData_type(set.getString("data_type"));
                    columnAlias.setDes(set.getString("comments"));
                    columnAlias.setTable_id(tableId);
                    if (columnAliasMapper.insertColumnAlias(columnAlias) != 1) {
                        commonResponse.setMessage("插入列" + columnAlias.getColumn_name() + "失败,请稍后再试！");
                        commonResponse.setSuccess(false);
                        return commonResponse;
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
                commonResponse.setMessage(e.getMessage());
                commonResponse.setSuccess(false);
                return commonResponse;
            } finally {
                if (pStemt != null) {
                    try {
                        pStemt.close();
                        conn.close();
                    } catch (SQLException e) {
                    }
                }
            }
        } else {
            commonResponse.setMessage("不支持自动拉列的数据库类型：" + dbType + "，请手动添加列！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        return commonResponse;
    }

    public synchronized CommonResponse insertUserToken(UserToken userToken) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setSuccess(false);
        //校验参数
        if (userToken.getUser_name() == null || userToken.getUser_name().equals("")) {
            commonResponse.setMessage("用户名不能为空！");
            return commonResponse;
        }
        //校验用户是否存在
        if (userTokenMapper.getUserTokenByUserName(userToken.getUser_name()) != null) {
            commonResponse.setMessage("用户已经存在,请不要重复添加！");
            return commonResponse;
        }
        //生成唯一token
        Random random = new Random();
        char[] numbersAndLetters = ("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ").toCharArray();
        char[] randBuffer = new char[20];
        for (int i = 0; i < randBuffer.length; i++) {
            randBuffer[i] = numbersAndLetters[random.nextInt(36)];
        }
        userToken.setToken(new String(randBuffer));
        //入库
        if (userTokenMapper.insertUserToken(userToken) != 1) {
            commonResponse.setMessage("入库失败,请稍后再试！");
            return commonResponse;
        }
        commonResponse.setSuccess(true);
        commonResponse.setMessage("新增用户成功");
        refreshUserToken();
        return commonResponse;
    }

    public CommonResponse updateUserToken(UserToken userToken) {
        CommonResponse commonResponse = new CommonResponse();
        //校验用户是否存在
        UserToken oldUserToken = userTokenMapper.getUserTokenById(userToken.getId());
        if (oldUserToken == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("用户不存在");
            return commonResponse;
        }
        oldUserToken.setDes(userToken.getDes());
        oldUserToken.setTables(userToken.getTables());
        if (userTokenMapper.updateUserToken(oldUserToken) != 1) {
            commonResponse.setMessage("数据库更新失败,请稍后再试！");
            return commonResponse;
        }
        commonResponse.setMessage("更新成功！");
        refreshUserToken();
        return commonResponse;
    }

    public CommonResponse updateUserTokenStatus(int id, int status) {
        CommonResponse commonResponse = new CommonResponse();
        //校验用户是否存在
        UserToken oldUserToken = userTokenMapper.getUserTokenById(id);
        if (oldUserToken == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("用户不存在");
            return commonResponse;
        }
        oldUserToken.setState(status);
        if (userTokenMapper.updateUserToken(oldUserToken) != 1) {
            commonResponse.setMessage("操作失败,请稍后再试！");
            return commonResponse;
        }
        commonResponse.setMessage("操作成功！");
        refreshUserToken();
        return commonResponse;
    }

    public CommonResponse deleteUserToken(int id) {
        CommonResponse commonResponse = new CommonResponse();
        //校验用户是否存在
        UserToken oldUserToken = userTokenMapper.getUserTokenById(id);
        if (oldUserToken == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("用户不存在");
            return commonResponse;
        }
        oldUserToken.setIs_delete(1);
        if (userTokenMapper.updateUserToken(oldUserToken) != 1) {
            commonResponse.setMessage("删除失败,请稍后再试！");
            return commonResponse;
        }
        commonResponse.setMessage("删除成功！");
        refreshUserToken();
        return commonResponse;
    }

    public CommonResponse getUserToken() {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(userTokenMapper.getUserToken());
        return commonResponse;
    }

    public CommonResponse refreshUserToken() {
        CommonResponse commonResponse = new CommonResponse();
        List<UserToken> superUserToken = userTokenMapper.findSuperUserToken();
        //roc服务加载用户权限
        List<UserToken> userTokenCk = userTokenMapper.findTokenTables("Clickhouse");
        Map<String, String> userTokenCkMap = new HashMap<>();
        for (UserToken userToken : superUserToken) {
            userTokenCkMap.put(userToken.getToken(), "ALL");
        }
        if (userTokenCk != null && userTokenCk.size() > 0) {
            for (UserToken userToken : userTokenCk) {
                userTokenCkMap.put(userToken.getToken(), userToken.getDes());
            }
        }
        redisUtil.set("roc_token", userTokenCkMap);
        //chatbot服务加载用户权限
        List<UserToken> userTokenKylin = userTokenMapper.findTokenTables("Mysql");
        Map<String, String> userTokenMapKylin = new HashMap<>();
        for (UserToken userToken : superUserToken) {
            userTokenMapKylin.put(userToken.getToken(), "ALL");
        }
        if (userTokenKylin != null && userTokenKylin.size() > 0) {
            for (UserToken userToken : userTokenKylin) {
                userTokenMapKylin.put(userToken.getToken(), userToken.getDes());
            }
        }
        redisUtil.set("chatbot_token", userTokenMapKylin);

        commonResponse.setMessage("刷新成功！");
        return commonResponse;
    }

    public CommonResponse getTableAccessInfo(String token) {
        CommonResponse commonResponse = new CommonResponse();
        if (token == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("token值不能为空");
            return commonResponse;
        }
        UserToken userToken = userTokenMapper.getUserTokenByToken(token);
        if (userToken == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("此token不存在,请联系数据服务管理员获取token");
            return commonResponse;
        }
        if (userToken.getTables() != null && userToken.getTables().length == 1 && userToken.getTables()[0] == 0) {
            commonResponse.setData(userTokenMapper.findAllTableAccessInfo());
            return commonResponse;
        }
        commonResponse.setData(userTokenMapper.findTableAccessInfo(token));
        return commonResponse;
    }

    public CommonResponse getSourceInfo() {
        CommonResponse commonResponse = new CommonResponse();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = java.util.Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, -24);
        String yesterDayDate = dateFormat.format(calendar.getTime());
        calendar.set(Calendar.HOUR_OF_DAY, -24);
        String beforeYesterday = dateFormat.format(calendar.getTime());
        List<SourceInfo> sourceInfos = new ArrayList<>();
        Connection conn = null;
        // 与数据库的连接
        PreparedStatement pStemt = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://172.16.31.1:32086/cdmdq", "postgres", "cloud1688");
            pStemt = conn.prepareStatement("select table_type_name,sum(db_count) as db_count,sum(tb_count) as tb_count,sum(total_size) as total_size from ads.warehouse_stat where dt='" + yesterDayDate + "' group by table_type_name");
            ResultSet set = pStemt.executeQuery();
            while (set.next()) {
                SourceInfo sourceInfo = new SourceInfo();
                sourceInfo.setSourceName(set.getString("table_type_name") + "_db");
                sourceInfo.setDb(set.getLong("db_count"));
                sourceInfo.setTable(set.getLong("tb_count"));
                sourceInfo.setTotalFileSize(set.getLong("total_size"));
                sourceInfos.add(sourceInfo);
            }
            if (sourceInfos.isEmpty()) {
                pStemt = conn.prepareStatement("select table_type_name,sum(db_count) as db_count,sum(tb_count) as tb_count,sum(total_size) as total_size from ads.warehouse_stat where dt='" + beforeYesterday + "' group by table_type_name");
                set = pStemt.executeQuery();
                while (set.next()) {
                    SourceInfo sourceInfo = new SourceInfo();
                    sourceInfo.setSourceName(set.getString("table_type_name") + "_db");
                    sourceInfo.setDb(set.getLong("db_count"));
                    sourceInfo.setTable(set.getLong("tb_count"));
                    sourceInfo.setTotalFileSize(set.getLong("total_size"));
                    sourceInfos.add(sourceInfo);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
        commonResponse.setData(sourceInfos);
        return commonResponse;
    }

    public CommonResponse getDepartmentSize() {
        CommonResponse commonResponse = new CommonResponse();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = java.util.Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, -24);
        String yesterDayDate = dateFormat.format(calendar.getTime());
        calendar.set(Calendar.HOUR_OF_DAY, -24);
        String beforeYesterday = dateFormat.format(calendar.getTime());
        Map<String, Long> departmentSize = new HashMap<>();
        Connection conn = null;
        // 与数据库的连接
        PreparedStatement pStemt = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://172.16.31.1:32086/cdmdq", "postgres", "cloud1688");
            pStemt = conn.prepareStatement("select analyse_value,sum(total_size) as total_size from (select analyse_value,sum(file_size) as total_size from ads.hdfs_stat where dt='" + yesterDayDate + "' and analyse_type_name='部门' GROUP BY analyse_value union all select 'bigdata' as analyse_value,sum(file_size) as total_size from ads.hdfs_stat where dt='2022-06-27' and analyse_type_name='主题') t group by analyse_value");
            ResultSet set = pStemt.executeQuery();
            while (set.next()) {
                departmentSize.put(set.getString("analyse_value"), set.getLong("total_size"));
            }
            if (departmentSize.isEmpty()) {
                pStemt = conn.prepareStatement("select analyse_value,sum(total_size) as total_size from (select analyse_value,sum(file_size) as total_size from ads.hdfs_stat where dt='" + beforeYesterday + "' and analyse_type_name='部门' GROUP BY analyse_value union all select 'bigdata' as analyse_value,sum(file_size) as total_size from ads.hdfs_stat where dt='2022-06-27' and analyse_type_name='主题') t group by analyse_value");
                set = pStemt.executeQuery();
                while (set.next()) {
                    departmentSize.put(set.getString("analyse_value"), set.getLong("total_size"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
        commonResponse.setData(departmentSize);
        return commonResponse;
    }

    public CommonResponse insertDbInfo(DbInfo dbInfo) {
        CommonResponse commonResponse = new CommonResponse();
        if (dbInfo.getDb_name().isEmpty() || dbInfo.getUserName().isEmpty() || dbInfo.getPassword().isEmpty() || dbInfo.getService_name().isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("服务地址,服务类型,用户名,密码都不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(dbInfo.getDb_url()) && dbInfo.getExtend() == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("服务连接参数不能为空");
            return commonResponse;
        }
        if (StringUtils.isEmpty(dbInfo.getDb_url())) {
            String dbType = dbInfo.getDb_name().toLowerCase();
            if (dbType.equals("mysql") || dbType.equals("kylin") || dbType.equals("clickhouse") || dbType.equals("postgresql") || dbType.equals("sqlserver") || dbType.equals("oracle")) {
                String dbUrl = "jdbc:" + dbType;
                if (dbType.equals("oracle")) {
                    dbUrl = dbUrl + ":thin:@";
                    if (StringUtils.isEmpty(dbInfo.getExtend().getService_name())) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("oracle服务名不能为空");
                        return commonResponse;
                    }
                } else {
                    dbUrl = dbUrl + "://";
                }
                dbUrl = dbUrl + dbInfo.getExtend().getHost() + ":" + dbInfo.getExtend().getPort();

                if (dbType.equals("oracle")) {
                    dbUrl = dbUrl + ":" + dbInfo.getExtend().getService_name();
                }
                if (!StringUtils.isEmpty(dbInfo.getExtend().getConnect_param())) {
                    if (dbInfo.getExtend().getConnect_param().startsWith("?")) {
                        dbUrl = dbUrl + dbInfo.getExtend().getConnect_param();
                    } else {
                        dbUrl = dbUrl + "?" + dbInfo.getExtend().getConnect_param();
                    }
                }
                dbInfo.setDb_url(dbUrl);
            } else {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("不支持得dbType");
                return commonResponse;
            }
        }
        //判断service_name是否重复
        if (databaseInfoMapper.getDbInfoByServiceName(dbInfo.getService_name()) != null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("服务名称已存在");
            return commonResponse;
        }

        if (dbInfo.getService_name().equals("DATASERVICE")) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("DATASERVICE为系统配置服务的名称,请更换其他名称");
            return commonResponse;
        }

        if (dbInfo.getCommon_service() == 1) {
            dbInfo.setService_path(commonDataserviceUrl + dbInfo.getService_name() + "/");
        }
        DbInfo dbInfoOld = databaseInfoMapper.getDbInfoByDbUrl(dbInfo);
        if (dbInfoOld != null) {
            if (dbInfoOld.getIs_delete() == 0) {
                commonResponse.setMessage("数据已存在,请不要重复新增！");
                commonResponse.setSuccess(false);
            } else {
                if (databaseInfoMapper.updateDbInfoDelete(dbInfoOld.getId(), 0) != 1) {
                    commonResponse.setMessage("新增数据失败,请稍后再试！");
                    commonResponse.setSuccess(false);
                }
                dbInfo.setId(dbInfoOld.getId());
                databaseInfoMapper.updateDbInfo(dbInfo);
            }
        } else {
            if (databaseInfoMapper.insertDnInfo(dbInfo) != 1) {
                commonResponse.setMessage("新增数据失败,请稍后再试！");
                commonResponse.setSuccess(false);
            } else {
                if (dbInfo.getCommon_service() == 1) {
                    //设置redis订阅通道去更新通用数据服务
                    redisUtil.convertAndSend(commonDataserviceName, "reflushDbInfo");
                }
            }
        }
        return commonResponse;
    }

    public CommonResponse updateDbInfo(DbInfo dbInfo) {
        CommonResponse commonResponse = new CommonResponse();
        if (dbInfo.getDb_name().isEmpty() || dbInfo.getUserName().isEmpty() || dbInfo.getPassword().isEmpty() || dbInfo.getService_name().isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("服务地址,服务类型,用户名,密码都不能为空");
            return commonResponse;
        }
        if (dbInfo.getCommon_service() == 1) {
            String dbType = dbInfo.getDb_name().toLowerCase();
            if (dbType.equals("mysql") || dbType.equals("kylin") || dbType.equals("clickhouse") || dbType.equals("postgresql") || dbType.equals("sqlserver") || dbType.equals("oracle")) {
                String dbUrl = "jdbc:" + dbType;
                if (dbType.equals("oracle")) {
                    dbUrl = dbUrl + ":thin:@";
                    if (StringUtils.isEmpty(dbInfo.getExtend().getService_name())) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("oracle服务名不能为空");
                        return commonResponse;
                    }
                } else {
                    dbUrl = dbUrl + "://";
                }
                dbUrl = dbUrl + dbInfo.getExtend().getHost() + ":" + dbInfo.getExtend().getPort();

                if (dbType.equals("oracle")) {
                    dbUrl = dbUrl + ":" + dbInfo.getExtend().getService_name();
                }
                if (!StringUtils.isEmpty(dbInfo.getExtend().getConnect_param())) {
                    if (dbInfo.getExtend().getConnect_param().startsWith("?")) {
                        dbUrl = dbUrl + dbInfo.getExtend().getConnect_param();
                    } else {
                        dbUrl = dbUrl + "?" + dbInfo.getExtend().getConnect_param();
                    }
                }
                dbInfo.setDb_url(dbUrl);
            }
        }

        DbInfo dbInfoOld = databaseInfoMapper.getDbInfoById(dbInfo.getId());
        if (dbInfoOld.getCommon_service() == 1) {
            dbInfo.setService_path(dbInfoOld.getService_path());
        }
        if (dbInfoOld == null) {
            commonResponse.setMessage("原始数据不存在,请刷新后再操作");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        //更改服务名称后需要判断是否重复
        if (!dbInfoOld.getService_name().equals(dbInfo.getService_name())) {
            if (dbInfo.getService_name().equals("DATASERVICE")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("DATASERVICE为系统配置服务的名称,请更换其他名称");
                return commonResponse;
            }
            if (databaseInfoMapper.getDbInfoByServiceName(dbInfo.getService_name()) != null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("服务名称已存在");
                return commonResponse;
            }
            if (dbInfoOld.getCommon_service() == 1) {
                dbInfo.setService_path(commonDataserviceUrl + dbInfo.getService_name() + "/");
            }
        }

        if (databaseInfoMapper.updateDbInfo(dbInfo) < 1) {
            commonResponse.setMessage("更新失败,请稍后再试");
            commonResponse.setSuccess(false);
            return commonResponse;
        } else {
            if (dbInfo.getCommon_service() != dbInfoOld.getCommon_service() ||
                    (dbInfo.getCommon_service() == 1 && ((!dbInfoOld.getService_name().equals(dbInfo.getService_name()) || (!dbInfoOld.getDb_url().equals(dbInfo.getDb_url())) ||
                            (!dbInfoOld.getUserName().equals(dbInfo.getUserName())) || (!dbInfoOld.getPassword().equals(dbInfo.getPassword())))))) {
                //设置redis订阅通道去更新通用数据服务
                redisUtil.convertAndSend(commonDataserviceName, "reflushDbInfo");
            }
        }
        commonResponse.setMessage("更新成功");
        return commonResponse;
    }

    public CommonResponse deleteDbInfo(DbInfo dbInfo) {
        CommonResponse commonResponse = new CommonResponse();
        DbInfo dbInfoOld = databaseInfoMapper.getDbInfoById(dbInfo.getId());
        if (dbInfoOld == null) {
            commonResponse.setMessage("原始数据不存在,请刷新后再操作");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        //判断下面是不是有库
        List<DatabaseInfo> databaseInfos = databaseInfoMapper.getDataBaseByDbid(dbInfo.getId());
        if (databaseInfos != null && databaseInfos.size() > 0) {
            commonResponse.setMessage("该服务下面有库,请先删除库");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (databaseInfoMapper.updateDbInfoDelete(dbInfo.getId(), 1) < 1) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
            return commonResponse;
        } else {
            if (dbInfoOld.getCommon_service() == 1) {
                //设置redis订阅通道去更新通用数据服务
                redisUtil.convertAndSend(commonDataserviceName, "reflushDbInfo");
            }
        }
        commonResponse.setMessage("删除成功");
        return commonResponse;
    }

    public CommonResponse getDbInfoById(int id) {
        CommonResponse commonResponse = new CommonResponse();
        DbInfo dbInfo = databaseInfoMapper.getDbInfoById(id);
        if (dbInfo == null) {
            commonResponse.setMessage("数据不存在!");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        commonResponse.setData(dbInfo);
        return commonResponse;
    }

    public CommonResponse getAllTableInfo() {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tableInfoMapper.getAllTableInfo());
        return commonResponse;
    }

    public CommonResponse getUserTokenByUserName(String userName) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(userTokenMapper.getUserTokenByUserName(userName));
        return commonResponse;
    }

    public CommonResponse getTableInfoByThemeId(int themeId) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tableInfoMapper.getTableInfoByThemeId(themeId));
        return commonResponse;
    }

    public CommonResponse getTableInfoByDepartment(String department) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tableInfoMapper.getTableInfoByDepartment(department));
        return commonResponse;
    }

    public CommonResponse getTableNum() {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tableInfoMapper.getTableNum());
        return commonResponse;
    }

    public CommonResponse getApiAccessTotalGroupByDay(String startDate, String endDate) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tableInfoMapper.getApiAccessTotalGroupByDay(startDate, endDate));
        return commonResponse;
    }

    public CommonResponse getApiAccessTop(String startDate, String endDate, int top) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tableInfoMapper.getApiAccessTop(startDate, endDate, top));
        return commonResponse;
    }

    public void refreshDataService(int tableId) {
        //查询数据服务刷新地址
        String servicePath = tableInfoMapper.getDataServicePathByTableId(tableId);
        if (org.apache.commons.lang.StringUtils.isEmpty(servicePath)) {
            return;
        }
        if (servicePath.startsWith(commonDataserviceUrl)) {
            //设置redis订阅通道去更新通用数据服务的配置信息
            redisUtil.convertAndSend(commonDataserviceName, "reflushConfig");
            return;
        }
        Thread t = new Thread() {
            @Override
            public void run() {
                String url = dataServiceUrl + servicePath + "refreshConfig";
                //请求刷新地址
                ResponseEntity<String> responseEntity = restTemplate.getForEntity(url, String.class);
                if (!responseEntity.getStatusCode().is2xxSuccessful()) {
                    System.out.println(url + "服务不可用");
                    return;
                }
            }
        };
        t.start();
    }

    public boolean isContainChinese(String str) {
        Pattern p = Pattern.compile("[\u4E00-\u9FA5|\\！|\\，|\\。|\\（|\\）|\\《|\\》|\\“|\\”|\\？|\\：|\\；|\\【|\\】]");
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        }
        return false;
    }

    public CommonResponse markMetricColumn(ColumnAlias columnAlias) {
        CommonResponse commonResponse = new CommonResponse();
        if (columnAlias == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("入参不能为空");
            return commonResponse;
        }
        ColumnAlias oldColumnAlias = columnAliasMapper.getColumnAliasById(columnAlias.getId());
        if (columnAlias.isMetric() == oldColumnAlias.isMetric()) {
            return commonResponse;
        }
        if (oldColumnAlias.isMetric()) {
            //判断是否有原址指标在使用
            String name = quotoInfoMapper.getQuotoByMetric(oldColumnAlias.getColumn_alias(), oldColumnAlias.getTable_id());
            if (!StringUtils.isEmpty(name)) {
                commonResponse.setMessage("有激活的指标(" + name + ")运行,不能取消！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (columnAliasMapper.updateColumnMetric(columnAlias.getId(), columnAlias.isMetric()) < 1) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("标记为度量失败,请联系管理员");
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse uploadFile(MultipartFile file, String project) {
        CommonResponse commonResponse = new CommonResponse();
        if (file == null || file.isEmpty() || StringUtils.isEmpty(project)) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("file和project不能为空");
            return commonResponse;
        }
        File tmpFile = toFile(file);
        long nowTime = System.currentTimeMillis();
        String fileKey = project + "/" + nowTime + "_" + file.getOriginalFilename();
        PutObjectResult result = OssUtils.uploadFile(bucketName, fileKey, tmpFile);
        tmpFile.delete();
        commonResponse.setData(endpoint + "/bigdata/" + fileKey);
        return commonResponse;
    }

    private File toFile(MultipartFile file) {
        File toFile = null;
        if ("".equals(file) || file.getSize() <= 0) {
            return null;
        } else {
            InputStream ins = null;
            try {
                ins = file.getInputStream();
                toFile = new File(file.getOriginalFilename());
                inputStreamToFile(ins, toFile);
                ins.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return toFile;
    }

    private void inputStreamToFile(InputStream ins, File file) {
        try {
            OutputStream os = new FileOutputStream(file);
            int bytesRead = 0;
            byte[] buffer = new byte[8192];
            while ((bytesRead = ins.read(buffer, 0, 8192)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            os.close();
            ins.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CommonResponse deleteFile(String url) {
        CommonResponse commonResponse = new CommonResponse();
        if (StringUtils.isEmpty(url)) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("url和project不能为空");
            return commonResponse;
        }
        if (!url.contains(endpoint + "/" + bucketName + "/")) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("url不是通过上传接口生成的,不支持删除");
            return commonResponse;
        }
        String fileName = url.replace(endpoint + "/" + bucketName + "/", "");
        OssUtils.deleteFile(bucketName, fileName);
        return commonResponse;
    }

    public CommonResponse getApiActiveUserTop(String startDate, String endDate, int top) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(tableInfoMapper.getApiActiveUserTop(startDate, endDate, top));
        return commonResponse;
    }
}
