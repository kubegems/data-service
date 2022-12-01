package com.cloudminds.bigdata.dataservice.quoto.config.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.utils.StringUtils;
import org.springframework.web.client.RestTemplate;

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
        if (columnAliasMapper.updateColumnAliasDelete(id, 1) != 1) {
            commonResponse.setMessage("删除失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        refreshDataService(columnAlias.getTable_id());
        return commonResponse;
    }

    public CommonResponse insertColumnAlias(ColumnAlias columnAlias) {
        CommonResponse commonResponse = new CommonResponse();
        ColumnAlias columnAliasOld = columnAliasMapper.getColumnAlias(columnAlias);
        if (columnAliasOld != null) {
            if (columnAliasOld.getIs_delete() == 0) {
                commonResponse.setMessage("数据已存在,请不要重复新增！");
                commonResponse.setSuccess(false);
            } else {
                columnAliasOld.setDes(columnAlias.getDes());
                columnAliasOld.setColumn_alias(columnAlias.getColumn_alias());
                if (columnAliasMapper.updateColumnAlias(columnAliasOld) != 1) {
                    commonResponse.setMessage("新增数据失败,请稍后再试！");
                    commonResponse.setSuccess(false);
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
            refreshDataService(columnAlias.getTable_id());
        }
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
        refreshDataService(oldColumnAlias.getTable_id());
        return commonResponse;
    }

    // datainfo
    public CommonResponse getdbInfo() {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(databaseInfoMapper.getdbInfo());
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
            String name = quotoInfoMapper.getQuotoByField(quotoInfo.getQuoto_name());
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
            String name = quotoInfoMapper.getQuotoByField(quotoInfo.getQuoto_name());
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
            if (quotoInfoMapper.getQuotoInfoByQuotoName(quotoInfo.getQuoto_name(),quotoInfo.getTable_id()) != null) {
                commonResponse.setMessage("指标名已存在！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (quotoInfoMapper.updateQuotoInfo(quotoInfo) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
            return commonResponse;
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
            insertColumnAlias(tableInfo.getId());
            QuotoInfo quotoInfo = new QuotoInfo();
            quotoInfo.setTable_id(tableInfo.getId());
            quotoInfo.setQuoto_sql("count(*)");
            quotoInfo.setQuoto_name("count");
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
        insertColumnAlias(tableInfo.getId());
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
            conn = DriverManager.getConnection(dbConnInfo.getDb_url(), dbConnInfo.getUserName(),
                    dbConnInfo.getPassword());
            pStemt = conn.prepareStatement("SELECT \"" + columnName + "\" FROM \"" + dbConnInfo.getDatabase() + "\".\""
                    + dbConnInfo.getTable_name() + "\" limit 1");
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

    public boolean insertColumnAlias(int tableId) {
        DbConnInfo dbConnInfo = databaseInfoMapper.getdbConnInfoByTableId(tableId);
        if (dbConnInfo == null) {
            System.out.println("tableId:" + tableId + " 不存在");
            return false;
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
                        return false;
                    }
                }

            } catch (

                    SQLException e) {
                e.printStackTrace();
                return false;
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
                        System.out.println("kylin访问失败");
                        return false;
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
                                    return false;
                                }
                            }
                            return true;
                        }
                    }
                    System.out.println("kylin查询表信息的接口返回数据有问题");
                    return false;
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
                    return true;
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
                        return false;
                    }
                }

            } catch (

                    SQLException e) {
                e.printStackTrace();
                return false;
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
            return false;
        }

        return true;
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
        if (dbInfo.getDb_url().isEmpty() || dbInfo.getDb_name().isEmpty() || dbInfo.getUserName().isEmpty() || dbInfo.getPassword().isEmpty() || dbInfo.getService_name().isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("服务地址,服务类型,用户名,密码都不能为空");
            return commonResponse;
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
            }
        }
        return commonResponse;
    }

    public CommonResponse updateDbInfo(DbInfo dbInfo) {
        CommonResponse commonResponse = new CommonResponse();
        if (dbInfo.getDb_url().isEmpty() || dbInfo.getDb_name().isEmpty() || dbInfo.getUserName().isEmpty() || dbInfo.getPassword().isEmpty() || dbInfo.getService_name().isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("服务地址,服务类型,用户名,密码都不能为空");
            return commonResponse;
        }
        DbInfo dbInfoOld = databaseInfoMapper.getDbInfoById(dbInfo.getId());
        if (dbInfoOld == null) {
            commonResponse.setMessage("原始数据不存在,请刷新后再操作");
            commonResponse.setSuccess(false);
            return commonResponse;
        }
        if (databaseInfoMapper.updateDbInfo(dbInfo) < 1) {
            commonResponse.setMessage("更新失败,请稍后再试");
            commonResponse.setSuccess(false);
            return commonResponse;
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
        if (databaseInfoMapper.updateDbInfoDelete(dbInfo.getId(), 1) < 1) {
            commonResponse.setMessage("删除失败,请稍后再试");
            commonResponse.setSuccess(false);
            return commonResponse;
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
}
