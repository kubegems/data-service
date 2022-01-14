package com.cloudminds.bigdata.dataservice.quoto.config.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

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
import org.springframework.stereotype.Service;

import com.cloudminds.bigdata.dataservice.quoto.config.mapper.ApiDocMapper;
import com.cloudminds.bigdata.dataservice.quoto.config.mapper.ColumnAliasMapper;
import com.cloudminds.bigdata.dataservice.quoto.config.mapper.DatabaseInfoMapper;
import com.cloudminds.bigdata.dataservice.quoto.config.mapper.QuotoInfoMapper;
import com.cloudminds.bigdata.dataservice.quoto.config.mapper.TableInfoMapper;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.utils.StringUtils;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.DatabaseInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.DbConnInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.QuotoInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableInfo;

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

    // columnAlias
    public CommonResponse getColumnAlias(int tableId) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(columnAliasMapper.getColumnAliasByTableId(tableId));
        return commonResponse;
    }

    public CommonResponse updateColumnAliasStatus(int id, int status) {
        CommonResponse commonResponse = new CommonResponse();
        if (columnAliasMapper.updateColumnAliasStatus(id, status) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
        }
        return commonResponse;
    }

    public CommonResponse deleteColumnAlias(int id) {
        CommonResponse commonResponse = new CommonResponse();
        if (columnAliasMapper.updateColumnAliasDelete(id, 1) != 1) {
            commonResponse.setMessage("删除失败,请稍后再试！");
            commonResponse.setSuccess(false);
        }
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
            }
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
        }
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
            } else {
                if (databaseInfoMapper.updateDatabaseInfoDelete(databaseInfoOld.getId(), 0) != 1) {
                    commonResponse.setMessage("新增数据失败,请稍后再试！");
                    commonResponse.setSuccess(false);
                }
            }
        } else {
            if (databaseInfoMapper.insertDatabaseInfo(databaseInfo) != 1) {
                commonResponse.setMessage("新增数据失败,请稍后再试！");
                commonResponse.setSuccess(false);
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
        }
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
        }
        return commonResponse;
    }

    @SuppressWarnings("deprecation")
    public CommonResponse insertQuotoInfo(QuotoInfo quotoInfo) {
        CommonResponse commonResponse = new CommonResponse();
        QuotoInfo quotoInfoOld = quotoInfoMapper.getQuotoInfo(quotoInfo);
        if (quotoInfoOld != null) {
            if (quotoInfoOld.getIs_delete() == 0) {
                commonResponse.setMessage("数据已存在,请不要重复新增！");
                commonResponse.setSuccess(false);
            } else {
                // 判断是否符合规则
                if (NumberUtils.isNumber(quotoInfo.getQuoto_name()) || quotoInfo.getQuoto_name().contains("(")
                        || quotoInfo.getQuoto_name().contains(")") || quotoInfo.getQuoto_name().contains("+")
                        || quotoInfo.getQuoto_name().contains("-") || quotoInfo.getQuoto_name().contains("*")
                        || quotoInfo.getQuoto_name().contains("/") || quotoInfo.getQuoto_name().contains("#")
                        || quotoInfo.getQuoto_name().contains("&")) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("指标名不能是数或者含有()+-*/&#特殊符号");
                    return commonResponse;
                }
                // 判断是否有同名的指标
                if (quotoInfoMapper.getQuotoInfoByQuotoName(quotoInfo.getQuoto_name()) != null) {
                    commonResponse.setMessage("指标名已存在！");
                    commonResponse.setSuccess(false);
                    return commonResponse;
                }
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
                    || quotoInfo.getQuoto_name().contains("&")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("指标名不能是数或者含有()+-*/&#特殊符号");
                return commonResponse;
            }
            // 判断是否有同名的指标
            if (quotoInfoMapper.getQuotoInfoByQuotoName(quotoInfo.getQuoto_name()) != null) {
                commonResponse.setMessage("指标名已存在！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
            if (quotoInfoMapper.insertQuotoInfo(quotoInfo) != 1) {
                commonResponse.setMessage("新增数据失败,请稍后再试！");
                commonResponse.setSuccess(false);
            }
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
                    || quotoInfo.getQuoto_name().contains("&")) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("指标名不能是数或者含有()+-*/&#特殊符号");
                return commonResponse;
            }
            if (quotoInfoMapper.getQuotoInfoByQuotoName(quotoInfo.getQuoto_name()) != null) {
                commonResponse.setMessage("指标名已存在！");
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        }
        if (quotoInfoMapper.updateQuotoInfo(quotoInfo) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
        }
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
        }
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
        }
        return commonResponse;
    }

    public CommonResponse insertTableInfo(TableInfo tableInfo) {
        CommonResponse commonResponse = new CommonResponse();
        TableInfo tableInfoOld = tableInfoMapper.getTableInfo(tableInfo);
        if (tableInfoOld != null) {
            if (tableInfoOld.getIs_delete() == 0) {
                commonResponse.setMessage("数据已存在,请不要重复新增！");
                commonResponse.setSuccess(false);
            } else {
                tableInfoOld.setTable_alias(tableInfo.getTable_alias());
                tableInfoOld.setDes(tableInfo.getDes());
                tableInfoOld.setBusiness_process_id(tableInfo.getBusiness_process_id());
                if (tableInfoMapper.updateTableInfo(tableInfoOld) != 1) {
                    commonResponse.setMessage("新增数据失败,请稍后再试！");
                    commonResponse.setSuccess(false);
                }
            }
        } else {
            if (tableInfoMapper.insertTableInfo(tableInfo) != 1) {
                commonResponse.setMessage("新增数据失败,请稍后再试！");
                commonResponse.setSuccess(false);
            }
        }
        if (commonResponse.isSuccess()) {
            insertColumnAlias(tableInfo.getId());
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
        return commonResponse;
    }

    public CommonResponse updateTableInfo(TableInfo tableInfo) {
        CommonResponse commonResponse = new CommonResponse();
        if (tableInfoMapper.updateTableInfo(tableInfo) != 1) {
            commonResponse.setMessage("更新失败,请稍后再试！");
            commonResponse.setSuccess(false);
        }
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

        } else {
            return false;
        }

        return true;
    }

}
