package dataservice.controller;

import apijson.JSON;
import apijson.entity.DbInfo;
import apijson.framework.APIJSONApplication;
import apijson.framework.APIJSONController;
import apijson.framework.APIJSONCreator;
import apijson.framework.APIJSONParser;
import apijson.orm.Parser;
import apijson.orm.SQLConfig;
import com.alibaba.fastjson.JSONObject;
import dataservice.config.DBSQLConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import dataservice.redis.RedisUtil;
import dataservice.service.ExtendFunctionService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/common/dataservice")
public class DataServiceControl extends APIJSONController {
    @Autowired
    private RedisUtil redisUtil;
    public static String serviceName = "common";
    @Value("${configDbUrl}")
    private  String configDbUrl;
    @Value("${configDbAccount}")
    private  String configDbAccount;
    @Value("${configDbPassword}")
    private  String configDbPassword;
    @Autowired
    private ExtendFunctionService extendFunctionService;

    @Override
    public Parser<Long> newParser(HttpSession session, apijson.RequestMethod method) {
        return super.newParser(session, method).setNeedVerify(false); // TODO 这里关闭校验，方便新手快速测试，实际线上项目建议开启
    }

    @PostMapping(value = "data/{dataservice}/{database}")
    public String getCephMetaData(@PathVariable("dataservice") String dataservice, @PathVariable("database") String database, @RequestBody String request, HttpServletRequest session) {
        request = "{'@database':'" + dataservice + "','@schema':'" + database + "'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session);
    }

    public String getData(String request, HttpServletRequest httpServletRequest) {
        JSONObject response = new JSONObject();
        response.put("ok", false);
        response.put("code", 401);
        String result = get(request, httpServletRequest.getSession());
        JSONObject jsonResult = JSON.parseObject(result);
        jsonResult.remove("execute_sql");
        return jsonResult.toString();
    }

    public void refreshConfig() {
        APIJSONParser abstractParser = new APIJSONParser();
        abstractParser.loadAliasConfig();
    }



    public void accessHistory(String token, String service_path, String table_alias, String response, HttpSession session) {
        extendFunctionService.saveAccessHistory(token, service_path, table_alias, response, session);
    }

    public void initAPIJSONApplication() {
        Map<String, DbInfo> dbInfoMap = new HashMap<>();
        DbInfo dbInfo = new DbInfo();
        dbInfo.setDb_name("DATASERVICE");
        dbInfo.setDb_url(configDbUrl);
        dbInfo.setUserName(configDbAccount);
        dbInfo.setPassword(configDbPassword);
        dbInfoMap.put("DATASERVICE", dbInfo);
        //获取db连接
        Connection conn = null;
        // 与数据库的连接
        PreparedStatement pStemt = null;
        try {
            conn = DriverManager.getConnection(configDbUrl, configDbAccount, configDbPassword);
            pStemt = conn.prepareStatement("SELECT db_name,db_url,userName,password,service_name FROM bigdata_dataservice.Db_info where is_delete=0 and state=1 and common_service=1");
            ResultSet set = pStemt.executeQuery();
            while (set.next()) {
                DbInfo dbInfo1 = new DbInfo();
                dbInfo1.setDb_name(set.getString("db_name"));
                dbInfo1.setDb_url(set.getString("db_url"));
                dbInfo1.setUserName(set.getString("userName"));
                dbInfo1.setPassword(set.getString("password"));
                dbInfoMap.put(set.getString("service_name"), dbInfo1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
        APIJSONApplication.DEFAULT_APIJSON_CREATOR = new APIJSONCreator() {
            @Override
            public SQLConfig createSQLConfig() {
                return new DBSQLConfig(dbInfoMap);
            }
        };
        try {
            APIJSONApplication.init(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
