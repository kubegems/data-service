package com.cloudminds.bigdata.dataservice.quoto.roc.controller;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import apijson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.cloudminds.bigdata.dataservice.quoto.roc.service.SaveAccessHistory;
import com.mysql.cj.xdevapi.JsonArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.roc.redis.RedisUtil;

import apijson.entity.CommonResponse;
import apijson.entity.ConfigLoadResponse;
import apijson.framework.APIJSONController;
import apijson.framework.APIJSONParser;
import apijson.orm.AbstractSQLConfig;
import apijson.orm.Parser;

@RestController
@RequestMapping("/roc/quoto")
public class RobotQuotoControl extends APIJSONController {
    @Autowired
    private RedisUtil redisUtil;
    String serviceName = "roc";
    @Autowired
    private SaveAccessHistory saveAccessHistory;

    @Override
    public Parser<Long> newParser(HttpSession session, apijson.RequestMethod method) {
        return super.newParser(session, method).setNeedVerify(false); // TODO 这里关闭校验，方便新手快速测试，实际线上项目建议开启
    }

    @PostMapping(value = "get")
    public String getHarixData(@RequestBody String request, HttpServletRequest session) {
        return getData(request, session,"get");
    }

    @PostMapping(value = "cephMeta")
    public String getCephMetaData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'ceph_meta'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"cephMeta");
    }

    @PostMapping(value = "sv")
    public String getSvData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'sv'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"sv");
    }

    @PostMapping(value = "roc")
    public String getRocData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'roc'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"roc");
    }

    @PostMapping(value = "vbn")
    public String getVbnData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'vbn'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"vbn");
    }

    @PostMapping(value = "maQiaoDb")
    public String getMaqiaodbData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'maqiaodb'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"maQiaoDb");
    }

    @PostMapping(value = "menJing")
    public String getMenjingData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'menjing'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"menJing");
    }

    @PostMapping(value = "fangCangDb")
    public String getFangcangdbData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'fangcangdb'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"fangCangDb");
    }

    @PostMapping(value = "cross")
    public String getCrossData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'cross'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"cross");
    }

    @PostMapping(value = "cropsRedash")
    public String getcropsRedashData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'crops_redash'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"cropsRedash");
    }

    @PostMapping(value = "cms")
    public String getCmsData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'cms'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"cms");
    }

    @PostMapping(value = "boss")
    public String getBossData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'boss'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"boss");
    }

    @PostMapping(value = "cdmCo")
    public String getCdmCoData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'cdm_co'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"cdmCo");
    }

    @PostMapping(value = "cmd")
    public String getCmdData(@RequestBody String request, HttpServletRequest session) {
        request = "{'@schema':'cmd'," + request.substring(request.indexOf("{") + 1);
        return getData(request, session,"cmd");
    }

    @PostMapping(value = "tag")
    public String getTagData(@RequestBody String request, HttpServletRequest session) {
        JSONObject requestJson = JSON.parseObject(JSON.parseObject(request));
        String page = "0";
        String count = "10";
        String table="";
        boolean queryCount=false;
        String subSql="";
        //得到真实的表名和库名
        if(requestJson.containsKey("table_name")){
            String tableName = requestJson.get("table_name").toString();
            if(AbstractSQLConfig.TABLE_KEY_MAP.containsKey("tag."+tableName)){
                table=AbstractSQLConfig.TABLE_KEY_MAP.get("tag."+tableName);
                table="tag."+table;
            }else{
                JSONObject response=new JSONObject();
                response.put("ok",false);
                response.put("code",401);
                response.put("msg","表tag."+tableName+"没有配置,请联系管理员");
                return response.toString();
            }

        }else{
            JSONObject response=new JSONObject();
            response.put("ok",false);
            response.put("code",401);
            response.put("msg","table_name必须传入");
            return response.toString();
        }

        //是否请求count
        if(requestJson.containsKey("query")&&requestJson.getObject("query",Integer.class)==1){
            queryCount=true;
            subSql="select count(*) as total from "+table+" where oid in (select arrayJoin(";
        }else if(requestJson.containsKey("column")){
            subSql="select "+requestJson.getString("column")+" from "+table+" where oid in (select arrayJoin(";
        }else{
            subSql="select * from "+table+" where oid in (select arrayJoin(";
        }

        String sql = "WITH";
        //解析tag_str的sql
        boolean tag_str=false;
        String tag_str_sql="SELECT oids FROM tag.dis_cv_tag_string WHERE ";
        if(requestJson.containsKey("tag_str")){
            List<JSONObject> tagJsons = JSONArray.parseArray(requestJson.get("tag_str").toString(), JSONObject.class);
            for (JSONObject tagJson : tagJsons){
                if(tagJson.containsKey("column")&&tagJson.containsKey("op")&&tagJson.containsKey("value")){
                    if(tag_str){
                        tag_str_sql = tag_str_sql+" and ";
                    }
                    tag_str_sql = tag_str_sql + tagJson.getString("column")+" "+tagJson.getString("op")+" '"+tagJson.getString("value")+"'";
                    tag_str = true;
                }else{
                    JSONObject response=new JSONObject();
                    response.put("ok",false);
                    response.put("code",401);
                    response.put("msg","tag_str column,op,value必须成对出现!");
                    return response.toString();
                }
            }
        }
        //解析tag_int的sql
        boolean tag_int=false;
        String tag_int_sql="select oids from tag.dis_cv_tag_int where ";
        if(requestJson.containsKey("tag_int")){
            List<JSONObject> tagJsons = JSONArray.parseArray(requestJson.get("tag_int").toString(), JSONObject.class);
            for (JSONObject tagJson : tagJsons){
                if(tagJson.containsKey("column")&&tagJson.containsKey("op")&&tagJson.containsKey("value")){
                    if(tag_int){
                        tag_int_sql = tag_int_sql+" and ";
                    }
                    tag_int_sql = tag_int_sql + tagJson.getString("column")+" "+tagJson.getString("op");
                    if(tagJson.get("value") instanceof String){
                        tag_int_sql = tag_int_sql+" '"+tagJson.getString("value")+"'";
                    }else{
                        tag_int_sql = tag_int_sql+" "+tagJson.getString("value");
                    }
                    tag_int = true;
                }else{
                    JSONObject response=new JSONObject();
                    response.put("ok",false);
                    response.put("code",401);
                    response.put("msg","tag_int column,op,value必须成对出现!");
                    return response.toString();
                }
            }
        }
        //解析tag_long的sql
        boolean tag_long=false;
        String tag_long_sql="select oids from tag.dis_cv_tag_Long where ";
        if(requestJson.containsKey("tag_long")){
            List<JSONObject> tagJsons = JSONArray.parseArray(requestJson.get("tag_long").toString(), JSONObject.class);
            for (JSONObject tagJson : tagJsons){
                if(tagJson.containsKey("column")&&tagJson.containsKey("op")&&tagJson.containsKey("value")){
                    if(tag_long){
                        tag_long_sql = tag_long_sql+" and ";
                    }
                    tag_long_sql = tag_long_sql + tagJson.getString("column")+" "+tagJson.getString("op");
                    if(tagJson.get("value") instanceof String){
                        tag_long_sql = tag_long_sql+" '"+tagJson.getString("value")+"'";
                    }else{
                        tag_long_sql = tag_long_sql+" "+tagJson.getString("value");
                    }
                    tag_long = true;
                }else{
                    JSONObject response=new JSONObject();
                    response.put("ok",false);
                    response.put("code",401);
                    response.put("msg","tag_long column,op,value必须成对出现!");
                    return response.toString();
                }
            }
        }
        //解析tag_date的sql
        boolean tag_date=false;
        String tag_date_sql="select oids from tag.dis_cv_tag_date where ";
        if(requestJson.containsKey("tag_date")){
            List<JSONObject> tagJsons = JSONArray.parseArray(requestJson.get("tag_date").toString(), JSONObject.class);
            for (JSONObject tagJson : tagJsons){
                if(tagJson.containsKey("column")&&tagJson.containsKey("op")&&tagJson.containsKey("value")){
                    if(tag_date){
                        tag_date_sql = tag_date_sql+" and ";
                    }
                    tag_date_sql = tag_date_sql + tagJson.getString("column")+" "+tagJson.getString("op")+" '"+tagJson.getString("value")+"'";
                    tag_date = true;
                }else{
                    JSONObject response=new JSONObject();
                    response.put("ok",false);
                    response.put("code",401);
                    response.put("msg","tag_date column,op,value必须成对出现!");
                    return response.toString();
                }
            }
        }

        //组装整体sql
        int i=0;
        if(tag_str){
            i++;
            sql=sql+"("+tag_str_sql+") AS bitmap"+i;
        }
        if(tag_int){
            if(i>0){
                sql=sql+",";
            }
            i++;
            sql=sql+"("+tag_int_sql+") AS bitmap"+i;
        }
        if(tag_long){
            if(i>0){
                sql=sql+",";
            }
            i++;
            sql=sql+"("+tag_long_sql+") AS bitmap"+i;
        }
        if(tag_date){
            if(i>0){
                sql=sql+",";
            }
            i++;
            sql=sql+"("+tag_date_sql+") AS bitmap"+i;
        }
        if(i==0){
            JSONObject response=new JSONObject();
            response.put("ok",false);
            response.put("code",401);
            response.put("msg","筛选条件必须传入");
            return response.toString();
        }else if(i==1){
            sql =sql+" "+subSql+"bitmapToArray(bitmap1)))";
        }else if(i==2){
            sql =sql+" "+subSql+"bitmapToArray(bitmapAnd(bitmap1, bitmap2))))";
        }else if(i==3){
            sql =sql+" "+subSql+"bitmapToArray(bitmapAnd(bitmapAnd(bitmap1, bitmap2),bitmap3))))";
        }else if(i==4){
            sql =sql+" "+subSql+"bitmapToArray(bitmapAnd(bitmapAnd(bitmapAnd(bitmap1, bitmap2),bitmap3),bitmap4))))";
        }
        if(!queryCount){
            if(requestJson.containsKey("count")&&requestJson.containsKey("page")){
                page = requestJson.getString("page");
                count = requestJson.getString("count");
                sql=sql+" order by oid LIMIT "+requestJson.getString("count");
                if(requestJson.getObject("page",Integer.class)>0){
                    sql=sql + " offset "+requestJson.getObject("count",Integer.class)*requestJson.getObject("page",Integer.class);
                }
            }
        }
        request = "{\"@schema\":\"tag\",\"[]\":{\""+requestJson.getString("table_name")+"\": {\"@sql\":\""+sql+"\"},\"page\":"+page+",\"count\":"+count+"}}";
        String result = getData(request, session,"tag");
        JSONObject jsonResult=JSON.parseObject(result);
        if(jsonResult.getString("msg").contains("DB::Exception: Scalar subquery returned empty result of type")){
            JSONObject makeResult=new JSONObject();
            makeResult.put("ok",true);
            makeResult.put("code",200);
            makeResult.put("msg","success");
            if(queryCount){
                makeResult.put("total",0);
            }
            return makeResult.toString();
        }
        if(queryCount){

            if(jsonResult.containsKey("ok")&&jsonResult.getObject("ok",Boolean.class)){
                Object total = JSONArray.parseArray(jsonResult.get("[]").toString(), JSONObject.class).get(0).getJSONObject(requestJson.getString("table_name")).get("total");
                jsonResult.remove("[]");
                jsonResult.put("total",total);
                return jsonResult.toString();
            }else{
                return result;
            }
        }else{
            return result;
        }
    }

    public String getData(String request, HttpServletRequest httpServletRequest, String servicePath) {
        JSONObject response=new JSONObject();
        response.put("ok",false);
        response.put("code",401);
        //取表名
        String tableName="";
        try {
            tableName = JSON.parseObject(JSON.parseObject(request).get("[]")).keySet().iterator().next();
        }catch (Exception e){
            response.put("msg","验证用户权限时解析表名出错,请检查请求参数是否合法!");
            return response.toString();
        }

        HttpSession session = httpServletRequest.getSession();
        String token = httpServletRequest.getHeader("token");
        //第一步取token
        if (token == null) {
            response.put("msg","token不能为空!");
            return response.toString();
        }
        //第二步验证token值对应的权限
        Object token_map_object=redisUtil.get(serviceName+"_token");
        if(token_map_object!=null){
            Map<String, String> token_map = JSONObject.parseObject(JSONObject.toJSONString(token_map_object),
                    Map.class);
            if(token_map!=null){
                String tokenAccess=token_map.get(token);
                boolean hasAccess = false;
                if(tokenAccess==null){
                    response.put("msg","用户没有此表的访问权限,请联系管理员!");
                    return response.toString();
                }
                if (!tokenAccess.equals("ALL")) {
                    String[] tokenAccessList = tokenAccess.toString().split(",");
                    for (String tokenAccessValue : tokenAccessList) {
                        if (tokenAccessValue.equals(servicePath + "." + tableName)) {
                            hasAccess = true;
                            break;
                        }
                    }
                    if (!hasAccess) {
                        response.put("msg","用户没有"+tableName+"表的访问权限,请联系管理员!");
                        return response.toString();
                    }
                }
            }
        }

        // 从redis获取配置信息
        try {
            Object TABLE_KEY_MAP = redisUtil.get(serviceName + "_table_key_map");
            Object TABLE_COLUMN_MAP = redisUtil.get(serviceName + "_table_column_map");
            if (TABLE_KEY_MAP != null) {
                @SuppressWarnings("unchecked")
                Map<String, String> table_key_map = JSONObject.parseObject(JSONObject.toJSONString(TABLE_KEY_MAP),
                        Map.class);
                if (table_key_map != null) {
                    AbstractSQLConfig.TABLE_KEY_MAP.clear();
                    AbstractSQLConfig.TABLE_KEY_MAP = table_key_map;
                }
            }
            if (TABLE_COLUMN_MAP != null) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, String>> table_column_map = JSONObject
                        .parseObject(JSONObject.toJSONString(TABLE_COLUMN_MAP), Map.class);
                if (table_column_map != null) {
                    AbstractSQLConfig.tableColumnMap.clear();
                    AbstractSQLConfig.tableColumnMap = table_column_map;
                }
            }

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        // 从redis获取查询数据
        if (request == null || request.equals("")) {
            return get(request, session);
        }
        String item = DigestUtils.md5DigestAsHex(request.getBytes(StandardCharsets.UTF_8));
        Object value = null;
        boolean redisExce = false;
        try {
            value = redisUtil.hget(serviceName, item);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            redisExce = true;
        }
        if (value != null) {
            String valueS = value.toString();
            if (!valueS.equals("")) {
                accessHistory(token,servicePath,tableName,valueS,session);
                JSONObject jsonResult=JSON.parseObject(valueS);
                jsonResult.remove("execute_sql");
                return jsonResult.toString();
            }

        }
        String result = get(request, session);
        accessHistory(token,servicePath,tableName,result,session);
        if (redisExce) {
            JSONObject jsonResult=JSON.parseObject(result);
            jsonResult.remove("execute_sql");
            return jsonResult.toString();
        }
        if (result.contains("\"code\":200,\"msg\":\"success\"")) {
            if (!redisUtil.hset(serviceName, item, result, 60)) {
                System.err.println(
                        "\n\n\n redis数据存储失败,存储的value:" + result + "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
            }
        }
        JSONObject jsonResult=JSON.parseObject(result);
        jsonResult.remove("execute_sql");
        return jsonResult.toString();
    }

    @GetMapping(value = "refreshConfig")
    public CommonResponse refush() {
        APIJSONParser abstractParser = new APIJSONParser();
        ConfigLoadResponse configLoadResponse = abstractParser.loadAliasConfig();
        if (configLoadResponse.isSuccess()) {
            if (configLoadResponse.getTABLE_KEY_MAP() != null) {
                redisUtil.set(serviceName + "_table_key_map", configLoadResponse.getTABLE_KEY_MAP());
            }
            if (configLoadResponse.getTableColumnMap() != null) {
                redisUtil.set(serviceName + "_table_column_map", configLoadResponse.getTableColumnMap());
            }
        }
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setData(configLoadResponse.getData());
        commonResponse.setMessage(configLoadResponse.getMessage());
        commonResponse.setSuccess(configLoadResponse.isSuccess());
        return commonResponse;
    }

    public void accessHistory(String token,String service_path,String table_alias,String response, HttpSession session){
        saveAccessHistory.saveAccessHistory(token,service_path,table_alias,response,session);
        //Thread t = new Thread(new SaveAccessHistory(token,service_path,table_alias,response,session));
        //t.start();
    }

}
