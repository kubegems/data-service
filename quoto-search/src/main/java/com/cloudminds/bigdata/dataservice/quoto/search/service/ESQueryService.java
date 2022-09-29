package com.cloudminds.bigdata.dataservice.quoto.search.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.*;
import com.cloudminds.bigdata.dataservice.quoto.search.mapper.SearchMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;

@Service
public class ESQueryService {
    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private SearchMapper searchMapper;
    @Autowired
    private Connection hbaseConnection;
    @Autowired
    RestTemplate restTemplate;
    @Value("${elasticsearch.host}")
    private String esHost;
    @Value("${elasticsearch.port}")
    private String esPort;

    /**
     * 查询所有
     * 1. 高级查询
     * 2. 将查询结果封装为Goods对象，装载到List中
     * 3. 分页。默认显示10条
     */
    public CommonResponse matchQuery(String searchKey) {
        CommonResponse commonResponse = new CommonResponse();
        //2. 构建查询请求对象，指定查询的索引名称
        SearchRequest searchRequest = new SearchRequest("label_helper");

        //3. 创建查询条件构建器SearchSourceBuilder
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();


        //match Query即全文检索，它的搜索方式是先将搜索字符串分词，再使用各各词条从索引中搜索。
        MatchQueryBuilder query = QueryBuilders.matchQuery("name", searchKey);
        //query.operator(Operator.AND);//求交集

        //5.指定查询条件
        sourceBuilder.query(query);

        //6.添加查询条件构建器
        searchRequest.source(sourceBuilder);
        sourceBuilder.size(20);

        //1. 查询,获取查询结果
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            //8.获取命中对象
            SearchHits hits = searchResponse.getHits();
            //8.2获取hits数据 数组
            SearchHit[] hits1 = hits.getHits();
            //获取json字符串格式的数据
            List<String> goodsList = new ArrayList<String>();
            for (SearchHit searchHit : hits1) {
                String goodName = searchHit.getSourceAsMap().get("name").toString();
                goodsList.add(goodName);
            }
            commonResponse.setData(goodsList);
        } catch (Exception e) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse boolQuery(String key) {
        CommonResponse commonResponse = new CommonResponse();
        //2. 构建查询请求对象，指定查询的索引名称
        SearchRequest searchRequest = new SearchRequest("label_helper");

        //3. 创建查询条件构建器SearchSourceBuilder
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //boolQuery：对多个查询条件连接。
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", key);
        boolQuery.should(matchQuery);

        RegexpQueryBuilder query = QueryBuilders.regexpQuery("barcode", ".*" + key + ".*");
        boolQuery.should(query);

        //5.指定查询条件
        sourceBuilder.query(boolQuery);

        //6.添加查询条件构建器
        searchRequest.source(sourceBuilder);
        sourceBuilder.size(20);

        //1. 查询,获取查询结果
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            //8.获取命中对象
            SearchHits hits = searchResponse.getHits();
            //8.2获取hits数据 数组
            SearchHit[] hits1 = hits.getHits();
            //获取json字符串格式的数据
            List<String> goodsList = new ArrayList<String>();
            for (SearchHit searchHit : hits1) {
                String goodName = searchHit.getSourceAsMap().get("name").toString();
                goodsList.add(goodName);
            }
            commonResponse.setData(goodsList);
        } catch (Exception e) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonResponse queryDataInfo(String request) {
        CommonResponse commonResponse = new CommonResponse();
        int count = 10;
        int page = 1;
        int query = 2;
        boolean scroll_search = true;
        boolean original_req = false;
        String scroll_id = "";
        //校验参数
        if (StringUtils.isEmpty(request)) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("请求参数不能为空");
            return commonResponse;
        }
        JSONObject jsonObjectRequest = JSONObject.parseObject(request);
        if (jsonObjectRequest == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("请求参数不是个json结构");
            return commonResponse;
        }
        if (jsonObjectRequest.get("count") != null) {
            count = jsonObjectRequest.getIntValue("count");
            if (count > 1000) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("count不能超过1000");
                return commonResponse;
            }
        }

        if (jsonObjectRequest.get("query") != null) {
            query = jsonObjectRequest.getIntValue("query");
        }

        if (jsonObjectRequest.get("page") != null) {
            page = jsonObjectRequest.getIntValue("page");
            if (page < 1) {
                page = 1;
            }
        }

        if (jsonObjectRequest.get("original_req") != null && jsonObjectRequest.getBoolean("original_req")) {
            original_req = true;
        } else {
            if (query != 1) {
                if (jsonObjectRequest.get("scroll_search") == null || jsonObjectRequest.getBoolean("scroll_search")) {
                    if (jsonObjectRequest.get("scroll_id") != null) {
                        scroll_id = jsonObjectRequest.getString("scroll_id");
                    }
                } else {
                    scroll_search = false;
                    if ((page * count + count) > 10000) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("最多不能超过10000条数据");
                        return commonResponse;
                    }
                }
            }
        }

        if (jsonObjectRequest.get("object_code") == null || StringUtils.isEmpty(jsonObjectRequest.get("object_code").toString())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("object_code必须有值");
            return commonResponse;
        }
        //查询对象编码对应的es索引
        TagObject tagObject = searchMapper.queryTagObjectByCode(jsonObjectRequest.get("object_code").toString());
        if (tagObject == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("object_code对应的标签对象不存在");
            return commonResponse;
        }
        if (StringUtils.isEmpty(tagObject.getEs_index())) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(tagObject.getCode() + " 对应的标签对象没有设置es索引,请联系管理员");
            return commonResponse;
        }

        Map<String, String> columnAttribute = new HashMap<>();
        List<ColumnAlias> columnAliases = searchMapper.queryTagObjectColunmAttribute(tagObject.getDatabase(), tagObject.getTable());
        if (columnAliases == null || columnAliases.size() == 0) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(tagObject.getDatabase() + "." + tagObject.getTable() + " 对应的表没在数据服务里配置,请联系管理员");
            return commonResponse;
        } else {
            Map<String, String> columnAttributeTmp = new HashMap<>();
            for (ColumnAlias columnAlias : columnAliases) {
                columnAttributeTmp.put(columnAlias.getColumn_name(), columnAlias.getData_type());
            }
            if (jsonObjectRequest.get("column") == null || StringUtils.isEmpty(jsonObjectRequest.getString("column"))) {
                columnAttribute = columnAttributeTmp;
            } else {
                String columns = jsonObjectRequest.getString("column");
                for (String colunm : columns.split(",")) {
                    if (columnAttributeTmp.containsKey(colunm)) {
                        columnAttribute.put(colunm, columnAttributeTmp.get(colunm));
                    } else {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("标签对象不存在" + colunm + "属性");
                        return commonResponse;
                    }
                }
            }
        }


        Object scroll_id_result = null;
        List<String> rowKeys = new ArrayList<String>();
        //去es请求
        if (original_req) {
            String esReq = jsonObjectRequest.getString("filter");
            if (StringUtils.isEmpty(esReq)) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("filter必须有值");
                return commonResponse;
            }
            String url = "http://" + esHost + ":" + esPort + "/" + tagObject.getEs_index();
            if (query == 1) {
                url = url + "/_count";
            } else {
                url = url + "/_search";
            }
            try {
                ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity(jsonObjectRequest.getJSONObject("filter"), null), String.class);
                if (!responseEntity.getStatusCode().is2xxSuccessful()) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("es服务不可用,请联系管理员");
                    return commonResponse;
                } else {
                    JSONObject esResponse = JSONObject.parseObject(responseEntity.getBody());
                    if (esResponse == null) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("es响应错误,请联系管理员");
                        return commonResponse;
                    }
                    if (esResponse.containsKey("error")) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage(esResponse.getString("error"));
                        return commonResponse;
                    }
                    if (query == 1) {
                        commonResponse.setData(esResponse.getLong("count"));
                        return commonResponse;
                    }
                    JSONObject hitResult = esResponse.getJSONObject("hits");
                    scroll_id_result = hitResult.getJSONObject("total").getLong("value");
                    JSONArray hitJsonArray = hitResult.getJSONArray("hits");
                    if (hitJsonArray != null && hitJsonArray.size() > 0) {
                        for (int i = 0; i < hitJsonArray.size(); i++) {
                            rowKeys.add(hitJsonArray.getJSONObject(i).getJSONObject("_source").getString("rowkey"));
                            if (i == hitJsonArray.size() - 1 && hitJsonArray.getJSONObject(i).containsKey("sort")) {
                                JSONArray sortResult = hitJsonArray.getJSONObject(i).getJSONArray("sort");
                                if (sortResult != null) {
                                    scroll_id_result = sortResult;
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                commonResponse.setMessage(e.getMessage());
                commonResponse.setSuccess(false);
                return commonResponse;
            }
        } else {
            //boolQuery：对多个查询条件连接。
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            //支持and或者or
            Boolean fatherAndOp = true;
            if (jsonObjectRequest.get("op") != null && jsonObjectRequest.get("op").toString().toLowerCase().equals("or")) {
                fatherAndOp = false;
            }
            JSONArray jsonArray = jsonObjectRequest.getJSONArray("filter");
            if (jsonArray == null) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("filter必须有值");
                return commonResponse;
            }
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject subJsonObjectRequest = jsonArray.getJSONObject(i);
                JSONArray subJsonArray = subJsonObjectRequest.getJSONArray("filter");
                if (subJsonArray == null) {
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage("嵌套里的filter必须有值");
                    return commonResponse;
                }
                if(subJsonArray.isEmpty()){
                    continue;
                }
                BoolQueryBuilder subBoolQuery = QueryBuilders.boolQuery();
                for (int j = 0; j < subJsonArray.size(); j++) {
                    JSONObject finalJsonObject = subJsonArray.getJSONObject(j);
                    Boolean finalInOp = true;
                    if (finalJsonObject.get("op") != null && finalJsonObject.get("op").toString().toLowerCase().equals("not in")) {
                        finalInOp = false;
                    }
                    List<String> tagValues = JSONArray.parseArray(finalJsonObject.getString("tag_values"), String.class);
                    if (tagValues == null || tagValues.isEmpty()) {
                        commonResponse.setSuccess(false);
                        commonResponse.setMessage("tag_values必须有值");
                        return commonResponse;
                    }
                    if (finalInOp) {
                        subBoolQuery.must(QueryBuilders.matchQuery("tags", tagValues.toString().replaceAll(",", " ")).operator(Operator.OR));
                    } else {
                        subBoolQuery.mustNot(QueryBuilders.matchQuery("tags", tagValues.toString().replaceAll(",", " ")).operator(Operator.OR));
                    }
                }
                if (fatherAndOp) {
                    boolQuery.must(subBoolQuery);
                } else {
                    boolQuery.should(subBoolQuery);
                }

            }
            //查询总数
            if (query == 1) {
                CountRequest countRequest = new CountRequest();
                // 绑定索引名
                countRequest.indices(tagObject.getEs_index());
                countRequest.query(boolQuery);
                try {
                    CountResponse response = client.count(countRequest, RequestOptions.DEFAULT);
                    commonResponse.setData(response.getCount());
                    return commonResponse;
                } catch (IOException e) {
                    e.printStackTrace();
                    commonResponse.setSuccess(false);
                    commonResponse.setMessage(e.getMessage());
                    return commonResponse;
                }
            }

            //2. 构建查询请求对象，指定查询的索引名称
            SearchRequest searchRequest = new SearchRequest(tagObject.getEs_index());

            //3. 创建查询条件构建器SearchSourceBuilder
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //5.指定查询条件
            sourceBuilder.query(boolQuery);

            //6.添加查询条件构建器
            if (scroll_search) {
                sourceBuilder.sort("rowkey");
                if (!StringUtils.isEmpty(scroll_id)) {
                    sourceBuilder.searchAfter(new Object[]{scroll_id});
                }
            } else {
                sourceBuilder.from((page - 1) * count);
            }
            sourceBuilder.size(count);
            searchRequest.source(sourceBuilder);

            //1. 查询,获取查询结果
            try {
                SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                //8.获取命中对象
                SearchHits hits = searchResponse.getHits();
                //8.2获取hits数据 数组
                SearchHit[] hits1 = hits.getHits();
                //获取json字符串格式的数据
                for (SearchHit searchHit : hits1) {
                    rowKeys.add(searchHit.getSourceAsMap().get("rowkey").toString());
                }
                if (scroll_search&&(!rowKeys.isEmpty())) {
                    scroll_id_result = hits1[hits1.length - 1].getSortValues()[0].toString();
                }
            } catch (Exception e) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage(e.getMessage());
                return commonResponse;
            }
        }

        List<Map<String, Object>> hbaseResult = getDataBatch("bigdata:" + tagObject.getTable(), rowKeys, columnAttribute);
        if (original_req || scroll_search) {
            CommonScrollResponse commonScrollResponse = new CommonScrollResponse();
            commonScrollResponse.setScroll_id(scroll_id_result);
            commonScrollResponse.setData(hbaseResult);
            return commonScrollResponse;
        } else {
            commonResponse.setData(hbaseResult);
            return commonResponse;
        }
    }

    /**
     *
     * @param tableName hbase表名
     * @param rowKeys 表的rowkey数组
     * @param columnAttribute 过滤的列属性
     * @return
     */
    public List<Map<String, Object>> getDataBatch(String tableName, List<String> rowKeys, Map<String, String> columnAttribute) {
        List<Map<String, Object>> dataResult = new ArrayList<Map<String, Object>>();
        if (rowKeys == null || rowKeys.size() == 0) {
            return dataResult;
        }
        List getList = new ArrayList<>();
        try {
            Table table = hbaseConnection.getAdmin().getConnection().getTable(TableName.valueOf(tableName));
            rowKeys.forEach(rowKey -> {
                Get get = new Get(Bytes.toBytes(rowKey));
                getList.add(get);
            });
            Result[] results = table.get(getList);
            Arrays.stream(results).forEach(result -> {
                Map<String, Object> data = new HashMap<>();
                if(columnAttribute.containsKey("rowkey")) {
                    data.put("rowkey", new StringBuffer(Bytes.toString(result.getRow())).reverse().toString());
                }
                Arrays.stream(result.rawCells()).forEach(cell -> {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if (columnAttribute != null && (!StringUtils.isEmpty(columnAttribute.get(qualifier)))) {
                        String type = columnAttribute.get(qualifier).toLowerCase();
                        try {
                            if (type.contains("int")) {
                                data.put(qualifier, Bytes.toInt(CellUtil.cloneValue(cell)));
                            } else if (type.contains("float")) {
                                data.put(qualifier, Bytes.toFloat(CellUtil.cloneValue(cell)));
                            } else {
                                String value = Bytes.toString(CellUtil.cloneValue(cell));
                                if (value.startsWith("{") && value.endsWith("}")) {
                                    data.put(qualifier, JSONObject.parseObject(value));
                                } else {
                                    data.put(qualifier, value);
                                }
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            if (value.startsWith("{") && value.endsWith("}")) {
                                data.put(qualifier, JSONObject.parseObject(value));
                            } else {
                                data.put(qualifier, value);
                            }
                        }
                    }
                });
                dataResult.add(data);
            });
        } catch (IOException e) {
            e.printStackTrace();
            return dataResult;
        }
        return dataResult;
    }

    public CommonResponse queryApiDoc(String object_code) {
        CommonResponse commonResponse = new CommonResponse();
        if(StringUtils.isEmpty(object_code)){
            commonResponse.setSuccess(false);
            commonResponse.setMessage("object_code必传");
            return commonResponse;
        }
        TagObject tagObject = searchMapper.queryTagObjectByCode(object_code);
        if (tagObject == null) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("object_code对应的标签对象不存在");
            return commonResponse;
        }
        List<ExtendField> extendFields = new ArrayList<>();
        ExtendField extendFieldQuery = new ExtendField();
        extendFieldQuery.setName("query");
        extendFieldQuery.setType("int");
        extendFieldQuery.setAllowBlank(true);
        extendFieldQuery.setSample(2);
        extendFieldQuery.setDesc("默认值为2查询数据明细,1查询总条数");
        extendFields.add(extendFieldQuery);

        ExtendField extendFieldScroll = new ExtendField();
        extendFieldScroll.setName("scroll_id");
        extendFieldScroll.setType("String");
        extendFieldQuery.setAllowBlank(true);
        extendFieldScroll.setSample("");
        extendFieldScroll.setDesc("首次查询不传或传空串,后面查询传入前一次查询结果里的scroll_id值");
        extendFields.add(extendFieldScroll);

        ExtendField extendFieldObject = new ExtendField();
        extendFieldObject.setName("object_code");
        extendFieldObject.setType("String");
        extendFieldObject.setAllowBlank(true);
        extendFieldObject.setSample(object_code);
        extendFieldObject.setDesc("标签对象编码");
        extendFields.add(extendFieldObject);

        ExtendField extendFieldColumn = new ExtendField();
        extendFieldColumn.setName("column");
        extendFieldColumn.setType("String");
        extendFieldColumn.setAllowBlank(true);
        extendFieldColumn.setSample("");
        extendFieldColumn.setDesc("不传或传空串返回所有的列,指定列请用逗号隔开：XXX,XXX");
        extendFields.add(extendFieldColumn);

        ExtendField extendFieldOp = new ExtendField();
        extendFieldOp.setName("op");
        extendFieldOp.setType("String");
        extendFieldOp.setAllowBlank(true);
        extendFieldOp.setSample("and");
        extendFieldOp.setDesc("默认值为and,filter的数组之间且操作;or filter的数组之间或操作");
        extendFields.add(extendFieldOp);

        ExtendField extendFieldFilter = new ExtendField();
        extendFieldFilter.setName("filter");
        extendFieldFilter.setType("Array");
        extendFieldFilter.setAllowBlank(false);
        extendFieldFilter.setSample("[{\"filter\":[{\"op\":\"in\",\"tag_values\":[\"svr1001005001_001\",\"svr1003002001_016\"]},{\"op\":\"not in\",\"tag_values\":[\"svr1001005001_001\",\"svr1001005001_001\"]}]},{\"filter\":[{\"op\":\"not in\",\"tag_values\":[\"svr1001001003_001\"]}]}]");
        extendFieldFilter.setDesc("1：空的数组[]查询所有的数据 2：最外层数组之间的逻辑由外层的op控制,最里层的filter数组都是且操作 3：op为in表示被打上tag_values里的任意一个标签,not in表示没被打上tag_values里的任意一个标签");
        extendFields.add(extendFieldFilter);
        commonResponse.setData(extendFields);
        return commonResponse;
    }
}
