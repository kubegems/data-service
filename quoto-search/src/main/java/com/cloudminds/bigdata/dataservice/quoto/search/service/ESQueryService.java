package com.cloudminds.bigdata.dataservice.quoto.search.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.TagObject;
import com.cloudminds.bigdata.dataservice.quoto.search.mapper.SearchMapper;
import org.apache.avro.data.Json;
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
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

        //  查询条件 match_all查询
        /*        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();*/


        /*//term Query为精确查询，在搜索时会整体匹配关键字，不再将关键字分词。
        QueryBuilder query = QueryBuilders.termQuery("title", "华为");*/


        //match Query即全文检索，它的搜索方式是先将搜索字符串分词，再使用各各词条从索引中搜索。
        MatchQueryBuilder query = QueryBuilders.matchQuery("name", searchKey);
        //query.operator(Operator.AND);//求交集

        /*//模糊查询 wildcard：会对查询条件进行分词
        WildcardQueryBuilder query = QueryBuilders.wildcardQuery("title", "华*");//华后多个字符
        //正则查询
        RegexpQueryBuilder query = QueryBuilders.regexpQuery("title", "\\w+(.)*");
        //前缀查询
        PrefixQueryBuilder query = QueryBuilders.prefixQuery("brandName", "三");*/


        /*//范围查询 以price 价格为条件
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price");
        //指定下限
        query.gte(2000);
        //指定上限
        query.lte(3000);*/


        /*//queryString
        QueryStringQueryBuilder query =
                QueryBuilders.queryStringQuery("华为手机")
                        .field("title").field("categoryName").field("brandName")
                        .defaultOperator(Operator.AND);*/


        /*//boolQuery：对多个查询条件连接。
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.构建各个查询条件
        //2.1 查询品牌名称为:华为
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("brandName", "华为");
        boolQuery.must(termQueryBuilder);

        //2.2. 查询标题包含：手机
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("title", "手机");
        boolQuery.filter(matchQuery);

        //2.3 查询价格在：2000-3000
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
        rangeQuery.gte(2000);
        rangeQuery.lte(3000);
        boolQuery.filter(rangeQuery);*/



        /*//高亮查询
        // 1. 查询title包含手机的数据
        MatchQueryBuilder query = QueryBuilders.matchQuery("title", "手机");

        sourceBulider.query(query);

        //设置高亮
        HighlightBuilder highlighter = new HighlightBuilder();
        //设置三要素
        highlighter.field("title");
        //设置前后缀标签
        highlighter.preTags("<font color='red'>");
        highlighter.postTags("</font>");

        //加载已经设置好的高亮配置
        sourceBulider.highlighter(highlighter);*/


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
        /*    String sourceAsString = searchHit.getSourceAsString();
            //转为java对象

            Goods goods = JSON.parseObject(sourceAsString, Goods.class);
            goodsList.add(goods);*/
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



        /*//模糊查询 wildcard：会对查询条件进行分词
        WildcardQueryBuilder query = QueryBuilders.wildcardQuery("title", "华*");//华后多个字符
        //正则查询
        RegexpQueryBuilder query = QueryBuilders.regexpQuery("title", "\\w+(.)*");
        //前缀查询
        PrefixQueryBuilder query = QueryBuilders.prefixQuery("brandName", "三");*/


        /*//范围查询 以price 价格为条件
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price");
        //指定下限
        query.gte(2000);
        //指定上限
        query.lte(3000);*/


        /*//queryString
        QueryStringQueryBuilder query =
                QueryBuilders.queryStringQuery("华为手机")
                        .field("title").field("categoryName").field("brandName")
                        .defaultOperator(Operator.AND);*/


        //boolQuery：对多个查询条件连接。
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", key);
        boolQuery.should(matchQuery);

        RegexpQueryBuilder query = QueryBuilders.regexpQuery("barcode", ".*" + key + ".*");
        boolQuery.should(query);



        /*//高亮查询
        // 1. 查询title包含手机的数据
        MatchQueryBuilder query = QueryBuilders.matchQuery("title", "手机");

        sourceBulider.query(query);

        //设置高亮
        HighlightBuilder highlighter = new HighlightBuilder();
        //设置三要素
        highlighter.field("title");
        //设置前后缀标签
        highlighter.preTags("<font color='red'>");
        highlighter.postTags("</font>");

        //加载已经设置好的高亮配置
        sourceBulider.highlighter(highlighter);*/


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
        /*    String sourceAsString = searchHit.getSourceAsString();
            //转为java对象

            Goods goods = JSON.parseObject(sourceAsString, Goods.class);
            goodsList.add(goods);*/
            }
            commonResponse.setData(goodsList);
        } catch (Exception e) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
        return commonResponse;
    }

    public CommonQueryResponse queryDataInfo(String request) {
        CommonQueryResponse commonResponse = new CommonQueryResponse();
        int count = 10;
        int page = 1;
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

        if (jsonObjectRequest.get("page") != null) {
            page = jsonObjectRequest.getIntValue("page");
            if (page < 1) {
                page = 1;
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
        List<ColumnAlias> columnAliases = searchMapper.queryTagObjectColunmAttribute(tagObject.getDatabase(),tagObject.getTable());
        if(columnAliases==null||columnAliases.size()==0){
            commonResponse.setSuccess(false);
            commonResponse.setMessage(tagObject.getDatabase() + "."+tagObject.getTable()+" 对应的表没在数据服务里配置,请联系管理员");
            return commonResponse;
        }
        //2. 构建查询请求对象，指定查询的索引名称
        SearchRequest searchRequest = new SearchRequest(tagObject.getEs_index());

        //3. 创建查询条件构建器SearchSourceBuilder
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //boolQuery：对多个查询条件连接。
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //支持and或者or
        Boolean fatherAndOp = true;
        if (jsonObjectRequest.get("op") != null && jsonObjectRequest.get("op").toString().toLowerCase().equals("or")) {
            fatherAndOp = false;
        }
        JSONArray jsonArray = jsonObjectRequest.getJSONArray("filter");
        if (jsonArray == null || jsonArray.isEmpty()) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage("filter必须有值");
            return commonResponse;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject subJsonObjectRequest = jsonArray.getJSONObject(i);
            JSONArray subJsonArray = subJsonObjectRequest.getJSONArray("filter");
            if (subJsonArray == null || subJsonArray.isEmpty()) {
                commonResponse.setSuccess(false);
                commonResponse.setMessage("嵌套里的filter必须有值");
                return commonResponse;
            }
            BoolQueryBuilder subBoolQuery = QueryBuilders.boolQuery();
            for (int j = 0; j < subJsonArray.size(); j++) {
                JSONObject finalJsonObject = subJsonArray.getJSONObject(j);
                Boolean finalInOp = true;
                if (finalJsonObject.get("op") != null && finalJsonObject.get("op").toString().toLowerCase().equals("not in")) {
                    finalInOp = false;
                }
                ArrayList<String> tagValues = finalJsonObject.getObject("tag_values", ArrayList.class);
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

        //5.指定查询条件
        sourceBuilder.query(boolQuery);

        //6.添加查询条件构建器
        searchRequest.source(sourceBuilder);
        sourceBuilder.size(count);
        sourceBuilder.from((page - 1) * count);
        //1. 查询,获取查询结果
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            //8.获取命中对象
            SearchHits hits = searchResponse.getHits();
            //8.2获取hits数据 数组
            SearchHit[] hits1 = hits.getHits();
            //获取json字符串格式的数据
//            List<String> rowKeys = new ArrayList<String>();
//            for (SearchHit searchHit : hits1) {
//                rowKeys.add(searchHit.getSourceAsMap().get("rowkey").toString());
//            }
//            Map<String,String> columnAttribute = new HashMap<>();
//            for(ColumnAlias columnAlias:columnAliases){
//                columnAttribute.put(columnAlias.getColumn_name(),columnAlias.getData_type());
//            }
//            List<Map<String, Object>> hbaseResult = getDataBatch("bigdata:"+tagObject.getTable(),rowKeys,columnAttribute);

            List<Map<String, Object>> rowKeystest = new ArrayList<>();
            for (SearchHit searchHit : hits1) {
                rowKeystest.add(searchHit.getSourceAsMap());
            }
            commonResponse.setCurrentPage(page);
            commonResponse.setTotal(hits.getTotalHits().value);
            commonResponse.setData(rowKeystest);
        } catch (Exception e) {
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
        return commonResponse;
    }

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
                Arrays.stream(result.rawCells()).forEach(cell -> {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if(columnAttribute!=null&&(!StringUtils.isEmpty(columnAttribute.get(qualifier)))){
                        String type = columnAttribute.get(qualifier).toLowerCase();
                        if(type.contains("int")){
                            data.put(qualifier, Bytes.toInt(CellUtil.cloneValue(cell)));
                        }else if(type.contains("float")){
                            data.put(qualifier, Bytes.toFloat(CellUtil.cloneValue(cell)));
                        }else{
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            if(value.startsWith("{")&&value.endsWith("}")){
                                data.put(qualifier, JSONObject.parseObject(value));
                            }else{
                                data.put(qualifier, value);
                            }
                        }
                    }else{
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        if(value.startsWith("{")&&value.endsWith("}")){
                            data.put(qualifier, JSONObject.parseObject(value));
                        }else{
                            data.put(qualifier, value);
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
}
