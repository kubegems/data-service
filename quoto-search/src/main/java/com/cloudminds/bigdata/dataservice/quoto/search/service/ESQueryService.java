package com.cloudminds.bigdata.dataservice.quoto.search.service;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.CommonResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ESQueryService {
    @Autowired
    private RestHighLevelClient client;
    /**
     * 查询所有
     *  1. 高级查询
     *  2. 将查询结果封装为Goods对象，装载到List中
     *  3. 分页。默认显示10条
     */
    public CommonResponse matchQuery(String searchKey){
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
        }catch (Exception e){
            commonResponse.setSuccess(false);
            commonResponse.setMessage(e.getMessage());
            return commonResponse;
        }
        return commonResponse;
    }
}
