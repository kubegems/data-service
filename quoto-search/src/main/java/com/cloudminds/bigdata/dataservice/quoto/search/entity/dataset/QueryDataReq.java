package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import lombok.Data;

import java.util.Set;

@Data
public class QueryDataReq {
    private int id;
    private int page;
    private int count;
    private int query=2;  //2是查数据 1查总量
    private String scroll_id;
    private boolean scroll_search=true;
    private Set<String> order;
}
