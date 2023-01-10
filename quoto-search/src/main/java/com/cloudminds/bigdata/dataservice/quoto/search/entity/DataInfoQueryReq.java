package com.cloudminds.bigdata.dataservice.quoto.search.entity;

import com.alibaba.fastjson.JSONArray;
import lombok.Data;

@Data
public class DataInfoQueryReq {
    private int query;
    private int page;
    private int count;
    private String object_code;
    private String op;
    private JSONArray filter;
    private String column;
    private boolean scroll_search;
    private String scroll_id;
}
