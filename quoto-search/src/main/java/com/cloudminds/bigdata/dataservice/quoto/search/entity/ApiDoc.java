package com.cloudminds.bigdata.dataservice.quoto.search.entity;

import lombok.Data;

import java.util.List;

@Data
public class ApiDoc {
    private List<ExtendField> requestParament;
    private String service_path="/search/label/queryDataInfo";
    private String method="post";
}
