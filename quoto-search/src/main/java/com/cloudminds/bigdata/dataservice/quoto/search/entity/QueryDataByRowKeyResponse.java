package com.cloudminds.bigdata.dataservice.quoto.search.entity;

import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class QueryDataByRowKeyResponse {
    private Map<String,Object> dataInfo;
    private Set<String> tags;
}
