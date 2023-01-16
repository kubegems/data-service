package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import lombok.Data;

@Data
public class AnalyseFilter {
    private boolean success=true;
    private String message="请求成功";
    private String[] tag_item_complexs;
    private String[] tag_enum_values;
}
