package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import lombok.Data;

@Data
public class DataAccountReq {
    private int data_type;
    private int data_source_id;
    private String data_source_name;
    private String data_rule;
}
