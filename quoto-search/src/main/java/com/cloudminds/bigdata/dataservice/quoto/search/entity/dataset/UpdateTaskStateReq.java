package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import lombok.Data;

@Data
public class UpdateTaskStateReq {
    private int id;
    private int state;
    private String run_info;
}
