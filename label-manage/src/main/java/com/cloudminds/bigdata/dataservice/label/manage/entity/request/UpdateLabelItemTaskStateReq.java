package com.cloudminds.bigdata.dataservice.label.manage.entity.request;

import lombok.Data;

@Data
public class UpdateLabelItemTaskStateReq {
    private int id;
    private int state;
    private String run_info;
}
