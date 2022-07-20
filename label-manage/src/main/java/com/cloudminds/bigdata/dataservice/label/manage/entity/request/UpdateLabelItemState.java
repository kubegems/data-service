package com.cloudminds.bigdata.dataservice.label.manage.entity.request;

import lombok.Data;

@Data
public class UpdateLabelItemState {
    private String[] tag_ids;
    private int state;
    private String updater;
}
