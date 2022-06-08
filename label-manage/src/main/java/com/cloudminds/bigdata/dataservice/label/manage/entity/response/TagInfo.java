package com.cloudminds.bigdata.dataservice.label.manage.entity.response;

import lombok.Data;

@Data
public class TagInfo {
    private String tag_id;
    private String tag_name;
    private int value_type=0;
    private boolean finale=false;
}
