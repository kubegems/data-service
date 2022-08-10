package com.cloudminds.bigdata.dataservice.quoto.manage.entity.response;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Field;
import lombok.Data;

@Data
public class ExtendField {
    private boolean allowBlank=true;
    private String name;
    private String type;
    private Object sample;
    private String desc;
}
