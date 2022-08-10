package com.cloudminds.bigdata.dataservice.quoto.manage.entity.response;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;
import lombok.Data;

@Data
public class AdjectiveExtend extends Adjective {
    private int dimension_id;
    private String dimension_name;
    private String dimension_code;
    private int dimension_object_id;
    private String dimension_object_name;
}
