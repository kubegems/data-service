package com.cloudminds.bigdata.dataservice.quoto.manage.entity.response;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Dimension;
import lombok.Data;

@Data
public class DimensionExtend extends Dimension {
    private String dimension_object_name;
    private String dimension_object_code;
}
