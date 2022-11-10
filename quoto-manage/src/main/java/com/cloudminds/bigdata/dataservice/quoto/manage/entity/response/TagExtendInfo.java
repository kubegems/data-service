package com.cloudminds.bigdata.dataservice.quoto.manage.entity.response;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Tag;
import lombok.Data;

@Data
public class TagExtendInfo extends Tag {
    private int use_count;
}
