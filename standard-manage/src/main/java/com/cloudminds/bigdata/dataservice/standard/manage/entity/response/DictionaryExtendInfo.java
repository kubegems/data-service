package com.cloudminds.bigdata.dataservice.standard.manage.entity.response;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Dictionary;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class DictionaryExtendInfo extends Dictionary {
    private int classify_id;
    private String classify_name;
}
