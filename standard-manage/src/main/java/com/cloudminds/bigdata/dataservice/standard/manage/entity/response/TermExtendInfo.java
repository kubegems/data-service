package com.cloudminds.bigdata.dataservice.standard.manage.entity.response;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class TermExtendInfo extends Term {
    private int classify_id;
    private String classify_name;
}
