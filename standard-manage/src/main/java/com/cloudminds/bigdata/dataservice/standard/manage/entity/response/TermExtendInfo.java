package com.cloudminds.bigdata.dataservice.standard.manage.entity.response;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class TermExtendInfo extends Term {
    private int classify_id_three;
    private String classify_name_three;
    private int classify_id_two;
    private String classify_name_two;
    private int classify_id_one;
    private String classify_name_one;
}
