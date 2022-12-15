package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class MetaDataTableExtendInfo extends MetaDataTable{
    private String business_process_name;
    private String theme_name;
    private int business_id_three_level;
    private String business_name_three_level;
    private int business_id_two_level;
    private String business_name_two_level;
    private int business_id_one_level;
    private String business_name_one_level;
}
