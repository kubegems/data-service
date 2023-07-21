package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class DbInfo extends BaseEntity {
    private int id;
    private String db_url;
    private String db_name;
    private String userName;
    private String password;
    private String service_name;
    private String service_path;
    private String creator;
    private int common_service = 1;
    private DbInfoExtend extend;
}
