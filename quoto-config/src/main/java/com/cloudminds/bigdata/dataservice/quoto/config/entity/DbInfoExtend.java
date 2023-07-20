package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class DbInfoExtend {
    private String host;
    private int port;
    private String service_name;
    private String connect_param;
}
