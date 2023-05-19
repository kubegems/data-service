package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class ActiveUser {
    private String user_name;
    private int access_cnt;
}
