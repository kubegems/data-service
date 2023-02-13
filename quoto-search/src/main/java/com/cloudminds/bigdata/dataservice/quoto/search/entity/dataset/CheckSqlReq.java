package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import lombok.Data;

@Data
public class CheckSqlReq {
    private int table_id;
    private String sql;
    private boolean hive_type = false;
    private String url;
    private String user;
    private String password;

}
