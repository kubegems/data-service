package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class HistoryDataAddDataBase {
    private String database_name;
    private String table_name;
    private int table_type;
    private String model_level;
    private int theme_id;
    private String data_domain;
    private String mapping_instance_table;
}
