package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

import java.util.List;
@Data
public class MetaDataTable extends NewBaseEntity{
    private int id;
    private String database_name;
    private String name;
    private int table_type=1;
    private String storage_format;
    private boolean external_table;
    private boolean system_storage_location;
    private String storage_location;
    private boolean system_delimiter;
    private String delimiter;
    private boolean partition;
    private String model_level;
    private int life_cycle;
    private String data_domain;
    private int theme_id;
    private String ddl;
    private List<Column> columns;
    private List<Partition_field> partition_field;
}
