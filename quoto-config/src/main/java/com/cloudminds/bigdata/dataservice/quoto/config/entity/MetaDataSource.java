package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class MetaDataSource extends NewBaseEntity{
    private int table_type_id;
    private String datahub_instance;
    private String datahub_ingestion_source;
}
