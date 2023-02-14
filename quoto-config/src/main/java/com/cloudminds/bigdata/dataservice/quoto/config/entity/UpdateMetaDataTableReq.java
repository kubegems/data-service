package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class UpdateMetaDataTableReq extends MetaDataTable{
    private String[] updateSql;
}
