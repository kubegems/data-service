package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class UpdateMetaDataTableReq extends MetaDataTable{
    private String[] updateSql;
    private List<Column> updateColumn;
    private List<Column> oldUpdateColumn;
    private List<Column> deleteColumn;
    private List<Column> insertColumn;
}
