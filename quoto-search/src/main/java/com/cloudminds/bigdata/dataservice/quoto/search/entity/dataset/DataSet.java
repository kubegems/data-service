package com.cloudminds.bigdata.dataservice.quoto.search.entity.dataset;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.BaseEntity;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.Column;
import lombok.Data;

import java.util.List;

@Data
public class DataSet extends BaseEntity {
    private int id;
    private String name;
    private int data_type;
    private int data_source_id;
    private String data_source_name;
    private String data_source_type;
    private int data_connect_type;
    private int directory_id;
    private String data_rule;
    private List<Column> data_columns;
    private String[] tag_item_complexs;
    private String[] tag_enum_values;
    private String mapping_ck_table;
    private int state;
    private String message;
}
