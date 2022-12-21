package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class DeleteReq {
private int id;
private int[] ids;
private String database_name;
private String name;
private int table_type;
}
