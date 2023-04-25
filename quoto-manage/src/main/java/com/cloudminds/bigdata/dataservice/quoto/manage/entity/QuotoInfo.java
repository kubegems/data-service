package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
@EqualsAndHashCode(callSuper = false)
@Data
public class QuotoInfo extends BaseEntity{
	private int id;
	private int table_id;
	private String quoto_name;
	private String quoto_sql;
	private int state;
	private boolean is_column;
}
