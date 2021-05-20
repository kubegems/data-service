package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class Quoto extends BaseEntity {
	private int id;
	private String name;
	private String field;
	private int business_process_id;
	private int quoto_level;
	private String data_type;
	private String data_unit;
	private int table_id;
	private boolean accumulation;
	private int[] dimension;
	private int origin_quoto;
	private int[] adjective;
	private int[] quotos;
	private int cycle;
	private int type;
	private int state;
	private String expression;
	
	//查询的信息
	private String business_process_name;
	private String data_domain_name;
	private String business_name;
}
