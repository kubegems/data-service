package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = false)
@Data
public class Quoto extends BaseEntity {
	private int id;
	private String name;
	private String field;
	private int theme_id;
	private Integer business_process_id;
	private int quoto_level;
	private String data_type;
	private String data_unit;
	private int table_id;
	private boolean accumulation;
	private int[] dimension;
	private int origin_quoto;
	private int[] adjective;
	private int[] quotos;
	private int time_column_id;
	private String sql;
	private boolean use_sql;
	private int cycle;
	private int type;
	private int state;
	private String expression;
	
	//查询的信息
	private String business_process_name;
	private String theme_name;
	private int business_id_three_level;
	private String business_name_three_level;
	private int business_id_two_level;
	private String business_name_two_level;
	private int business_id_one_level;
	private String business_name_one_level;
	private List<Tag> tags;
}
