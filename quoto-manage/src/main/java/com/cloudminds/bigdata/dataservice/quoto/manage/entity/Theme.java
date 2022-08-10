package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class Theme extends BaseEntity {
	private int id;
	private int business_id;
	private String name;
	private String en_name;
	private String code;
	private int business_id_three_level;
	private String business_name_three_level;
	private int business_id_two_level;
	private String business_name_two_level;
	private int business_id_one_level;
	private String business_name_one_level;
}
