package com.cloudminds.bigdata.dataservice.standard.manage.entity;

import lombok.Data;

@Data
public class Field {
	private String name;
	private String zh_name;
	private String type;
	private String scope;
	private String sample;
	private boolean allowBlank;
	private String desc;
}
