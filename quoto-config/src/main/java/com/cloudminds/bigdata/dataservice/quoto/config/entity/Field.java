package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class Field {
	private String name;
	private String type;
	private String sample;
	private boolean allowBlank;
	private String desc;
}
