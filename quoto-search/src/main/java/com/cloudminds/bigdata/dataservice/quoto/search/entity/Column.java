package com.cloudminds.bigdata.dataservice.quoto.search.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public class Column{
	private String name;
	private String comment;
	private String type;
}
