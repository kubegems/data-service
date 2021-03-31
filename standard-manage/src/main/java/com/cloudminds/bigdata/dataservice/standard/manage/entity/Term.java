package com.cloudminds.bigdata.dataservice.standard.manage.entity;


import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class Term extends BaseEntity{
	private int id;
	private String zh_name;
	private String en_name;
	private String term_field;

}
