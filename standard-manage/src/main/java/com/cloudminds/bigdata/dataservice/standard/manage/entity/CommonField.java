package com.cloudminds.bigdata.dataservice.standard.manage.entity;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class CommonField extends BaseEntity{
	private int type;
	private String version;
	private List<Field> fields;
}
