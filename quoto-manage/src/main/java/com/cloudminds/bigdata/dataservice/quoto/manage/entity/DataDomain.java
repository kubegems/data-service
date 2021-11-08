package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class DataDomain extends BaseEntity {
	private int id;
	private int business_id;
	private String name;
}
