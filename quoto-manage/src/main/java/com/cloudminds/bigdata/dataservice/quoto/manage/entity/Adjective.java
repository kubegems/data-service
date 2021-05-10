package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.BaseEntity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class Adjective extends BaseEntity {
	private int id;
	private String name;
	private String code;
	private String code_name;
	private int type;
	private int quotoNum;
}
