package com.cloudminds.bigdata.dataservice.quoto.manage.entity;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.BaseEntity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class AdjectiveType extends BaseEntity {
	private int id;
	private String name;
}
