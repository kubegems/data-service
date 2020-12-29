package com.cloudminds.bigdata.dataservice.quoto.roc.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class RobotSize extends BaseEntity {
	private int size;
}
