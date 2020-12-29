package com.cloudminds.bigdata.dataservice.quoto.roc.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class RobotCount extends BaseEntity{
private int count;
}
