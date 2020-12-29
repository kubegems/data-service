package com.cloudminds.bigdata.dataservice.quoto.roc.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class RobotHours extends BaseEntity{
private float hours;
}
