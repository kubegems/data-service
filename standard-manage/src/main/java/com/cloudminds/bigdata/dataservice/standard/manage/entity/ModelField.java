package com.cloudminds.bigdata.dataservice.standard.manage.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class ModelField extends CommonField {
	private String model_id;
	private String model_name;
	private String model_version;
}
