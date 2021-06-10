package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
@Data
public class ApiDoc extends BaseEntity{
	private int id;
	private String name;
	private String service_path;
	private String method;	
	private List<Field> parameters;
	private String example_req;
	private String example_response;
}
