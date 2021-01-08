package com.cloudminds.bigdata.dataservice.quoto.config.entity;

import lombok.Data;

@Data
public class CommonResponse {
	private boolean success=true;
	private String message="请求成功";
	private Object data;
}
