package com.cloudminds.bigdata.dataservice.quoto.search.entity;

import lombok.Data;

@Data
public class CommonResponse {
	private boolean success=true;
	private String message="请求成功";
	private Object data;
}
