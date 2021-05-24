package com.cloudminds.bigdata.dataservice.quoto.manage.entity.response;

import java.util.Set;

import lombok.Data;

@Data
public class DataCommonResponse {
	private boolean success=true;
	private String message="请求成功";
	private Object data;
	private int type=-1; //0代表无数据 1代表list size为1 2代表list size为多个  3代表从外面传的数进来
	private Set<String> dimensions;
	private int[] dimensionIds;
	private int cycle;
	private String field;
}
