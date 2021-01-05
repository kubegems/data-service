package com.cloudminds.bigdata.dataservice.quoto.roc.entity;

import lombok.Data;

@Data
public class QuotoResponse {
private boolean success=false;
private String message="数据请求失败!";
private Object data;
}
