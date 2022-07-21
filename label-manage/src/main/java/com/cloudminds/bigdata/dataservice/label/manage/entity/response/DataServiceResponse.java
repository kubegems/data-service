package com.cloudminds.bigdata.dataservice.label.manage.entity.response;

import lombok.Data;

@Data
public class DataServiceResponse {
private String msg;
private int code;
private boolean ok;
private int total;
}
