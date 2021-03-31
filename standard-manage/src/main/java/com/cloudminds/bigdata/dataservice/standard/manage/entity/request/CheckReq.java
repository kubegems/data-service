package com.cloudminds.bigdata.dataservice.standard.manage.entity.request;

import lombok.Data;

@Data
public class CheckReq {
	private byte checkflag;   //术语: 0代表中文名 1代表英文 2代表字段
	private String checkValue;
}
