package com.cloudminds.bigdata.dataservice.quoto.manage.entity.request;

import lombok.Data;

@Data
public class CheckReq {
	private byte checkflag;   //修饰词: 0名字 1编码 2编码简称  指标 0 名字 1字段
	private String checkValue;
}
