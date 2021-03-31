package com.cloudminds.bigdata.dataservice.standard.manage.entity.enums;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum StateEnum implements BaseCodeEnum{

	develop_state(0, "初稿"),
	wait_state(1, "待审核"), 
	
	pass_state(2, "审核通过"),
	publish_state(3, "已发布"),
	offline_state(4, "下线"), 
	oldpublish_state(5,"历史版本_已发布"),
	oldoffline_state(6,"历史版本_下线"),
	;

	private int code;
	private String desc;

	private StateEnum(int code, String desc) {
		this.code = code;
		this.desc = desc;
	}

	public int getCode() {
		return code;
	}

	public String getDesc() {
		return desc;
	}

}
