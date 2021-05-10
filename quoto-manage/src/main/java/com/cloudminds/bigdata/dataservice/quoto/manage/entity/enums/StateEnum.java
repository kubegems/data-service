package com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum StateEnum implements BaseCodeEnum{

	init_state(0, "初始状态"),
	active_state(1, "活跃状态"), 
	
	invalid_state(2, "无效状态")
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
