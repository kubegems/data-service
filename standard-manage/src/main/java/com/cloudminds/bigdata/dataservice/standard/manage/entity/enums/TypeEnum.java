package com.cloudminds.bigdata.dataservice.standard.manage.entity.enums;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum TypeEnum implements BaseCodeEnum {

	cloud(0, "业务"), terminal(1, "终端"),;

	private int code;
	private String desc;

	private TypeEnum(int code, String desc) {
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
