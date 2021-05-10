package com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum LevelEnum implements BaseCodeEnum {

	T1(1, "公司"), T2(2, "部门"), T3(3, "分析类"),;

	private int code;
	private String desc;

	private LevelEnum(int code, String desc) {
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
