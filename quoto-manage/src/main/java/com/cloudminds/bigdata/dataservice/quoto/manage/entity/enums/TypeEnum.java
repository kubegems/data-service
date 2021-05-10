package com.cloudminds.bigdata.dataservice.quoto.manage.entity.enums;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum TypeEnum implements BaseCodeEnum {

	atomic_quoto(0, "原子指标"), derive_quoto(1, "派生指标"), complex_quoto(2, "衍生指标"),;

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
