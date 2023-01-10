package com.cloudminds.bigdata.dataservice.label.manage.entity;

import lombok.Data;

import java.util.List;

@Data
public class TagItemComplexExtend extends TagItemComplex{
    private List<TagEnumValueExtend> tag_enum_values_list;
}
