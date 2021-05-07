package com.cloudminds.bigdata.dataservice.quoto.manage.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.AdjectiveType;

@Mapper
public interface AdjectiveTypeMapper {
	@Select("select * from adjective_type where deleted=0")
	public List<AdjectiveType> findAllAdjectiveType();
}
