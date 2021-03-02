package com.cloudminds.bigdata.dataservice.quoto.chatbot.mapper;



import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;



@Mapper
public interface ColumnAliasMapper {
	@Select("SELECT TDATE FROM SV.KYLIN_HITLOG_2003 WHERE TDATE = '2020-10-27 00:00:00' LIMIT 100 OFFSET 0")
	public List<String> aaa();
}
