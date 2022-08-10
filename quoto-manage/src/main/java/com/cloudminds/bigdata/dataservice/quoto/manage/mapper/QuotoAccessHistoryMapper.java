package com.cloudminds.bigdata.dataservice.quoto.manage.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.QuotoAccessHistory;

@Mapper
public interface QuotoAccessHistoryMapper {
	@Insert("insert into quoto_access_history(quoto_id,quoto_name,business,theme,level,type,success,message) "
			+ "values(#{quoto_id}, #{quoto_name},#{business},#{theme},#{level},#{type},#{success}, #{message})")
	public void insertAccessHistory(QuotoAccessHistory quotoAccessHistory);
}
