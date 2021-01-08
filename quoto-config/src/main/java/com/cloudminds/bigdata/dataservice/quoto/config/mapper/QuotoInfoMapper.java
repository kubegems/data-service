package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.QuotoInfo;


@Mapper
public interface QuotoInfoMapper {
	@Select("SELECT * FROM Quoto_info WHERE is_delete=0 AND table_id=#{tableId}")
	public List<QuotoInfo> getQuotoInfoByTableId(int tableId);
	
	@Update("update Quoto_info set state=#{state} where id=#{id}")
	public int updateQuotoInfoStatus(int id, int state);

	@Update("update Quoto_info set is_delete=#{delete} where id=#{id}")
	public int updateQuotoInfoDelete(int id,int delete);
	
	@Select("SELECT * FROM Quoto_info WHERE quoto_name=#{quoto_name} AND table_id=#{table_id} AND quoto_sql=#{quoto_sql}")
	public QuotoInfo getQuotoInfo(QuotoInfo quotoInfo);
	
	@Update("insert into QuotoInfo(table_id,quoto_name,quoto_sql) VALUES(#{table_id},#{quoto_name},#{quoto_sql})")
	public int insertQuotoInfo(QuotoInfo quotoInfo);
}
