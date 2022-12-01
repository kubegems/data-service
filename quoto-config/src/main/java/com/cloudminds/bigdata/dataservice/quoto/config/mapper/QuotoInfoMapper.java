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

	@Update("update Quoto_info set is_delete=#{delete} where table_id=#{tableId}")
	public int updateQuotoInfoDeleteByTableId(int tableId,int delete);
	
	@Update("update Quoto_info set quoto_name=#{quoto_name},des=#{des},quoto_sql=#{quoto_sql},is_delete=0,state=1 where id=#{id}")
	public int updateQuotoInfo(QuotoInfo quotoInfo);
	
	@Select("SELECT * FROM Quoto_info WHERE quoto_name=#{quoto_name} AND table_id=#{table_id}")
	public QuotoInfo getQuotoInfo(QuotoInfo quotoInfo);
	
	@Select("SELECT * FROM Quoto_info WHERE quoto_name=#{quotoName} AND table_id=#{table_id} AND is_delete=0 limit 1")
	public QuotoInfo getQuotoInfoByQuotoName(String quotoName,int table_id);
	
	@Select("SELECT * FROM Quoto_info WHERE is_delete=0 AND id=#{id}")
	public QuotoInfo getQuotoInfoById(int id);
	
	@Update("insert into Quoto_info(table_id,quoto_name,quoto_sql,des) VALUES(#{table_id},#{quoto_name},#{quoto_sql},#{des})")
	public int insertQuotoInfo(QuotoInfo quotoInfo);
	
	@Select("SELECT name from quoto where field=#{field} and deleted=0 and state=1 limit 1")
	public String getQuotoByField(String field);
}
