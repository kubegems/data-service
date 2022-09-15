package com.cloudminds.bigdata.dataservice.quoto.search.mapper;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.TagObject;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

@Mapper
public interface SearchMapper {
    @Select("select * from tag_object where deleted=0 and code=#{code}")
    public TagObject queryTagObjectByCode(String code);

    @Select("select * from bigdata_dataservice.Column_alias where is_delete=0 and table_id = (select t.id from bigdata_dataservice.Table_info t left join bigdata_dataservice.Database_info d on t.database_id=d.id where t.table_alias=#{tableName} and d.`database`=#{db})")
    public List<ColumnAlias> queryTagObjectColunmAttribute(String db, String tableName);
}
