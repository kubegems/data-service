package com.cloudminds.bigdata.dataservice.quoto.search.mapper;

import com.cloudminds.bigdata.dataservice.quoto.search.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.quoto.search.entity.TagObject;
import com.cloudminds.bigdata.dataservice.quoto.search.handler.JsonListTypeHandler;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.type.JdbcType;

import java.sql.Array;
import java.util.List;
import java.util.Map;

@Mapper
public interface SearchMapper {
    @Select("select * from tag_object where deleted=0 and code=#{code}")
    @Results(value = {
            @Result(column = "columns", property = "columns", typeHandler = JsonListTypeHandler.class)})
    public TagObject queryTagObjectByCode(String code);
}
