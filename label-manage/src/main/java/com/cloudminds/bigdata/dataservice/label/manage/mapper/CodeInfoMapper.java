package com.cloudminds.bigdata.dataservice.label.manage.mapper;

import com.cloudminds.bigdata.dataservice.label.manage.entity.CodeInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface CodeInfoMapper {

    @Select("select * from code_info where code like #{code} and deleted=0")
    public List<CodeInfo> findCodeInfoByLikeCode(String code);
}
