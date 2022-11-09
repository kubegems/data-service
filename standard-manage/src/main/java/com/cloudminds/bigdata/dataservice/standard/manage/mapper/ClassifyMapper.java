package com.cloudminds.bigdata.dataservice.standard.manage.mapper;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Classify;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface ClassifyMapper {
    @Insert("insert into classify(pid,name,type,creator,create_time,update_time) "
            + "values(#{pid}, #{name}, #{type}, #{creator},now(),now())")
    public int insertClassify(Classify classify);

    @Update("update classify set name=#{name} where id=#{id}")
    public int updateClassifyName(int id,String name);

    @Update("update classify set deleted=1 where id=#{id}")
    public int deleteClassifyName(int id);

    @Select("select * from classify where deleted=0 and pid=#{pid} and type=#{type} and name=#{name}")
    public Classify findClassifyByName(int pid,int type,String name);

    @Select("select * from classify where deleted=0 and pid=#{pid} and type=#{type}")
    public List<Classify> findClassifyByPid(int pid, int type);

    @Select("select * from classify where deleted=0 and type=#{type}")
    public List<Classify> findAllClassify(int type);

    @Select("select * from classify where deleted=0 and id=#{id}")
    public Classify findClassifyById(int id);
}
