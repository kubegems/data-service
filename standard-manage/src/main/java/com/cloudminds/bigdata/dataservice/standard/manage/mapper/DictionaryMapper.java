package com.cloudminds.bigdata.dataservice.standard.manage.mapper;

import com.cloudminds.bigdata.dataservice.standard.manage.entity.Dictionary;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.Term;
import com.cloudminds.bigdata.dataservice.standard.manage.entity.response.DictionaryExtendInfo;
import com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface DictionaryMapper {
    @Select("select * from dictionary where zh_name=#{zh} and deleted=0")
    public Dictionary findDictionaryByZh(String zh);

    @Select("select * from dictionary where en_name=#{en} and deleted=0")
    public Dictionary findDictionaryByEn(String en);

    @Select("select * from dictionary where code=#{code} and deleted=0")
    public Dictionary findDictionaryByCode(String code);

    @Select("select * from dictionary where id=#{id} and deleted=0")
    public Dictionary findDictionaryById(int id);

    @Update({"<script> update dictionary set deleted=null where id in <foreach collection='array' item='id' index='no' open='(' separator=',' close=')'> #{id} </foreach></script>"})
    public int batchDeleteDictionary(int[] id);

    @Update("update dictionary set state=1 where id=#{id}")
    public int onlineDictionary(int id);

    @Update("update dictionary set zh_name=#{zh_name},classify_id=#{classify_id},en_name=#{en_name},code=#{code},fields=#{fields,typeHandler=com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler},descr=#{descr} where id=#{id}")
    public int updateDictionary(Dictionary dictionary);

    @Update("update dictionary set state=2 where id=#{id}")
    public int offlineDictionary(int id);

    @Insert("insert into dictionary(zh_name, classify_id,en_name,code,fields,create_time,update_time, creator,descr,state) values(#{zh_name},#{classify_id}, #{en_name}, #{code}, #{fields,typeHandler=com.cloudminds.bigdata.dataservice.standard.manage.handler.JsonListTypeHandler},now(),now(), #{creator}, #{descr}, #{state})")
    public int insertDictionary(Dictionary dictionary);

    @Select("select d.*,three.id as classify_id,three.name as classify_name from dictionary d left join classify three on d.classify_id=three.id left join classify two on three.pid=two.id left join classify one on two.pid=one.id where d.deleted=0 ${condition} order by d.update_time desc limit #{startLine},#{size}")
    @Result(column = "fields", property = "fields", typeHandler = JsonListTypeHandler.class)
    public List<DictionaryExtendInfo> queryDictionary(String condition, int startLine, int size);

    @Select("select * from dictionary where deleted=0 and classify_id=#{classify_id}")
    public List<Dictionary> queryDictionaryByClassify(int classify_id);

    @Select("select count(*) from dictionary d left join classify three on d.classify_id=three.id left join classify two on three.pid=two.id left join classify one on two.pid=one.id where d.deleted=0 ${condition}")
    public int queryDictionaryCount(String condition);
}
