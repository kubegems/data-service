package com.cloudminds.bigdata.dataservice.quoto.manage.mapper;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Dimension;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.DimensionObject;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.DimensionExtend;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface DimensionMapper {
    @Select("select d.*,dd.`name` as dimension_object_name,dd.`code` as dimension_object_code from (select * from Column_alias where table_id=#{tableId} and is_delete=0) c left join dimension d on c.column_alias=d.code left join dimension_object dd on d.dimension_object_id=dd.id where d.deleted=0 and dd.code!='time'")
    public List<DimensionExtend> querySupportDimension(int tableId);

    @Select("select d.*,dd.`name` as dimension_object_name,dd.`code` as dimension_object_code from dimension d left join dimension_object dd on d.dimension_object_id=dd.id where d.deleted=0 and dd.code='time'")
    public List<DimensionExtend> queryTimeDimension();

    @Select("select * from dimension_object where deleted=0 and name=#{name}")
    public DimensionObject queryDimensionObjectByName(String name);

    @Select("select * from dimension_object where deleted=0 and id=#{id}")
    public DimensionObject queryDimensionObjectById(int id);

    @Select("select * from dimension_object where deleted=0 and code=#{code}")
    public DimensionObject queryDimensionObjectByCode(String code);

    @Insert("insert into dimension_object(name,code,type,create_time,update_time, creator,descr) "
            + "values(#{name},#{code},#{type},now(),now(), #{creator}, #{descr})")
    public int addDimensionObject(DimensionObject dimensionObject);

    @Update("update dimension_object set name=#{name},code=#{code},type=#{type},descr=#{descr} where id=#{id}")
    public int updateDimensionObject(DimensionObject dimensionObject);

    @Select({"<script> select DISTINCT o.`name` from dimension d left join dimension_object o on d.dimension_object_id=o.id where d.deleted=0 and o.id in " +
            "<foreach collection ='ids' item ='items' index ='index' separator=',' open='(' close=')'  > " +
            "#{items} " +
            "</foreach> </script>"})
    public List<String> findDimensionObjectName(int[] ids);

    @Update({"<script>update dimension_object set deleted=null where id in " +
            "<foreach collection ='ids' item ='items' index ='index' separator=',' open='(' close=')'  > " +
            "#{items} " +
            "</foreach> </script>"})
    public int deleteDimensionObject(int[] ids);

    @Select("select * from dimension_object where ${condition} limit #{startLine},#{size}")
    public List<DimensionObject> queryDimensionObject(String condition, int startLine, int size);

    @Select("select count(*) from dimension_object where ${condition}")
    public int queryDimensionObjectTotal(String condition);

    @Select("select * from dimension where deleted=0 and name=#{name}")
    public Dimension queryDimensionByName(String name);

    @Select("select * from dimension where deleted=0 and dimension_object_id=#{dimension_object_id} and code=#{code}")
    public Dimension queryDimensionByCode(int dimension_object_id,String code);

    @Select("select * from dimension where deleted=0 and id=#{id}")
    public Dimension queryDimensionById(int id);

    @Insert("insert into dimension(dimension_object_id,name,value_type,code,create_time,update_time, creator,descr) "
            + "values(#{dimension_object_id},#{name},#{value_type},#{code},now(),now(), #{creator}, #{descr})")
    public int addDimension(Dimension dimension);

    @Update("update dimension set name=#{name},code=#{code},descr=#{descr},value_type=#{value_type} where id=#{id}")
    public int updateDimension(Dimension dimension);

    @Update({"<script>update dimension set deleted=null where id in " +
            "<foreach collection ='ids' item ='items' index ='index' separator=',' open='(' close=')'  > " +
            "#{items} " +
            "</foreach> </script>"})
    public int deleteDimension(int[] ids);

    @Select({"<script> select `name` from adjective where deleted=0 and dimension_id in " +
            "<foreach collection ='ids' item ='items' index ='index' separator=',' open='(' close=')'  > " +
            "#{items} " +
            "</foreach> </script>"})
    public List<String> queryAdjectiveNameyDimensionIds(int[] ids);

    @Select({"<script> select q.`name` from (select substring_index(substring_index(a.dimension,',',b.help_topic_id+1),',',-1) as id,a.name from  quoto a join mysql.help_topic b on b.help_topic_id" +
            " &lt; (length(a.dimension) - length(replace(a.dimension,',',''))+1) where a.deleted=0) q where id in " +
            "<foreach collection ='ids' item ='items' index ='index' separator=',' open='(' close=')'  > " +
            "#{items} " +
            "</foreach> </script>"})
    public List<String> queryQuotoNamesByDimensionIds(int[] ids);

    @Select("select * from dimension where ${condition} limit #{startLine},#{size}")
    public List<Dimension> queryDimension(String condition, int startLine, int size);

    @Select("select count(*) from dimension where ${condition}")
    public int queryDimensionTotal(String condition);

    @Select("select * from dimension_object where ${condition}")
    public List<DimensionObject> queryAllDimensionObject(String condition);

    @Select("select * from Column_alias where is_delete=0 and table_id=${tableId} and data_type like 'Date%'")
    public List<ColumnAlias> queryTimeColumnByTableId(int tableId);

    @Select("select * from Column_alias where is_delete=0 and table_id=${tableId} and id=${id} and data_type like 'Date%'")
    public ColumnAlias queryTimeColumnById(int tableId,int id);

    @Select({
            "<script> select d.* from dimension d left join dimension_object o on d.dimension_object_id=o.id where d.deleted=0 and o.code='time' and d.id in <foreach collection='array' item='ids' index='no' open='(' separator=',' close=')'> #{ids} </foreach></script>" })
    public List<Dimension> queryTimeDimensionByIds(int[] ids);
}
