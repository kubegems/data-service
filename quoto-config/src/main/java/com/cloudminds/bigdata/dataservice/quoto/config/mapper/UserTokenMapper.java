package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.UserToken;
import com.cloudminds.bigdata.dataservice.quoto.config.handler.ArrayTypeHandler;
import org.apache.ibatis.annotations.*;
import org.apache.ibatis.type.JdbcType;

import java.sql.Array;
import java.util.List;

@Mapper
public interface UserTokenMapper {

    @Select("SELECT * FROM user_token WHERE user_name=#{userName} AND is_delete=0")
    @Result(column = "tables", property = "tables", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    public UserToken getUserTokenByUserName(String userName);

    @Select("SELECT * FROM user_token WHERE id=#{id} AND is_delete=0")
    @Result(column = "tables", property = "tables", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    public UserToken getUserTokenById(int id);

    @Select("SELECT * FROM user_token WHERE is_delete=0")
    @Result(column = "tables", property = "tables", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    public List<UserToken> getUserToken();

    @Insert("insert into user_token(user_name,token,tables,creator,des,create_time,update_time) VALUES(#{user_name},#{token},#{tables,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.ArrayTypeHandler},#{creator},#{des},now(),now())")
    @Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
    public int insertUserToken(UserToken userToken);

    @Update("update user_token set tables=#{tables,typeHandler=com.cloudminds.bigdata.dataservice.quoto.config.handler.ArrayTypeHandler},des=#{des},state=#{state},is_delete=#{is_delete} where id=#{id}")
    public int updateUserToken(UserToken userToken);

    @Select("select t.token,GROUP_CONCAT(CONCAT_WS('.',Database_info.service_path,Table_info.table_alias)) as des from (SELECT a.token," +
            " SUBSTRING_INDEX( SUBSTRING_INDEX( a.`tables`, ',', b.help_topic_id + 1 ), ',',- 1 ) AS table_id" +
            " FROM user_token a JOIN mysql.help_topic AS b ON b.help_topic_id < ( length( a.`tables` ) - length( REPLACE ( a.`tables`, ',', '' ) ) + 1 ) where a.is_delete=0 and a.state=1) t LEFT JOIN Table_info on t.table_id=Table_info.id LEFT JOIN Database_info ON Table_info.database_id=Database_info.id LEFT JOIN Db_info ON Database_info.db_id=Db_info.id where Db_info.db_name=#{db_name}" +
            " group BY token")
    public List<UserToken> findTokenTables(String db_name);

    @Select("select * from user_token where `tables`='0'")
    public List<UserToken> findSuperUserToken();
}
