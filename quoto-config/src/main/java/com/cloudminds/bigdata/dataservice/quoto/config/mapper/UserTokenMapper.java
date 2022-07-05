package com.cloudminds.bigdata.dataservice.quoto.config.mapper;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableAccessInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.UserToken;
import com.cloudminds.bigdata.dataservice.quoto.config.handler.ArrayTypeHandler;
import com.cloudminds.bigdata.dataservice.quoto.config.handler.ArrayVarcharHandler;
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

    @Select("SELECT * FROM user_token WHERE token=#{token} AND is_delete=0")
    @Result(column = "tables", property = "tables", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    public UserToken getUserTokenByToken(String token);

    @Select("select t.token,t.user_name,t.`tables`,t.des,t.creator,t.create_time,t.update_time,t.is_delete,t.state,GROUP_CONCAT(Table_info.table_name) as table_names from (SELECT a.token,a.`tables`,a.user_name,a.des,a.creator,a.create_time,a.update_time,a.is_delete,a.state,SUBSTRING_INDEX( SUBSTRING_INDEX( a.`tables`, ',', b.help_topic_id + 1 ), ',',- 1 ) AS table_id FROM user_token a JOIN mysql.help_topic AS b ON b.help_topic_id < ( length( a.`tables` ) - length( REPLACE ( a.`tables`, ',', '' ) ) + 1 ) where a.is_delete=0 and a.state=1 and a.user_name !='hive') t LEFT JOIN Table_info on t.table_id=Table_info.id LEFT JOIN Database_info ON Table_info.database_id=Database_info.id LEFT JOIN Db_info ON Database_info.db_id=Db_info.id  group BY token,`tables`,des,creator,create_time,update_time,is_delete,state,user_name")
    @Result(column = "tables", property = "tables", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayTypeHandler.class)
    @Result(column = "table_names", property = "table_names", jdbcType = JdbcType.VARCHAR, javaType = Array.class, typeHandler = ArrayVarcharHandler.class)
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

    @Select("select tt.id,tt.table_alias,tt.des as table_des,dd.des as db_des,concat(db.service_path,dd.service_path) as service_path from Table_info tt left join Database_info dd on tt.database_id=dd.id left JOIN Db_info db on dd.db_id=db.id where tt.id is not null and tt.is_delete=0 and tt.state=1 and dd.is_delete=0 and dd.state=1 and db.is_delete=0 and db.state=1")
    public List<TableAccessInfo> findAllTableAccessInfo();

    @Select("select tt.id,tt.table_alias,tt.des as table_des,dd.des as db_des,concat(db.service_path,dd.service_path) as service_path from (SELECT a.token, SUBSTRING_INDEX( SUBSTRING_INDEX( a.`tables`, ',', b.help_topic_id + 1 ), ',',- 1 ) AS table_id FROM user_token a JOIN mysql.help_topic AS b ON b.help_topic_id < ( length( a.`tables` ) - length( REPLACE ( a.`tables`, ',', '' ) ) + 1 ) where a.is_delete=0 and a.state=1 and a.token=#{token}) too left join Table_info tt on too.table_id=tt.id left join Database_info dd on tt.database_id=dd.id left JOIN Db_info db on dd.db_id=db.id where tt.id is not null and tt.is_delete=0 and tt.state=1 and dd.is_delete=0 and dd.state=1 and db.is_delete=0 and db.state=1")
    public List<TableAccessInfo> findTableAccessInfo(String token);
}
