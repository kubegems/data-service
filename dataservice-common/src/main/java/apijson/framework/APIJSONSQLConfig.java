/*Copyright ©2016 TommyLemon(https://github.com/TommyLemon/APIJSON)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package apijson.framework;

import static apijson.framework.APIJSONConstant.ID;
import static apijson.framework.APIJSONConstant.PRIVACY_;
import static apijson.framework.APIJSONConstant.USER_;
import static apijson.framework.APIJSONConstant.USER_ID;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import apijson.entity.DbInfo;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;

import apijson.RequestMethod;
import apijson.orm.AbstractSQLConfig;
import apijson.orm.Join;
import apijson.orm.SQLConfig;


/**SQL配置
 * TiDB 用法和 MySQL 一致
 * @author Lemon
 */
public class APIJSONSQLConfig extends AbstractSQLConfig {
	public static final String TAG = "APIJSONSQLConfig";
	public static Callback SIMPLE_CALLBACK;
	public static APIJSONCreator APIJSON_CREATOR;
	static {
		DEFAULT_DATABASE = DATABASE_MYSQL;  //TODO 默认数据库类型，改成你自己的
		DEFAULT_SCHEMA = "sys";  //TODO 默认模式名，改成你自己的，默认情况是 MySQL: sys, PostgreSQL: public, SQL Server: dbo, Oracle: 
		//		TABLE_KEY_MAP.put(Access.class.getSimpleName(), "apijson_access");

		//  由 APIJSONVerifier.init 方法读取数据库 Access 表来替代手动输入配置
		//		//表名映射，隐藏真实表名，对安全要求很高的表可以这么做
		//		TABLE_KEY_MAP.put(User.class.getSimpleName(), "apijson_user");
		//		TABLE_KEY_MAP.put(Privacy.class.getSimpleName(), "apijson_privacy");

		APIJSON_CREATOR = new APIJSONCreator();

		SIMPLE_CALLBACK = new SimpleCallback() {

			@Override
			public SQLConfig getSQLConfig(RequestMethod method, String database, String schema, String table) {
				SQLConfig config = APIJSON_CREATOR.createSQLConfig();
				config.setMethod(method);
				config.setTable(table);
				return config;
			}


			@Override
			public String getUserIdKey(String database, String schema, String table) {
				return USER_.equals(table) || PRIVACY_.equals(table) ? ID : USER_ID; // id / userId
			}

			//取消注释来实现数据库自增 id
			//			@Override
			//			public Object newId(RequestMethod method, String database, String schema, String table) {
			//				return null; // return null 则不生成 id，一般用于数据库自增 id
			//			}
		};

	}


	@Override
	public String getDBVersion() {
		DbInfo dbInfo =dbInfoMap.get(getSQLDatabase());
		if(dbInfo == null){
			throw new IllegalArgumentException("没有名字为:"+getSQLDatabase()+" 的数据服务！");
		}
		String dbType = dbInfo.getDb_name();
		if (DATABASE_MYSQL.equals(dbType)) {
			return "5.7.24"; //"8.0.11"; //TODO 改成你自己的 MySQL 或 PostgreSQL 数据库版本号 //MYSQL 8 和 7 使用的 JDBC 配置不一样
		}
		if (DATABASE_POSTGRESQL.equals(dbType)) {
			return "9.6.15"; //TODO 改成你自己的
		}
		if (DATABASE_SQLSERVER.equals(dbType)) {
			return "2016"; //TODO 改成你自己的
		}
		if (DATABASE_ORACLE.equals(dbType)) {
			return "18c"; //TODO 改成你自己的
		}
		if (DATABASE_CLICKHOUSE.equals(dbType)) {
			return "5.7.24"; //TODO 改成你自己的
		}
		if (DATABASE_DATASERVICE.equals(dbType)) {
			return "5.7.24"; //TODO 改成你自己的
		}
		if (DATABASE_KYLIN.equals(dbType)) {
			return "5.7.24"; //TODO 改成你自己的
		}
		return null;
	}
	@JSONField(serialize = false)  // 不在日志打印 账号/密码 等敏感信息，用了 UnitAuto 则一定要加
	@Override
	public String getDBUri() {
		DbInfo dbInfo =dbInfoMap.get(getSQLDatabase());
		if(dbInfo == null){
			throw new IllegalArgumentException("没有名字为:"+getSQLDatabase()+" 的数据服务！");
		}
		return dbInfo.getDb_url();
	}

	@JSONField(serialize = false)  // 不在日志打印 账号/密码 等敏感信息，用了 UnitAuto 则一定要加
	@Override
	public String getDBAccount() {
		DbInfo dbInfo =dbInfoMap.get(getSQLDatabase());
		if(dbInfo == null){
			throw new IllegalArgumentException("没有名字为:"+getSQLDatabase()+" 的数据服务！");
		}
		return dbInfo.getUserName();
	}

	@JSONField(serialize = false)  // 不在日志打印 账号/密码 等敏感信息
	@Override
	public String getDBPassword() {
		DbInfo dbInfo =dbInfoMap.get(getSQLDatabase());
		if(dbInfo == null){
			throw new IllegalArgumentException("没有名字为:"+getSQLDatabase()+" 的数据服务！");
		}
		return dbInfo.getPassword();
	}

	/**获取 APIJSON 配置表所在数据库模式 database，默认与业务表一块
	 * @return
	 */
	public String getConfigDatabase() {
		return getDatabase();
	}
	/**获取 APIJSON 配置表所在数据库模式 schema，默认与业务表一块
	 * @return
	 */
	public String getConfigSchema() {
		return getSchema();
	}
	/**是否为 APIJSON 配置表，如果和业务表一块，可以重写这个方法，固定 return false 来提高性能
	 * @return
	 */
	public boolean isConfigTable() {
		return CONFIG_TABLE_LIST.contains(getTable());
	}
	@Override
	public String getSQLDatabase() {
		String db = isConfigTable() ? getConfigDatabase() : super.getSQLDatabase();
		return db == null ? DEFAULT_DATABASE : db;
	}
	@Override
	public String getSQLSchema() {
		String sch = isConfigTable() ? getConfigSchema() : super.getSQLSchema();
		return sch == null ? DEFAULT_SCHEMA : sch;
	}


	@Override
	public String getIdKey() {
		return SIMPLE_CALLBACK.getIdKey(getDatabase(), getSchema(), getTable());
	}

	@Override
	public String getUserIdKey() {
		return SIMPLE_CALLBACK.getUserIdKey(getDatabase(), getSchema(), getTable());
	}


	public APIJSONSQLConfig() {
		this(RequestMethod.GET);
	}
	public APIJSONSQLConfig(RequestMethod method) {
		super(method);
	}
	public APIJSONSQLConfig(RequestMethod method, String table) {
		super(method, table);
	}
	public APIJSONSQLConfig(RequestMethod method, int count, int page) {
		super(method, count, page);
	}



	/**获取SQL配置
	 * @param table
	 * @param alias 
	 * @param request
	 * @param isProcedure 
	 * @return
	 * @throws Exception 
	 */
	public static SQLConfig newSQLConfig(RequestMethod method, String table, String alias, JSONObject request, List<Join> joinList, boolean isProcedure) throws Exception {
		return newSQLConfig(method, table, alias, request, joinList, isProcedure, SIMPLE_CALLBACK);
	}

	@Override
	public String getDefaultSchema() {
		// TODO Auto-generated method stub
		return DEFAULT_SCHEMA;
	}


}
