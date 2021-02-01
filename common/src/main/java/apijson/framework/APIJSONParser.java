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

import static apijson.framework.APIJSONConstant.DEFAULTS;
import static apijson.framework.APIJSONConstant.FORMAT;
import static apijson.framework.APIJSONConstant.VERSION;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpSession;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import apijson.NotNull;
import apijson.RequestMethod;
import apijson.entity.ColumnAlias;
import apijson.entity.CommonResponse;
import apijson.entity.QuotoInfo;
import apijson.entity.TableInfo;
import apijson.orm.AbstractParser;
import apijson.orm.AbstractSQLConfig;
import apijson.orm.FunctionParser;
import apijson.orm.Parser;
import apijson.orm.SQLConfig;
import apijson.orm.SQLExecutor;
import apijson.orm.Verifier;
import apijson.orm.model.Column;
import apijson.orm.model.ExtendedProperty;
import apijson.orm.model.PgAttribute;
import apijson.orm.model.PgClass;
import apijson.orm.model.SysColumn;
import apijson.orm.model.SysTable;
import apijson.orm.model.Table;

/**
 * 请求解析器
 * 
 * @author Lemon
 */
public class APIJSONParser extends AbstractParser<Long> {
	public static final String TAG = "APIJSONParser";

	@NotNull
	public static APIJSONCreator APIJSON_CREATOR;
	static {
		APIJSON_CREATOR = new APIJSONCreator();
	}

	public APIJSONParser() {
		super();
	}

	public APIJSONParser(RequestMethod method) {
		super(method);
	}

	public APIJSONParser(RequestMethod method, boolean needVerify) {
		super(method, needVerify);
	}

	private HttpSession session;

	public HttpSession getSession() {
		return session;
	}

	public APIJSONParser setSession(HttpSession session) {
		this.session = session;
		setVisitor(APIJSONVerifier.getVisitor(session));
		return this;
	}

	@Override
	public Parser<Long> createParser() {
		return APIJSON_CREATOR.createParser();
	}

	@Override
	public FunctionParser createFunctionParser() {
		return APIJSON_CREATOR.createFunctionParser();
	}

	@Override
	public Verifier<Long> createVerifier() {
		return APIJSON_CREATOR.createVerifier();
	}

	@Override
	public SQLConfig createSQLConfig() {
		return APIJSON_CREATOR.createSQLConfig();
	}

	@Override
	public SQLExecutor createSQLExecutor() {
		return APIJSON_CREATOR.createSQLExecutor();
	}

	@Override
	public JSONObject parseResponse(JSONObject request) {
		// 补充format
		if (session != null && request != null) {
			if (request.get(FORMAT) == null) {
				request.put(FORMAT, session.getAttribute(FORMAT));
			}
			if (request.get(DEFAULTS) == null) {
				JSONObject defaults = (JSONObject) session.getAttribute(DEFAULTS);
				Set<Map.Entry<String, Object>> set = defaults == null ? null : defaults.entrySet();

				if (set != null) {
					for (Map.Entry<String, Object> e : set) {
						if (e != null && request.get(e.getKey()) == null) {
							request.put(e.getKey(), e.getValue());
						}
					}
				}
			}
		}
		return super.parseResponse(request);
	}

	private FunctionParser functionParser;

	public FunctionParser getFunctionParser() {
		return functionParser;
	}

	@Override
	public Object onFunctionParse(String key, String function, String parentPath, String currentName,
			JSONObject currentObject) throws Exception {
		if (functionParser == null) {
			functionParser = createFunctionParser();
			functionParser.setMethod(getMethod());
			functionParser.setTag(getTag());
			functionParser.setVersion(getVersion());
			functionParser.setRequest(requestObject);

			if (functionParser instanceof APIJSONFunctionParser) {
				((APIJSONFunctionParser) functionParser).setSession(getSession());
			}
		}
		functionParser.setKey(key);
		functionParser.setParentPath(parentPath);
		functionParser.setCurrentName(currentName);
		functionParser.setCurrentObject(currentObject);

		return functionParser.invoke(function, currentObject);
	}

	@Override
	public APIJSONObjectParser createObjectParser(JSONObject request, String parentPath, String name,
			SQLConfig arrayConfig, boolean isSubquery) throws Exception {

		return new APIJSONObjectParser(getSession(), request, parentPath, name, arrayConfig, isSubquery) {

			// @Override
			// protected APIJSONSQLConfig newQueryConfig() {
			// if (itemConfig != null) {
			// return itemConfig;
			// }
			// return super.newQueryConfig();
			// }

			// 导致最多评论的(Strong 30个)的那个动态详情界面Android(82001)无姓名和头像，即User=null
			// @Override
			// protected void onComplete() {
			// if (response != null) {
			// putQueryResult(path, response);//解决获取关联数据时requestObject里不存在需要的关联数据
			// }
			// }

		}.setMethod(getMethod()).setParser(this);
	}

	@Override
	public void onVerifyContent() throws Exception {
		// 补充全局缺省版本号 //可能在默认为1的前提下这个请求version就需要为0 requestObject.getIntValue(VERSION) <=
		// 0) {
		HttpSession session = getSession();
		if (session != null && requestObject.get(VERSION) == null) {
			requestObject.put(VERSION, session.getAttribute(VERSION));
		}
		super.onVerifyContent();
	}

	// //可重写来设置最大查询数量
	// @Override
	// public int getMaxQueryCount() {
	// return 50;
	// }
	public CommonResponse loadAliasConfig() {
		CommonResponse commonResponse=new CommonResponse();
		// 查询配置的数据库信息
		SQLConfig sqlConfig = APIJSONApplication.DEFAULT_APIJSON_CREATOR.createSQLConfig();
		String request = "{\"@database\":\"DATASERVICE\",\"@schema\":\"bigdata_dataservice\",\"Database_info\": {\"@column\":\"id\",\"is_delete\":0,\"state\":1,\"db_url\":\""
				+ sqlConfig.getDBUri() + "\",\"database\":\"" + sqlConfig.getDefaultSchema() + "\"}}";
		setNeedVerify(false);
		JSONObject object = parseResponse(request);
		if (object.get("Database_info") == null) {
			commonResponse.setSuccess(false);
			commonResponse.setMessage("数据源不存在，请稍后再试");
			return commonResponse;
		}

		// 根据数据库id查询表信息
		int dataBaseId = (int) JSONObject.parseObject(object.get("Database_info").toString()).get("id");
		request = "{\"@database\":\"DATASERVICE\",\"@schema\":\"bigdata_dataservice\",\"[]\":{\"Table_info\": {\"is_delete\":0,\"state\":1,\"database_id\":"
				+ dataBaseId + "},\"count\":0}}";
		object = parseResponse(request);
		AbstractSQLConfig.TABLE_KEY_MAP.clear();
		AbstractSQLConfig.tableColumnMap.clear();
		if (object.get("[]") == null) {
			return commonResponse;
		}
		AbstractSQLConfig.TABLE_KEY_MAP.put(Table.class.getSimpleName(), Table.TABLE_NAME);
		AbstractSQLConfig.TABLE_KEY_MAP.put(Column.class.getSimpleName(), Column.TABLE_NAME);
		AbstractSQLConfig.TABLE_KEY_MAP.put(PgClass.class.getSimpleName(), PgClass.TABLE_NAME);
		AbstractSQLConfig.TABLE_KEY_MAP.put(PgAttribute.class.getSimpleName(), PgAttribute.TABLE_NAME);
		AbstractSQLConfig.TABLE_KEY_MAP.put(SysTable.class.getSimpleName(), SysTable.TABLE_NAME);
		AbstractSQLConfig.TABLE_KEY_MAP.put(SysColumn.class.getSimpleName(), SysColumn.TABLE_NAME);
		AbstractSQLConfig.TABLE_KEY_MAP.put(ExtendedProperty.class.getSimpleName(), ExtendedProperty.TABLE_NAME);
		List<JSONObject> tableInfos = JSONArray.parseArray(object.get("[]").toString(), JSONObject.class);
		for (int i = 0; i < tableInfos.size(); i++) {
			TableInfo tableInfo = JSONObject.parseObject(tableInfos.get(i).getString("Table_info"), TableInfo.class);
			// 根据表信息查询表别名信息
			if (tableInfo.getTable_alias() != null && (!tableInfo.getTable_alias().equals(""))) {
				AbstractSQLConfig.TABLE_KEY_MAP.put(tableInfo.getTable_alias(), tableInfo.getTable_name());
			}
			// 根据表信息查询列别名信息
			Map<String, String> valueAlias = new HashMap<String, String>();
			Map<String, String> valueReal = new HashMap<String, String>();
			Map<String, String> value = new HashMap<String, String>();
			request = "{\"@database\":\"DATASERVICE\",\"@schema\":\"bigdata_dataservice\",\"[]\":{\"Column_alias\": {\"is_delete\":0,\"state\":1,\"table_id\":"
					+ tableInfo.getId() + "},\"count\":0}}";
			object = parseResponse(request);
			if (object.get("[]") != null) {
				List<JSONObject> columnAliass = JSONArray.parseArray(object.get("[]").toString(), JSONObject.class);
				for (int j = 0; j < columnAliass.size(); j++) {
					ColumnAlias columnAlias = JSONObject.parseObject(columnAliass.get(j).getString("Column_alias"),
							ColumnAlias.class);
					value.put("\"" + columnAlias.getColumn_alias() + "\"", "\"" + columnAlias.getColumn_name() + "\"");
					valueAlias.put("\"" + columnAlias.getColumn_alias() + "\"", "\"" + columnAlias.getColumn_name() + "\"");
					valueReal.put(columnAlias.getColumn_alias(), columnAlias.getColumn_name());
				}
				AbstractSQLConfig.tableColumnMap.put(tableInfo.getTable_name() + "_processColumn", value);
				AbstractSQLConfig.tableColumnMap.put(tableInfo.getTable_name() + "_realColumn", valueReal);
			}

			// 根据表信息查询指标信息
			request = "{\"@database\":\"DATASERVICE\",\"@schema\":\"bigdata_dataservice\",\"[]\":{\"Quoto_info\": {\"is_delete\":0,\"state\":1,\"table_id\":"
					+ tableInfo.getId() + "},\"count\":0}}";
			object = parseResponse(request);
			if (object.get("[]") != null) {
				List<JSONObject> quotoInfos = JSONArray.parseArray(object.get("[]").toString(), JSONObject.class);
				for (int j = 0; j < quotoInfos.size(); j++) {
					QuotoInfo quotoInfo = JSONObject.parseObject(quotoInfos.get(j).getString("Quoto_info"),
							QuotoInfo.class);
					valueAlias.put("\"" + quotoInfo.getQuoto_name() + "\"", quotoInfo.getQuoto_sql());
				}
				AbstractSQLConfig.tableColumnMap.put(tableInfo.getTable_name() + "_aliasColumn", valueAlias);
			}
		}
		return commonResponse;
	}

}
