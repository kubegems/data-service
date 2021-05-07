package schedule.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

import schedule.entity.AtlasEntitiesWithExtInfo;
import schedule.entity.AtlasEntity;
import schedule.entity.ColumnInfo;
import schedule.entity.DatabaseInfo;
import schedule.entity.TableInfo;
import schedule.mapper.ClickHouseMapper;

@Service
public class ScheduleService {
	@Autowired
	RestTemplate restTemplate;
	@Autowired
	private ClickHouseMapper clickHouseMapper;
	private String atlasUrl = "http://172.16.31.107:41001";
	private int atlasEntityNum = 20;

	public void clickhouseToAtlas() {
		List<DatabaseInfo> databaseInfos = clickHouseMapper.getDatabaseInfo();
		for (int i = 2; i<3; i++) {
			DatabaseInfo databaseInfo = databaseInfos.get(i);
			// 第一步插入database的数据
			AtlasEntitiesWithExtInfo dataBaseAtlasEntitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
			List<AtlasEntity> dataBaseEntities = new ArrayList<AtlasEntity>();
			AtlasEntity dataBaseAtlasEntity = new AtlasEntity();
			dataBaseAtlasEntity.setTypeName("ck_db");
			dataBaseAtlasEntity.setProvenanceType(0);
			dataBaseAtlasEntity.setVersion(0L);
			dataBaseAtlasEntity.setIsProxy(false);
			Map<String, Object> dataBaseAttributes = new HashMap<String, Object>();
			dataBaseAttributes.put("engine", databaseInfo.getEngine());
			dataBaseAttributes.put("data_path", databaseInfo.getData_path());
			dataBaseAttributes.put("metadata_path", databaseInfo.getMetadata_path());
			dataBaseAttributes.put("size", databaseInfo.getDiskSize());
			dataBaseAttributes.put("origin_size", databaseInfo.getOriginSize());
			dataBaseAttributes.put("name", databaseInfo.getName());
			dataBaseAttributes.put("owner", "clickhouse");
			dataBaseAttributes.put("qualifiedName", databaseInfo.getName());
			List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
			dataBaseAtlasEntity.setAttributes(dataBaseAttributes);
			dataBaseEntities.add(dataBaseAtlasEntity);
			dataBaseAtlasEntitiesWithExtInfo.setEntities(dataBaseEntities);
			// db发送给atlas入库
			String dataBaseGid = "";
			try {
				dataBaseGid = doPost(atlasUrl + "/api/atlas/v2/entity/bulk",
						JSON.toJSONString(dataBaseAtlasEntitiesWithExtInfo)).get(0);
			} catch (Exception e) {
				System.err.println("database i:" + i + "  程序异常终止");
				return;
			}
//			List<TableInfo> tableInfos = clickHouseMapper.getTableInfo(databaseInfos.get(i).getName());
//			for (int j = 0; j < tableInfos.size(); j++) {
//				TableInfo tableInfo = tableInfos.get(j);
//				AtlasEntitiesWithExtInfo tableAtlasEntitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
//				List<AtlasEntity> tableEntities = new ArrayList<AtlasEntity>();
//				AtlasEntity tableAtlasEntity = new AtlasEntity();
//				tableAtlasEntity.setTypeName("clickhouse_table");
//				tableAtlasEntity.setProvenanceType(0);
//				tableAtlasEntity.setVersion(0L);
//				tableAtlasEntity.setIsProxy(false);
//				Map<String, Object> tableAttributes = new HashMap<String, Object>();
//				tableAttributes.put("create_table_query", tableInfo.getCreate_table_query());
//				Map<String, Object> tableDb = new HashMap<>();
//				tableDb.put("guid", dataBaseGid);
//				tableDb.put("typeName", "clickhouse_db");
//				tableAttributes.put("db", tableDb);
//				tableAttributes.put("name", tableInfo.getName());
//				tableAttributes.put("owner", "clickhouse");
//				tableAttributes.put("qualifiedName", databaseInfo.getName() + "." + tableInfo.getName());
//				List<Map<String, Object>> colunms = new ArrayList<Map<String, Object>>();
//				List<String> colunmsGid = new ArrayList<>();
//				tableAtlasEntity.setAttributes(tableAttributes);
//				tableEntities.add(tableAtlasEntity);
//				tableAtlasEntitiesWithExtInfo.setEntities(tableEntities);
//				// table信息发送给atlas入库
//				String tableGid = "";
//				try {
//					tableGid = doPost(atlasUrl + "/api/atlas/v2/entity/bulk",
//							JSON.toJSONString(tableAtlasEntitiesWithExtInfo)).get(0);
//				} catch (Exception e) {
//					// TODO: handle exception
//					System.err.println("database i:" + i + "  程序异常终止");
//					return;
//				}
//				List<ColumnInfo> columnInfos = clickHouseMapper.getColumnInfo(databaseInfo.getName(),
//						tableInfo.getName());
//				List<AtlasEntity> colunmEntities = new ArrayList<AtlasEntity>();
//				for (int n = 0; n < columnInfos.size(); n++) {
//					ColumnInfo columnInfo = columnInfos.get(n);
//					AtlasEntity columnAtlasEntity = new AtlasEntity();
//					columnAtlasEntity.setTypeName("clickhouse_column");
//					columnAtlasEntity.setProvenanceType(0);
//					columnAtlasEntity.setVersion(0L);
//					columnAtlasEntity.setIsProxy(false);
//					Map<String, Object> columnAttributes = new HashMap<String, Object>();
//					columnAttributes.put("type", columnInfo.getType());
//					Map<String, Object> columnTable = new HashMap<>();
//					columnTable.put("guid", tableGid);
//					columnTable.put("typeName", "clickhouse_table");
//					columnAttributes.put("table", columnTable);
//					columnAttributes.put("comment", columnInfo.getComment());
//					columnAttributes.put("name", columnInfo.getName());
//					columnAttributes.put("owner", "clickhouse");
//					columnAttributes.put("qualifiedName",
//							databaseInfo.getName() + "." + tableInfo.getName() + "." + columnInfo.getName());
//					columnAtlasEntity.setAttributes(columnAttributes);
//					colunmEntities.add(columnAtlasEntity);
//					if (colunmEntities.size() >= atlasEntityNum) {
//						AtlasEntitiesWithExtInfo columnEntitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
//						columnEntitiesWithExtInfo.setEntities(colunmEntities);
//						// 调用接口
//						try {
//							colunmsGid.addAll(doPost(atlasUrl + "/api/atlas/v2/entity/bulk",
//									JSON.toJSONString(columnEntitiesWithExtInfo)));
//						} catch (Exception e) {
//							System.err.println("database i:" + i + "  程序异常终止");
//							return;
//						}
//						colunmEntities.clear();
//					}
//				}
//				if (colunmEntities.size() > 0) {
//					AtlasEntitiesWithExtInfo columnEntitiesWithExtInfo = new AtlasEntitiesWithExtInfo();
//					columnEntitiesWithExtInfo.setEntities(colunmEntities);
//					// 调用接口
//					try {
//						colunmsGid.addAll(doPost(atlasUrl + "/api/atlas/v2/entity/bulk",
//								JSON.toJSONString(columnEntitiesWithExtInfo)));
//					} catch (Exception e) {
//						System.err.println("database i:" + i + "  程序异常终止");
//						return;
//					}
//					colunmEntities.clear();
//				}
//				// 处理column的gid
//				// table的列
//
//			}
			// 更新table的属性
//			dataBaseAttributes.put("tables", tables);
//			dataBaseAtlasEntity.setAttributes(dataBaseAttributes);
//			dataBaseEntities.clear();
//			dataBaseEntities.add(dataBaseAtlasEntity);
//			dataBaseAtlasEntitiesWithExtInfo.setEntities(dataBaseEntities);
//			doPost(atlasUrl+"/api/atlas/v2/entity/bulk",JSON.toJSONString(dataBaseAtlasEntitiesWithExtInfo));
		}
	}

	public List<String> doPost(String url, String json) throws Exception {
		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
			HttpEntity<String> request = new HttpEntity<>(json, headers);

			ResponseEntity<String> postForEntity = restTemplate.postForEntity(url, request, String.class);
			if (postForEntity.getStatusCode().is2xxSuccessful()) {
				@SuppressWarnings("unchecked")
				Map<String, String> map = (Map) JSON
						.parse(JSONObject.parseObject(postForEntity.getBody()).get("guidAssignments").toString());
				List<String> gids = new ArrayList<String>();
				Set keys = map.keySet();
				if (keys != null) {
					Iterator iterator = keys.iterator();
					while (iterator.hasNext()) {
						Object key = iterator.next();
						gids.add(map.get(key));
					}
				}
				if (gids != null) {
					return gids;
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("发送到atlas入库异常,程序中断");
		}
		throw new Exception();
	}
}
