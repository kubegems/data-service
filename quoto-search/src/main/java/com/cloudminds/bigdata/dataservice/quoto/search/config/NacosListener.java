package com.cloudminds.bigdata.dataservice.quoto.search.config;

import apijson.framework.APIJSONApplication;
import apijson.framework.APIJSONCreator;
import apijson.orm.SQLConfig;
import com.cloudminds.bigdata.dataservice.quoto.search.QuotoSearchApplication;
import com.purgeteam.dynamic.config.starter.event.ActionConfigEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.rmi.ServerException;
import java.util.HashMap;
import java.util.Map;

@Component
public class NacosListener implements ApplicationListener<ActionConfigEvent> {

	@Override
	public void onApplicationEvent(ActionConfigEvent event) {
		// TODO Auto-generated method stub
		boolean dbChange=false;
		Map<String, HashMap> change=event.getPropertyMap();
		for (String key : change.keySet()) {
			if(QuotoSearchApplication.dbInfo.containsKey(key)) {
				QuotoSearchApplication.dbInfo.put(key, change.get(key).get("after").toString());
				dbChange=true;
			}
			
		}
	    if(dbChange) {
	    	APIJSONApplication.DEFAULT_APIJSON_CREATOR = new APIJSONCreator() {
				@Override
				public SQLConfig createSQLConfig() {
					return new DBSQLConfig(QuotoSearchApplication.dbInfo.get("dbUrl"), QuotoSearchApplication.dbInfo.get("dbAccount"),QuotoSearchApplication.dbInfo.get("dbPassword"), QuotoSearchApplication.dbInfo.get("configDbUrl"),QuotoSearchApplication.dbInfo.get("configDbAccount"), QuotoSearchApplication.dbInfo.get("configDbPassword"));
				}
			};
			try {
				APIJSONApplication.init(false);
			} catch (ServerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  // 4.4.0 以上需要这句来保证以上 static 代码块中给 DEFAULT_APIJSON_CREATOR 赋值会生效
	    }
	}

}
