package com.cloudminds.bigdata.dataservice.quoto.roc.config;

import java.rmi.ServerException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import com.cloudminds.bigdata.dataservice.quoto.roc.QuotoRocApplication;
import com.purgeteam.dynamic.config.starter.event.ActionConfigEvent;

import apijson.framework.APIJSONApplication;
import apijson.framework.APIJSONCreator;
import apijson.orm.SQLConfig;

@Component
public class NacosListener implements ApplicationListener<ActionConfigEvent> {

	@Override
	public void onApplicationEvent(ActionConfigEvent event) {
		// TODO Auto-generated method stub
		boolean dbChange=false;
		Map<String, HashMap> change=event.getPropertyMap();
		for (String key : change.keySet()) {
			if(QuotoRocApplication.dbInfo.containsKey(key)) {
				QuotoRocApplication.dbInfo.put(key, change.get(key).get("after").toString());
				dbChange=true;
			}
			
		}
	    if(dbChange) {
	    	APIJSONApplication.DEFAULT_APIJSON_CREATOR = new APIJSONCreator() {
				@Override
				public SQLConfig createSQLConfig() {
					return new DBSQLConfig(QuotoRocApplication.dbInfo.get("dbUrl"), QuotoRocApplication.dbInfo.get("dbAccount"),QuotoRocApplication.dbInfo.get("dbPassword"), QuotoRocApplication.dbInfo.get("configDbUrl"),QuotoRocApplication.dbInfo.get("configDbAccount"), QuotoRocApplication.dbInfo.get("configDbPassword"));
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
