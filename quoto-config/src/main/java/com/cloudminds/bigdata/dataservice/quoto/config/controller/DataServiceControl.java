package com.cloudminds.bigdata.dataservice.quoto.config.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.ColumnAlias;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.DatabaseInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.QuotoInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.entity.TableInfo;
import com.cloudminds.bigdata.dataservice.quoto.config.service.DataServiceConfig;



@RestController
@RequestMapping("/dataservice/config")
public class DataServiceControl {
	@Autowired
	private DataServiceConfig dataServiceConfig;

	// columnAlias
	@RequestMapping(value = "getColumnAlias", method = RequestMethod.GET)
	public CommonResponse getColumnAlias(int tableId) {
		return dataServiceConfig.getColumnAlias(tableId);
	}

	@RequestMapping(value = "updateColumnAliasStatus", method = RequestMethod.POST)
	public CommonResponse updateColumnAliasStatus(int id, int status) {
		return dataServiceConfig.updateColumnAliasStatus(id, status);
	}

	@RequestMapping(value = "deleteColumnAlias", method = RequestMethod.POST)
	public CommonResponse deleteColumnAlias(int id) {
		return dataServiceConfig.deleteColumnAlias(id);
	}

	@RequestMapping(value = "insertColumnAlias", method = RequestMethod.POST)
	public CommonResponse insertColumnAlias(@RequestBody ColumnAlias columnAlias) {
		return dataServiceConfig.insertColumnAlias(columnAlias);
	}
	
	@RequestMapping(value = "updateColumnAlias", method = RequestMethod.POST)
	public CommonResponse updateColumnAlias(@RequestBody ColumnAlias columnAlias) {
		return dataServiceConfig.updateColumnAlias(columnAlias);
	}

	// database
	@RequestMapping(value = "getDataBase", method = RequestMethod.GET)
	public CommonResponse getDataBase() {
		return dataServiceConfig.getDataBase();
	}

	@RequestMapping(value = "updateDatabaseInfoStatus", method = RequestMethod.POST)
	public CommonResponse updateDatabaseInfoStatus(int id, int status) {
		return dataServiceConfig.updateDatabaseInfoStatus(id, status);
	}

	@RequestMapping(value = "deleteDatabaseInfo", method = RequestMethod.POST)
	public CommonResponse deleteDatabaseInfo(int id) {
		return dataServiceConfig.deleteDatabaseInfo(id);
	}

	@RequestMapping(value = "insertDatabaseInfo", method = RequestMethod.POST)
	public CommonResponse insertDatabaseInfo(@RequestBody DatabaseInfo databaseInfo) {
		return dataServiceConfig.insertDatabaseInfo(databaseInfo);
	}

	// quotoInfo
	@RequestMapping(value = "getQuotoInfo", method = RequestMethod.GET)
	public CommonResponse getQuotoInfo(int tableId) {
		return dataServiceConfig.getQuotoInfo(tableId);
	}

	@RequestMapping(value = "updateQuotoInfoStatus", method = RequestMethod.POST)
	public CommonResponse updateQuotoInfoStatus(int id, int status) {
		return dataServiceConfig.updateQuotoInfoStatus(id, status);
	}

	@RequestMapping(value = "deleteQuotoInfo", method = RequestMethod.POST)
	public CommonResponse deleteQuotoInfo(int id) {
		return dataServiceConfig.deleteQuotoInfo(id);
	}

	@RequestMapping(value = "insertQuotoInfo", method = RequestMethod.POST)
	public CommonResponse insertQuotoInfo(@RequestBody QuotoInfo quotoInfo) {
		return dataServiceConfig.insertQuotoInfo(quotoInfo);
	}
	
	@RequestMapping(value = "updateQuotoInfo", method = RequestMethod.POST)
	public CommonResponse updateQuotoInfo(@RequestBody QuotoInfo quotoInfo) {
		return dataServiceConfig.updateQuotoInfo(quotoInfo);
	}

	// tableInfo
	@RequestMapping(value = "getTableInfo", method = RequestMethod.GET)
	public CommonResponse getTableInfo(int databaseId) {
		return dataServiceConfig.getTableInfo(databaseId);
	}

	@RequestMapping(value = "updateTableInfoStatus", method = RequestMethod.POST)
	public CommonResponse updateTableInfoStatus(int id, int status) {
		return dataServiceConfig.updateTableInfoStatus(id, status);
	}

	@RequestMapping(value = "deleteTableInfo", method = RequestMethod.POST)
	public CommonResponse deleteTableInfo(int id) {
		return dataServiceConfig.deleteTableInfo(id);
	}

	@RequestMapping(value = "insertTableInfo", method = RequestMethod.POST)
	public CommonResponse insertTableInfo(@RequestBody TableInfo tableInfo) {
		return dataServiceConfig.insertTableInfo(tableInfo);
	}
	
	@RequestMapping(value = "updateTableInfo", method = RequestMethod.POST)
	public CommonResponse updateTableInfo(@RequestBody TableInfo tableInfo) {
		return dataServiceConfig.updateTableInfo(tableInfo);
	}
}
