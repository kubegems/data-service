package com.cloudminds.bigdata.dataservice.quoto.config.controller;

import com.cloudminds.bigdata.dataservice.quoto.config.entity.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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

	// dbInfo
	@RequestMapping(value = "getDbInfo", method = RequestMethod.GET)
	public CommonResponse getDbInfo() {
		return dataServiceConfig.getdbInfo();
	}

	@RequestMapping(value = "insertDbInfo", method = RequestMethod.POST)
	public CommonResponse insertDbInfo(@RequestBody DbInfo dbInfo) {
		return dataServiceConfig.insertDbInfo(dbInfo);
	}

	@RequestMapping(value = "updateDbInfo", method = RequestMethod.POST)
	public CommonResponse updateDbInfo(@RequestBody DbInfo dbInfo) {
		return dataServiceConfig.updateDbInfo(dbInfo);
	}

	@RequestMapping(value = "deleteDbInfo", method = RequestMethod.POST)
	public CommonResponse deleteDbInfo(@RequestBody DbInfo dbInfo) {
		return dataServiceConfig.deleteDbInfo(dbInfo);
	}

	@RequestMapping(value = "getDbInfoById", method = RequestMethod.GET)
	public CommonResponse getDbInfoById(int id) {
		return dataServiceConfig.getDbInfoById(id);
	}


	// database
	@RequestMapping(value = "getDataBase", method = RequestMethod.GET)
	public CommonResponse getDataBase() {
		return dataServiceConfig.getDataBase();
	}

	@RequestMapping(value = "getDataBaseBydbId", method = RequestMethod.GET)
	public CommonResponse getDataBase(int dbId) {
		return dataServiceConfig.getDataBaseBydbId(dbId);
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

	@RequestMapping(value = "getAllTableInfo", method = RequestMethod.GET)
	public CommonResponse getAllTableInfo(){
		return dataServiceConfig.getAllTableInfo();
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

	// quotoInfo
	@RequestMapping(value = "getApiDoc", method = RequestMethod.GET)
	public CommonResponse getApiDoc() {
		return dataServiceConfig.getApiDoc();
	}

	//新增用户token
	@RequestMapping(value="insertUserToken",method = RequestMethod.POST)
	public CommonResponse insertUserToken(@RequestBody UserToken userToken){
		return dataServiceConfig.insertUserToken(userToken);
	}

	//更新用户token
	@RequestMapping(value="updateUserToken",method = RequestMethod.POST)
	public CommonResponse updateUserToken(@RequestBody UserToken userToken){
		return dataServiceConfig.updateUserToken(userToken);
	}

	//禁用或启用用户token
	@RequestMapping(value="updateUserTokenStatus",method = RequestMethod.POST)
	public CommonResponse updateUserTokenStatus(int id, int status){
		return dataServiceConfig.updateUserTokenStatus(id,status);
	}

	//删除用户token
	@RequestMapping(value = "deleteUserToken", method = RequestMethod.POST)
	public CommonResponse deleteUserToken(int id) {
		return dataServiceConfig.deleteUserToken(id);
	}

	//查询用户token
	@RequestMapping(value = "getUserToken", method = RequestMethod.GET)
	public CommonResponse getUserToken() {
		return dataServiceConfig.getUserToken();
	}

	//查询用户token
	@RequestMapping(value = "getUserTokenByUserName", method = RequestMethod.GET)
	public CommonResponse getUserTokenByUserName(String userName) {
		return dataServiceConfig.getUserTokenByUserName(userName);
	}

	//刷新用户token
	@RequestMapping(value="refresh",method = RequestMethod.GET)
	public CommonResponse refreshUserToken(){
		return dataServiceConfig.refreshUserToken();
	}

	// 根据token获取表访问信息
	@RequestMapping(value = "getTableAccessInfo", method = RequestMethod.GET)
	public CommonResponse getTableAccessInfo(String token) {
		return dataServiceConfig.getTableAccessInfo(token);
	}

	// 查询数据源的信息详情
	@RequestMapping(value = "getSourceInfo", method = RequestMethod.GET)
	public CommonResponse getSourceInfo() {
		return dataServiceConfig.getSourceInfo();
	}

	// 查询各部门存储信息
	@RequestMapping(value = "getDepartmentSize", method = RequestMethod.GET)
	public CommonResponse getDepartmentSize() {
		return dataServiceConfig.getDepartmentSize();
	}
}
