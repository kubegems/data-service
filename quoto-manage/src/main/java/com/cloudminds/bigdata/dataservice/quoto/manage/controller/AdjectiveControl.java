package com.cloudminds.bigdata.dataservice.quoto.manage.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Adjective;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.AdjectiveQuery;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.service.AdjectiveService;

@RestController
@RequestMapping("/quotoManage/adjective")
public class AdjectiveControl {
	@Autowired
	private AdjectiveService adjectiveService;

	// 根据修饰词的所有类型
	@RequestMapping(value = "queryAllType", method = RequestMethod.GET)
	public CommonResponse queryAllType() {
		return adjectiveService.queryAllType();
	}

	// 删除修饰词
	@RequestMapping(value = "delete", method = RequestMethod.POST)
	public CommonResponse deleteAdjective(@RequestBody DeleteReq deleteReq) {
		return adjectiveService.deleteAdjective(deleteReq);
	}

	// 批量删除修饰词
	@RequestMapping(value = "batchDelete", method = RequestMethod.POST)
	public CommonResponse batchDeleteAdjective(@RequestBody BatchDeleteReq batchDeleteReq) {
		return adjectiveService.batchDeleteAdjective(batchDeleteReq);
	}

	// 检查是否唯一
	@RequestMapping(value = "checkUnique", method = RequestMethod.POST)
	public CommonResponse checkUnique(@RequestBody CheckReq checkReq) {
		return adjectiveService.checkUnique(checkReq);
	}

	// 创建修饰词
	@RequestMapping(value = "add", method = RequestMethod.POST)
	public CommonResponse insertAdjective(@RequestBody Adjective adjective) {
		return adjectiveService.insertAdjective(adjective);
	}

	// 查询修饰词
	@RequestMapping(value = "query", method = RequestMethod.POST)
	public CommonQueryResponse queryAdjective(@RequestBody AdjectiveQuery adjectiveQuery) {
		return adjectiveService.queryAdjective(adjectiveQuery);
	}

	// 查询所有的修饰词
	@RequestMapping(value = "queryAll", method = RequestMethod.POST)
	public CommonResponse queryAllAdjective(@RequestBody AdjectiveQuery adjectiveQuery) {
		return adjectiveService.queryAllAdjective(adjectiveQuery);
	}

	// 查询支持的修饰词
	@RequestMapping(value = "querySupportAdjective", method = RequestMethod.GET)
	public CommonResponse querySupportAdjective(int tableId) {
		return adjectiveService.querySupportAdjective(tableId);
	}

	// 编辑修饰词
	@RequestMapping(value = "update", method = RequestMethod.POST)
	public CommonResponse updateAdjective(@RequestBody Adjective adjective) {
		return adjectiveService.updateAdjective(adjective);
	}
}
