package com.cloudminds.bigdata.dataservice.quoto.manage.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Quoto;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.QuotoQuery;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.service.QuotoService;

@RestController
@RequestMapping("/quotoManage/quoto")
public class QuotoControl {
	@Autowired
	private QuotoService quotoService;

	// 检查是否唯一
	@RequestMapping(value = "checkUnique", method = RequestMethod.POST)
	public CommonResponse checkUnique(@RequestBody CheckReq checkReq) {
		return quotoService.checkUnique(checkReq);
	}

	// 获取所有的业务线
	@RequestMapping(value = "queryAllBusiness", method = RequestMethod.GET)
	public CommonResponse queryAllBusiness() {
		return quotoService.queryAllBusiness();
	}

	// 获取所有的数据域
	@RequestMapping(value = "queryAllDataDomain", method = RequestMethod.GET)
	public CommonResponse queryAllDataDomain(int businessId) {
		return quotoService.queryAllDataDomain(businessId);
	}

	// 获取所有的业务过程
	@RequestMapping(value = "queryAllBusinessProcess", method = RequestMethod.GET)
	public CommonResponse queryAllBusinessProcess(int dataDomainId) {
		return quotoService.queryAllBusinessProcess(dataDomainId);
	}

	// 获取所有的数据服务
	@RequestMapping(value = "queryAllDataService", method = RequestMethod.GET)
	public CommonResponse queryAllDataService() {
		return quotoService.queryAllDataService();
	}

	// 获取所有的计算周期
	@RequestMapping(value = "queryAllCycle", method = RequestMethod.GET)
	public CommonResponse queryAllCycle() {
		return quotoService.queryAllCycle();
	}

	// 根据id查询指标信息
	@RequestMapping(value = "queryQuotoById", method = RequestMethod.GET)
	public CommonResponse queryQuotoById(int id) {
		return quotoService.queryQuotoById(id);
	}

	// 获取所有的维度属性
	@RequestMapping(value = "queryAllDimension", method = RequestMethod.GET)
	public CommonResponse queryAllDimension(int tableId) {
		return quotoService.queryAllDimension(tableId);
	}

	// 删除指标
	@RequestMapping(value = "delete", method = RequestMethod.POST)
	public CommonResponse deleteQuoto(@RequestBody DeleteReq deleteReq) {
		return quotoService.deleteQuoto(deleteReq);
	}

	// 批量删除指标
	@RequestMapping(value = "batchDelete", method = RequestMethod.POST)
	public CommonResponse batchDeleteQuoto(@RequestBody BatchDeleteReq batchDeleteReq) {
		return quotoService.batchDeleteQuoto(batchDeleteReq);
	}

	// 创建指标
	@RequestMapping(value = "add", method = RequestMethod.POST)
	public CommonResponse insertQuoto(@RequestBody Quoto quoto) {
		return quotoService.insertQuoto(quoto);
	}

	// 查询指标
	@RequestMapping(value = "query", method = RequestMethod.POST)
	public CommonQueryResponse queryQuoto(@RequestBody QuotoQuery quotoQuery) {
		return quotoService.queryQuoto(quotoQuery);
	}

	// 编辑指标
	@RequestMapping(value = "update", method = RequestMethod.POST)
	public CommonResponse updateQuoto(@RequestBody Quoto quoto) {
		return quotoService.updateQuoto(quoto);
	}

	// 激活原子指标
	@RequestMapping(value = "active", method = RequestMethod.POST)
	public CommonResponse activeQuoto(@RequestBody DeleteReq deleteReq) {
		return quotoService.activeQuoto(deleteReq.getId());
	}

	// 模糊匹配指标
	@RequestMapping(value = "queryFuzzy", method = RequestMethod.POST)
	public CommonResponse queryFuzzy(@RequestBody QuotoQuery quotoQuery) {
		return quotoService.queryFuzzy(quotoQuery);
	}

	// 获取指标数据
	@RequestMapping(value = "queryQuotoData", method = RequestMethod.GET)
	public CommonResponse queryQuotoData(int id, String quotoName) {
		return quotoService.queryQuotoData(id, quotoName);
	}
}
