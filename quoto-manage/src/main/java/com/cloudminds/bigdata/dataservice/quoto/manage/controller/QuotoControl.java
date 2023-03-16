package com.cloudminds.bigdata.dataservice.quoto.manage.controller;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Business;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Theme;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.BusinessProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.nacos.api.utils.StringUtils;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.Quoto;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.BatchDeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.CheckReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.DeleteReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.ExpressInfoReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.QuotoDataReq;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.request.QuotoQuery;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonQueryResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.DataCommonResponse;
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
	@RequestMapping(value = "queryBusiness", method = RequestMethod.GET)
	public CommonResponse queryBusiness(int pid) {
		return quotoService.queryAllBusiness(pid);
	}

	// 增加业务线
	@RequestMapping(value = "addBusiness", method = RequestMethod.POST)
	public CommonResponse addBusiness(@RequestBody Business business) {
		return quotoService.addBusiness(business);
	}

	// 增加业务线
	@RequestMapping(value = "updateBusiness", method = RequestMethod.POST)
	public CommonResponse updateBusiness(@RequestBody Business business) {
		return quotoService.updateBusiness(business);
	}

	//删除业务线
	@RequestMapping(value = "deleteBusiness", method = RequestMethod.POST)
	public CommonResponse deleteBusiness(@RequestBody DeleteReq deleteReq) {
		return quotoService.deleteBusiness(deleteReq);
	}

	// 获取所有的业务过程
	@RequestMapping(value = "queryAllBusinessProcess", method = RequestMethod.GET)
	public CommonResponse queryAllBusinessProcess(int theme_id) {
		return quotoService.queryAllBusinessProcess(theme_id);
	}

	// 获取主题
	@RequestMapping(value = "queryBusinessProcess", method = RequestMethod.GET)
	public CommonResponse queryBusinessProcess(Integer theme_id,String search_key,int page,int size,String order_name,boolean desc) {
		return quotoService.queryBusinessProcess(theme_id,search_key,page,size,order_name,desc);
	}

	// 增加业务过程
	@RequestMapping(value = "addBusinessProcess", method = RequestMethod.POST)
	public CommonResponse addBusinessProcess(@RequestBody BusinessProcess businessProcess) {
		return quotoService.addBusinessProcess(businessProcess);
	}

	// 增加业务过程
	@RequestMapping(value = "updateBusinessProcess", method = RequestMethod.POST)
	public CommonResponse updateBusinessProcess(@RequestBody BusinessProcess businessProcess) {
		return quotoService.updateBusinessProcess(businessProcess);
	}

	//删除业务过程
	@RequestMapping(value = "deleteBusinessProcess", method = RequestMethod.POST)
	public CommonResponse deleteBusinessProcess(@RequestBody DeleteReq deleteReq) {
		return quotoService.deleteBusinessProcess(deleteReq);
	}


	// 获取主题
	@RequestMapping(value = "queryTheme", method = RequestMethod.GET)
	public CommonResponse queryTheme(Integer business_id,String search_key,int page,int size,String order_name,boolean desc) {
		return quotoService.queryTheme(business_id,search_key,page,size,order_name,desc);
	}

	// 获取主题
	@RequestMapping(value = "queryAllTheme", method = RequestMethod.GET)
	public CommonResponse queryAllTheme() {
		return quotoService.queryAllTheme();
	}

	// 增加主题
	@RequestMapping(value = "addTheme", method = RequestMethod.POST)
	public CommonResponse addTheme(@RequestBody Theme theme) {
		return quotoService.addTheme(theme);
	}

	// 更新主题
	@RequestMapping(value = "updateTheme", method = RequestMethod.POST)
	public CommonResponse updateTheme(@RequestBody Theme theme) {
		return quotoService.updateTheme(theme);
	}

	//删除主题
	@RequestMapping(value = "deleteTheme", method = RequestMethod.POST)
	public CommonResponse deleteTheme(@RequestBody DeleteReq deleteReq) {
		return quotoService.deleteTheme(deleteReq);
	}

	// 获取所有的数据服务
	@RequestMapping(value = "queryAllDataService", method = RequestMethod.GET)
	public CommonResponse queryAllDataService(Integer themeId) {
		return quotoService.queryAllDataService(themeId);
	}

	// 获取表下还没被使用的指标信息
	@RequestMapping(value = "queryUsableQuotoInfoByTableId", method = RequestMethod.GET)
	public CommonResponse queryUsableQuotoInfoByTableId(int tableId) {
		return quotoService.queryUsableQuotoInfoByTableId(tableId);
	}

	// 根据tableId查询时间列
	@RequestMapping(value = "queryTimeColunm", method = RequestMethod.GET)
	public CommonResponse queryTimeColunm(int tableId) {
		return quotoService.queryTimeColunm(tableId);
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

	// 查询指标
	@RequestMapping(value = "queryQuotoNeedParm", method = RequestMethod.GET)
	public CommonResponse queryQuotoNeedParm(int id) {
		return quotoService.queryQuotoNeedParm(id);
	}

	// 查询指标
	@RequestMapping(value = "queryAll", method = RequestMethod.POST)
	public CommonResponse queryAllQuoto(@RequestBody QuotoQuery quotoQuery) {
		return quotoService.queryAllQuoto(quotoQuery);
	}

	//查询指标的历史记录
	@RequestMapping(value = "queryQuotoUpdateHistory", method = RequestMethod.GET)
	public CommonResponse queryQuotoUpdateHistory(int id) {
		return quotoService.queryQuotoUpdateHistory(id);
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
	@RequestMapping(value = "queryQuotoData", method = RequestMethod.POST)
	public CommonResponse queryQuotoData(@RequestBody QuotoDataReq quotoDataReq) {
		CommonResponse commonResponse = new CommonResponse();
		DataCommonResponse dataCommonResponse = quotoService.queryQuotoData(quotoDataReq.getId(),
				quotoDataReq.getName(), quotoDataReq.getField(), quotoDataReq.getPage(), quotoDataReq.getCount(),
				quotoDataReq.getOrder(), quotoDataReq.getAcs(),quotoDataReq.getParm_value());
		commonResponse.setData(dataCommonResponse.getData());
		commonResponse.setMessage(dataCommonResponse.getMessage());
		commonResponse.setSuccess(dataCommonResponse.isSuccess());
		return commonResponse;
	}

	// 获取指标数据
	@RequestMapping(value = "queryExtendQuotoData", method = RequestMethod.POST)
	public DataCommonResponse queryExtendQuotoData(@RequestBody QuotoDataReq quotoDataReq) {
		DataCommonResponse dataCommonResponse = quotoService.queryQuotoData(quotoDataReq.getId(),
				quotoDataReq.getName(), quotoDataReq.getField(), quotoDataReq.getPage(), quotoDataReq.getCount(),
				quotoDataReq.getOrder(), quotoDataReq.getAcs(),quotoDataReq.getParm_value());
		return dataCommonResponse;
	}

	// 获取指标调用文档
	@RequestMapping(value = "queryQuotoApiDoc", method = RequestMethod.GET)
	public CommonResponse queryQuotoApiDoc(int id) {
		CommonResponse CommonResponse = quotoService.queryQuotoApiDoc(id);
		return CommonResponse;
	}

	// 获取express信息
	@RequestMapping(value = "queryExpressInfo", method = RequestMethod.POST)
	public DataCommonResponse queryExpressInfo(@RequestBody ExpressInfoReq express) {
		DataCommonResponse dataCommonResponse = new DataCommonResponse();

		if (StringUtils.isEmpty(express.getExpress())) {
			dataCommonResponse.setSuccess(false);
			dataCommonResponse.setMessage("参数值不能为空");
			return dataCommonResponse;
		}
		express.setExpress(express.getExpress().replace(" ", ""));
		try {
			dataCommonResponse = quotoService.caculate(express.getExpress() + "#", 0, 2, null,  null,express.getParm_value());
		} catch (Exception e) {
			// TODO: handle exception
			dataCommonResponse.setSuccess(false);
			dataCommonResponse.setMessage(e.getMessage());
		}
		return dataCommonResponse;
	}


}
