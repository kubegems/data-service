package com.cloudminds.bigdata.dataservice.quoto.manage.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.service.StatisticService;

@RestController
@RequestMapping("/quotoManage/statistic")
public class StatisticControl {
	@Autowired
	private StatisticService statisticService;

	// 查询指标类型统计
	@RequestMapping(value = "queryTypeNum", method = RequestMethod.GET)
	public CommonResponse queryTypeNum() {
		return statisticService.queryTypeNum();
	}
	
	//查询指标的调用率
	@RequestMapping(value="",method=RequestMethod.GET)
	public void queryQuotoCallBack()
	{
		
	}
}
