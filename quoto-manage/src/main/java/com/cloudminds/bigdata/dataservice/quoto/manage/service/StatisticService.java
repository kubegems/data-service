package com.cloudminds.bigdata.dataservice.quoto.manage.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudminds.bigdata.dataservice.quoto.manage.entity.response.CommonResponse;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.QuotoAccessHistoryMapper;
import com.cloudminds.bigdata.dataservice.quoto.manage.mapper.QuotoMapper;

@Service
public class StatisticService {
	@Autowired
	private QuotoAccessHistoryMapper quotoAccessHistoryMapper;

	@Autowired
	private QuotoMapper quotoMapper;

	public CommonResponse queryTypeNum() {
		// TODO Auto-generated method stub
		CommonResponse commonResponse = new CommonResponse();
		commonResponse.setData(quotoMapper.queryQuotoTypeNum());
		return commonResponse;
	}
}
