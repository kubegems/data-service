package com.cloudminds.bigdata.dataservice.quoto.roc.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudminds.bigdata.dataservice.quoto.roc.entity.CustomerSeat;
import com.cloudminds.bigdata.dataservice.quoto.roc.mapper.RocDigitalAllMapper;

@Service
public class CustomerServiceQuotoService {
	@Autowired
	private RocDigitalAllMapper mapper;

	// 实际使用人力
	public List<CustomerSeat> queryHiServiceLoginCountByEnv(String env) {
		return mapper.queryHiServiceLoginCountByEnv(env + "%");
	}

	// AI置信度1的回复
	public List<CustomerSeat> queryHiServiceCountByEnv(String env) {
		return mapper.queryHiServiceCountByEnv(env + "%");
	}
}
