package com.cloudminds.bigdata.dataservice.quoto.roc.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.quoto.roc.entity.CustomerSeat;
import com.cloudminds.bigdata.dataservice.quoto.roc.service.CustomerServiceQuotoService;

@RestController
@RequestMapping("/roc/customerService")
public class CustomerServiceQuotoControl {

	@Autowired
	private CustomerServiceQuotoService customerServiceQuotoService;

	// 实际使用人力
	@RequestMapping(value = "hiServiceLogin", method = RequestMethod.GET)
	public List<CustomerSeat> queryHiServiceLoginCountByEnv(String env) {
		return customerServiceQuotoService.queryHiServiceLoginCountByEnv(env);
	}

	// AI置信度1的回复
	@RequestMapping(value = "hiServiceCounter", method = RequestMethod.GET)
	public List<CustomerSeat> queryHiServiceCountByEnv(String env) {
		return customerServiceQuotoService.queryHiServiceCountByEnv(env);
	}
}
