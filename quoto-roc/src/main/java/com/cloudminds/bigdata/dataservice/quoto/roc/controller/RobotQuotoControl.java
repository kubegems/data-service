package com.cloudminds.bigdata.dataservice.quoto.roc.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotCount;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotHours;
import com.cloudminds.bigdata.dataservice.quoto.roc.service.RobotQuotoService;


@RestController
@RequestMapping("/roc/robot")
public class RobotQuotoControl {
	@Autowired
	private RobotQuotoService robotQuotoService;
	
	//总在线时长
	@RequestMapping(value = "onlineHours", method = RequestMethod.GET)
	public List<RobotHours> queryOnlineHoursByEnv(String env) {
		return robotQuotoService.queryOnlineHoursByEnv(env);
	}
	
	//上线次数
	@RequestMapping(value = "connectCount", method = RequestMethod.GET)
	public List<RobotCount> queryConnectCountByEnv(String env) {
		return robotQuotoService.queryConnectCountByEnv(env);
	}
	
	//异常下线次数
	@RequestMapping(value = "abnormalDisconnectCount", method = RequestMethod.GET)
	public List<RobotCount> queryAbnormalDisconnectCountByEnv(String env) {
		return robotQuotoService.queryAbnormalDisconnectCountByEnv(env);
	}
	
	//告警次数
	@RequestMapping(value = "alarmCount", method = RequestMethod.GET)
	public List<RobotCount> queryAlarmCountByEnv(String env) {
		return robotQuotoService.queryAlarmCountByEnv(env);
	}

}
