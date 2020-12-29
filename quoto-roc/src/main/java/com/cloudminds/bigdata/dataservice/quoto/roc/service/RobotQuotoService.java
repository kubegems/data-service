package com.cloudminds.bigdata.dataservice.quoto.roc.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotCount;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotHours;
import com.cloudminds.bigdata.dataservice.quoto.roc.mapper.RocDigitalAllMapper;

@Service
public class RobotQuotoService {
	@Autowired
	private RocDigitalAllMapper mapper;

	//总在线时长
	public List<RobotHours> queryOnlineHoursByEnv(String env) {
		return mapper.queryOnlineHoursByEnv(env+"%");
	}
	
	//上线次数
	public List<RobotCount> queryConnectCountByEnv(String env) {
		return mapper.queryConnectCountByEnv(env+"%");
	}
	
	//异常下线次数
	public List<RobotCount> queryAbnormalDisconnectCountByEnv(String env) {
		return mapper.queryAbnormalDisconnectCountByEnv(env+"%");
	}
	
	//告警次数
	public List<RobotCount> queryAlarmCountByEnv(String env) {
		return mapper.queryAlarmCountByEnv(env+"%");
	}
}
