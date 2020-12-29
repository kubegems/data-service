package com.cloudminds.bigdata.dataservice.quoto.roc.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotCount;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotHours;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotSize;
import com.cloudminds.bigdata.dataservice.quoto.roc.mapper.RocDigitalAllMapper;

@Service
public class DigitalAssetsQuotoService {
	@Autowired
	private RocDigitalAllMapper mapper;

	// 人脸抠图
	public List<RobotCount> queryFRCountByEnv(String env) {
		return mapper.queryFRCountByEnv(env + "%");
	}

	// 语音分片
	public List<RobotCount> queryVoiceClipCountByEnv(String env) {
		return mapper.queryVoiceClipCountByEnv(env + "%");
	}

	// ASR调用次数
	public List<RobotCount> queryAsrCountByEnv(String env) {
		return mapper.queryAsrCountByEnv(env + "%");
	}

	// 音视频大小
	public List<RobotSize> queryAvStreamSizeByEnv(String env) {
		return mapper.queryAvStreamSizeByEnv(env + "%");
	}

	// 音视频时长
	public List<RobotHours> queryAvStreamDurationByEnv(String env) {
		return mapper.queryAvStreamDurationByEnv(env + "%");
	}

	// HI回复数
	public List<RobotCount> queryHiInventionCountByEnv(String env) {
		return mapper.queryHiInventionCountByEnv(env + "%");
	}

	// AI置信度1的回复
	public List<RobotCount> queryAiResponseCountByEnv(String env) {
		return mapper.queryAiResponseCountByEnv(env + "%");
	}
}
