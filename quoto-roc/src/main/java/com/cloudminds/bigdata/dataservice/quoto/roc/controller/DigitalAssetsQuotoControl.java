package com.cloudminds.bigdata.dataservice.quoto.roc.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotCount;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotHours;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotSize;
import com.cloudminds.bigdata.dataservice.quoto.roc.service.DigitalAssetsQuotoService;

@RestController
@RequestMapping("/roc/digitalAssets")
public class DigitalAssetsQuotoControl {
	@Autowired
	private DigitalAssetsQuotoService digitalAssetsQuotoService;

	// 人脸抠图
	@RequestMapping(value = "FRCounter", method = RequestMethod.GET)
	public List<RobotCount> queryFRCountByEnv(String env) {
		return digitalAssetsQuotoService.queryFRCountByEnv(env);
	}

	// 语音分片
	@RequestMapping(value = "voiceClipCounter", method = RequestMethod.GET)
	public List<RobotCount> queryVoiceClipCountByEnv(String env) {
		return digitalAssetsQuotoService.queryVoiceClipCountByEnv(env);
	}

	// ASR调用次数
	@RequestMapping(value = "asrCounter", method = RequestMethod.GET)
	public List<RobotCount> queryAsrCountByEnv(String env) {
		return digitalAssetsQuotoService.queryAsrCountByEnv(env);
	}

	// 音视频大小
	@RequestMapping(value = "avStreamSize", method = RequestMethod.GET)
	public List<RobotSize> queryAvStreamSizeByEnv(String env) {
		return digitalAssetsQuotoService.queryAvStreamSizeByEnv(env);
	}

	// 音视频时长
	@RequestMapping(value = "avStreamDuration", method = RequestMethod.GET)
	public List<RobotHours> queryAvStreamDurationByEnv(String env) {
		return digitalAssetsQuotoService.queryAvStreamDurationByEnv(env);
	}

	// HI回复数
	@RequestMapping(value = "hiInventionCounter", method = RequestMethod.GET)
	public List<RobotCount> queryHiInventionCountByEnv(String env) {
		return digitalAssetsQuotoService.queryHiInventionCountByEnv(env);
	}

	// AI置信度1的回复
	@RequestMapping(value = "aiResponseCounter", method = RequestMethod.GET)
	public List<RobotCount> queryAiResponseCountByEnv(String env) {
		return digitalAssetsQuotoService.queryAiResponseCountByEnv(env);
	}
}
