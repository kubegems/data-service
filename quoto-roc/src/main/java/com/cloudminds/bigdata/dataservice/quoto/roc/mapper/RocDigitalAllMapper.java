package com.cloudminds.bigdata.dataservice.quoto.roc.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotHours;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotSize;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.CustomerSeat;
import com.cloudminds.bigdata.dataservice.quoto.roc.entity.RobotCount;

@Mapper
public interface RocDigitalAllMapper {

	// robotQuoto
	// 总在线时长
	@Select("select tenant_id as tenantId, robot_id as robotId, round(count(*)/6,1) as hours from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'robotOnlineTick' group by tenant_id,robot_id")
	public List<RobotHours> queryOnlineHoursByEnv(String env);

	// 上线次数
	@Select("select tenant_id as tenantId, robot_id as robotId, count(*) as count from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'robotConnect' group by tenant_id,robot_id")
	public List<RobotCount> queryConnectCountByEnv(String env);

	// 异常下线次数
	@Select("select tenant_id as tenantId, robot_id as robotId, count(*) as count from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'robotAbnormalDisconnect' group by tenant_id,robot_id")
	public List<RobotCount> queryAbnormalDisconnectCountByEnv(String env);

	// 告警次数
	@Select("select tenant_id as tenantId, robot_id as robotId, count(*) as count from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'robotAlarm' group by tenant_id,robot_id")
	public List<RobotCount> queryAlarmCountByEnv(String env);

	// digitalAssetsQuoto
	// 人脸抠图
	@Select("select tenant_id as tenantId, robot_id as robotId, count(*) as count from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'FRCounter' group by tenant_id,robot_id")
	public List<RobotCount> queryFRCountByEnv(String env);

	// 语音分片
	@Select("select tenant_id as tenantId, robot_id as robotId, count(*) as count from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'voiceClipCounter' group by tenant_id,robot_id")
	public List<RobotCount> queryVoiceClipCountByEnv(String env);

	// ASR调用次数
	@Select("select tenant_id as tenantId, robot_id as robotId, count(*) as count from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'asrCounter' group by tenant_id,robot_id")
	public List<RobotCount> queryAsrCountByEnv(String env);

	// 音视频大小
	@Select("select tenant_id as tenantId, robot_id as robotId, sum(size) as size from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'avStream' group by tenant_id,robot_id")
	public List<RobotSize> queryAvStreamSizeByEnv(String env);

	// 音视频时长
	@Select("select tenant_id as tenantId, robot_id as robotId, sum(duration) as hours from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'avStream' group by tenant_id,robot_id")
	public List<RobotHours> queryAvStreamDurationByEnv(String env);

	// HI回复数
	@Select("select tenant_id as tenantId, robot_id as robotId, count(*) as count from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'hiInventionCounter' group by tenant_id,robot_id")
	public List<RobotCount> queryHiInventionCountByEnv(String env);

	// AI置信度1的回复
	@Select("select tenant_id as tenantId, robot_id as robotId, count(*) as count from harix.mv_roc_digital_all prewhere startsWith(env, #{env}) and rod_type = 'aiResponseCounter' group by tenant_id,robot_id")
	public List<RobotCount> queryAiResponseCountByEnv(String env);

	// customerServiceQuoto
	// 实际使用人力
	@Select("select seat, count(*) as count from harix.mv_roc_digital_all where startsWith(env, #{env}) and rod_type = 'hiServiceLogin' group by seat")
	public List<CustomerSeat> queryHiServiceLoginCountByEnv(String env);

	// AI置信度1的回复
	@Select("select seat, count(*) as count from harix.mv_roc_digital_all where startsWith(env, #{env}) and rod_type = 'hiServiceCounter' group by seat")
	public List<CustomerSeat> queryHiServiceCountByEnv(String env);
}
