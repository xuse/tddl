package com.taobao.tddl.common.eagleeye;

import com.taobao.eagleeye.EagleEye;
import com.taobao.tddl.common.channel.SqlMetaData;

/**
 * @author jiechen.qzm
 * Eagleeye帮助类，协助记录查询时间
 */
public class EagleeyeHelper {
	
	/**
	 * execute之前写日志
	 * @param datasourceWrapper
	 * @param sqlType
	 * @throws Exception
	 */
	public static void startRpc(String ip, String port, String dbName, String sqlType){
		EagleEye.startRpc(dbName, sqlType);
		EagleEye.remoteIp(ip + ':' + port);
		EagleEye.rpcClientSend();
	}
	
	/**
	 * execute成功之后写日志
	 */
	public static void endSuccessRpc(String sql){
		EagleEye.rpcClientRecv(EagleEye.RPC_RESULT_SUCCESS, EagleEye.TYPE_TDDL, EagleEye.index(sql));
	}
	
	/**
	 * execute失败之后写日志
	 */
	public static void endFailedRpc(String sql){
		EagleEye.rpcClientRecv(EagleEye.RPC_RESULT_FAILED, EagleEye.TYPE_TDDL, EagleEye.index(sql));
	}

	/**
	 * @param sqlMetaData
	 * @param e
	 */
	public static void endRpc(SqlMetaData sqlMetaData, Exception e){
		if(e == null){
			endSuccessRpc(sqlMetaData.getLogicSql());
		}
		else {
			endFailedRpc(sqlMetaData.getLogicSql());
		}
	}
	
	
}
