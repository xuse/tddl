package com.taobao.tddl.jdbc.druid.config;

import com.taobao.tddl.common.config.ConfigDataListener;

public interface DbPasswdManager {
	
	/**获取数据库密码
	 * @return
	 */
	public String getPasswd();
	
	/**注册应用配置监听
	 * 
	 * @param Listener
	 */
	public void registerPasswdConfListener(ConfigDataListener Listener);
	
	/**
	 * 停止DbPasswdManager
	 */
	public void stopDbPasswdManager();
}
