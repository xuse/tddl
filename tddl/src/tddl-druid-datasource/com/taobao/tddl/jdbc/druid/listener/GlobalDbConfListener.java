package com.taobao.tddl.jdbc.druid.listener;

/**全局配置变化监听器
 * 
 * @author qihao
 *
 */
public interface GlobalDbConfListener {

	void handleData(String dataId, String data);
}
