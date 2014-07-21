package com.taobao.tddl.jdbc.druid.listener;

import com.taobao.tddl.common.standard.atom.AtomDbStatusEnum;

/**数据库状态变化监听器
 * 
 * @author qihao
 *
 */
public interface DruidDbStatusListener {

	void handleData(AtomDbStatusEnum oldStatus, AtomDbStatusEnum newStatus);
}
