package com.taobao.tddl.jdbc.atom.listener;

import com.taobao.tddl.common.standard.atom.AtomDbStatusEnum;

/**数据库状态变化监听器
 * 
 * @author qihao
 *
 */
public interface TAtomDbStatusListener {

	void handleData(AtomDbStatusEnum oldStatus, AtomDbStatusEnum newStatus);
}
