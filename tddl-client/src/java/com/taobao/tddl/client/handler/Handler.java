//Copyright(c) Taobao.com
package com.taobao.tddl.client.handler;

import java.sql.SQLException;

import com.taobao.tddl.client.databus.DataBus;

/**
 * @description Handler体系的顶级接口
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-08-15下午03:23:40
 */
public interface Handler {
	/**
	 * 从ctx取得所需数据进行处理，将处理结果放回ctx, 并流向下一个处理器
	 * 
	 * @param ctx
	 *            总线数据结构
	 * @throws SQLException
	 */
	public void handleDown(DataBus dataBus) throws SQLException;
}
