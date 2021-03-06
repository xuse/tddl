//Copyright(c) Taobao.com
package com.taobao.tddl.common.sync;

import java.util.Map;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-9-27上午11:46:09
 */
public interface SlaveDataHandler {
	/**
	 * application can do something special by implementing this interface,add
	 * column and value,reduce column and value, change column and value.
	 * 
	 * be careful,do not change <b>tableShardColumn</b> or <b>dbShardColumn</b>.
	 * this will cause replication fail.
	 * 
	 * @param masterRow
	 *            master db data, key for columnName, value for columnValue
	 * @param info
	 *            replication slave infomation,just like
	 *            <b>tableShardColumn</b>, <b>dbShardColumn</b>,and so on,you
	 *            can get these info from it
	 * @return return to replication core,key for columnName,value for
	 *         columnValue
	 */
	public Map<String, Object> handle(Map<String, Object> masterRow,
			SlaveInfo info);
}
