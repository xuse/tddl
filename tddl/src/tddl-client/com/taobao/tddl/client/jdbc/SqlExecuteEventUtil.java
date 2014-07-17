package com.taobao.tddl.client.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.taobao.tddl.client.controller.ColumnMetaData;
import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.interact.rule.enumerator.Enumerator;
import com.taobao.tddl.interact.rule.enumerator.EnumeratorImp;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeBaseList;

/**
 * 一个与ExecuteEvent相关的工具类
 * 
 * @author linxuan
 *
 */
public class SqlExecuteEventUtil {
	private static final Enumerator enumerator = new EnumeratorImp();

	/**
	 * 创建行复制的SqlExecuteEvent
	 */
	private static SqlExecuteEvent createEvent(DispatcherResult metaData, SqlType sqlType, String originalSql,
			Object primaryKeyValue) throws SQLException {
		DefaultSqlExecuteEvent event = new DefaultSqlExecuteEvent();
		event.setReplicated(metaData.needRowCopy());
		event.setSqlType(sqlType);
		event.setLogicTableName(metaData.getVirtualTableName().toString());
		event.setPrimaryKeyColumn(metaData.getPrimaryKey().key.toLowerCase());
		//event.setPrimaryKeyValue(metaData.getPrimaryKey().value.getValue());
		event.setPrimaryKeyValue(primaryKeyValue);

		if (hasAvalue(metaData.getSplitDB())) {
			event.setDatabaseShardColumn(metaData.getSplitDB().get(0).key.toLowerCase());
			event.setDatabaseShardValue(metaData.getSplitDB().get(0).value.getValue());
		}
		if (hasAvalue(metaData.getSplitTab())) {
			event.setTableShardColumn(metaData.getSplitTab().get(0).key.toLowerCase());
			event.setTableShardValue(metaData.getSplitTab().get(0).value.getValue());
		}

		event.setSql(originalSql);
		return event;
	}

	private static boolean hasAvalue(List<ColumnMetaData> columnMetaDatas) {
		if (columnMetaDatas != null && !columnMetaDatas.isEmpty() && columnMetaDatas.get(0).key != null
				&& columnMetaDatas.get(0).key.length() > 0 && columnMetaDatas.get(0).value != null) {
			return true;
		}
		return false;
	}

	private static boolean isConfuse(ColumnMetaData uniqeMeta, List<ColumnMetaData> splitMetas) {
		if (hasAvalue(splitMetas) && !uniqeMeta.key.equals(splitMetas.get(0).key)) {
			Comparative value = splitMetas.get(0).value;
			if ((value instanceof ComparativeBaseList) || (value.getComparison() != Comparative.Equivalent)) {
				//唯一键不是分库/分表键, 且分库/分表键在sql中有条件，并且不是仅仅一个=条件(and/or或>=<)
				return true;
			}
		}
		return false;
	}

	/**
	 * update in的问题：
	 * 如果update in 的字段本身就是分库分表字段，那是可以支持同时更新多条数据到不同库表的。
	 * 只是在update in不是分库分表字段的情况下，才要求where中的分库分表键要么只有一个=条件，要么没有。
	 * 否则tddl无法知道哪个id在分库分表键的哪个值中能找到，只能简单的让每个id对应第一个分库分表键，
	 * 这样在对不上号的情况下,行复制去getMasterRow（查主库）时就会定位到错误的库表，报主库记录不存在，
	 * 最终造成数据更行丢失
	 * 
	 * 上面一种效率低下的解决方法：当发现uniqekey不是分库分表字段，并且分库分表字段有多个条件的时候,
	 * 在行复制的event中干脆不填分库分表字段值。查主库时去做所有表扫描。但是这又要求业务不能配默认库表。
	 * 
	 * 总的来说有以下限制：
	 * 1. 规则缺少唯一键，或SQL中没带唯一键值的条件，行复制不支持，抛异常
	 * 2. 分库键或分表键有多个，不支持。抛异常。（因为sync_log日志表结构的问题，除非用逗号分隔的方式处理）
	 * 3. 就是上述说的：唯一键不是分库/分表键, 且分库/分表键在sql中有条件，并且不是仅仅一个=条件，抛异常
	 *    唯一键不是分库/分表键的情况，具体列为以下场景：
	 *    a：分库/分表键在SQL中没有条件：支持。日志记录中分库分表列值都为空，行复制读主库时，扫所有的表
	 *    b: 分库/分表键在SQL中只有一个等于条件：支持。插日志时，多条记录唯一键不同（in），分库/分表键填=后的值
	 *    c: 分库/分表键在SQL中只有一个条件，但不是=条件：不支持，抛异常。< > <= >= != 无法对号
	 *    d: 分库/分表键在SQL中有多个条件：不支持，抛异常。
	 */
	public static List<SqlExecuteEvent> createEvent(DispatcherResult metaData, SqlType sqlType, String originalSql)
			throws SQLException {

		if (metaData.getPrimaryKey() == null || metaData.getPrimaryKey().key == null
				|| metaData.getPrimaryKey().key.length() == 0) {
			throw new SQLException("分库分表规则缺少唯一键项");
		}
		if (metaData.getPrimaryKey().value == null) {
			throw new SQLException("SQL中没带唯一键, sql = " + originalSql);
		}

		if (metaData.getSplitDB() != null && metaData.getSplitDB().size() > 1) {
			throw new SQLException("TDDL行复制目前不支持sql中的分库字段多于两个：" + originalSql);
		}
		if (metaData.getSplitTab() != null && metaData.getSplitTab().size() > 1) {
			throw new SQLException("TDDL行复制目前不支持sql中的分表字段多于两个：" + originalSql);
		}

		if (isConfuse(metaData.getPrimaryKey(), metaData.getSplitDB())) {
			throw new SQLException("唯一键不是分库键, 且分库键在sql中有条件，并且不是仅仅一个等于条件：" + originalSql);
		}
		if (isConfuse(metaData.getPrimaryKey(), metaData.getSplitTab())) {
			throw new SQLException("唯一键不是分表键, 且分库键在sql中有条件，并且不是仅仅一个等于条件：" + originalSql);
		}

		boolean needMergeValueInCloseRange = false;
		List<SqlExecuteEvent> res = new ArrayList<SqlExecuteEvent>();
		Set<Object> uniqeKeyValues = enumerator.getEnumeratedValue(metaData.getPrimaryKey().value, null, null,needMergeValueInCloseRange);
		for (Object uniqeKeyValue : uniqeKeyValues) {
			res.add(createEvent(metaData, sqlType, originalSql, uniqeKeyValue));
		}
		return res;
	}
}
