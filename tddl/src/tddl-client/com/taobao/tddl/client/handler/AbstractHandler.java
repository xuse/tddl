//Copyright(c) Taobao.com
package com.taobao.tddl.client.handler;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.eagleeye.EagleEye;
import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.databus.PipelineRuntimeInfo;
import com.taobao.tddl.client.util.LogUtils;
import com.taobao.tddl.common.SQLPreParser;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.parser.ParserCache;

/**
 * @description 所有handler实现类的父类,主要提供以下几个方面的服务 1.日志记录方法 2.功能平行handler的公共方法,比如表名替换
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-08-27下午04:43:40
 */
public abstract class AbstractHandler implements Handler {
	public static final String ALL_PERF_TABLE_PREFIX = "__test_";
	protected static final Log sqlLog = LogFactory
			.getLog(LogUtils.TDDL_SQL_LOG);

	/**
	 * 全局表cache
	 */
	protected static final ParserCache globalCache = ParserCache.instance();

	/**
	 * 得到运行时信息
	 * 
	 * @param dataBus
	 * @return
	 */
	protected PipelineRuntimeInfo getPipeLineRuntimeInfo(DataBus dataBus) {
		return (PipelineRuntimeInfo) dataBus
				.getPluginContext(PipelineRuntimeInfo.INFO_NAME);
	}

	/**
	 * debug log
	 * 
	 * @param log
	 * @param contents
	 */
	protected void debugLog(Log log, Object[] contents) {
		if (log.isDebugEnabled()) {
			log.debug(getLogStr(contents));
		}
	}

	/**
	 * info log
	 * 
	 * @param log
	 * @param contents
	 */
	protected void infoLog(Log log, Object[] contents) {
		if (log.isInfoEnabled()) {
			log.info(getLogStr(contents));
		}
	}

	/**
	 * warn log
	 * 
	 * @param log
	 * @param contents
	 */
	protected void warnLog(Log log, Object[] contents) {
		if (log.isWarnEnabled()) {
			log.warn(getLogStr(contents));
		}
	}

	/**
	 * error log
	 * 
	 * @param log
	 * @param contents
	 */
	protected void errorLog(Log log, Object[] contents) {
		if (log.isErrorEnabled()) {
			log.error(getLogStr(contents));
		}
	}

	/**
	 * 取得日志
	 * 
	 * @param contents
	 * @return
	 */
	private String getLogStr(Object[] contents) {
		StringBuilder sb = new StringBuilder();
		for (Object obj : contents) {
			sb.append(String.valueOf(obj));
		}
		return sb.toString();
	}

	/**
	 * 从ThreadLocal里面取得本次查询是否使用并行
	 * 
	 * @return
	 */
	protected boolean getUseParallelFromThreadLocal() {
		Object obj = ThreadLocalMap.get(ThreadLocalString.PARALLEL_EXECUTE);
		boolean useParallel = false;
		if (null != obj) {
			useParallel = (Boolean) obj;
			ThreadLocalMap.put(ThreadLocalString.PARALLEL_EXECUTE, null);
		}

		return useParallel;
	}

	/**
	 * 替换SQL语句中虚拟表名为实际表名。 会 替换_tableName$ 替换_tableName_ 替换tableName.
	 * 替换tableName( 增加替换 _tableName, ,tableName, ,tableName_
	 * 
	 * @param originalSql
	 *            SQL语句
	 * @param virtualName
	 *            虚拟表名
	 * @param actualName
	 *            实际表名
	 * @return 返回替换后的SQL语句。
	 */
	public String replaceTableName(String originalSql, String virtualName,
			String actualName, Log log) {
		boolean padding = false;
		if (log.isDebugEnabled()) {
			StringBuilder buffer = new StringBuilder();
			buffer.append("virtualName = ").append(virtualName).append(", ");
			buffer.append("actualName = ").append(actualName);
			log.debug(buffer.toString());
		}

		//全链路压测需求
		String t=EagleEye.getUserData("t");
		
		if (virtualName.equalsIgnoreCase(actualName)
				&& (StringUtils.isBlank(t) || !t.equals("1"))) {
			return originalSql;
		}
		
		List<Object> sqlPieces = globalCache
				.getTableNameReplacement(originalSql);
		if (sqlPieces == null) {
			List<Object> pieces1 = parseAPattern_begin(virtualName,
					originalSql, new StringBuilder("\\s").append(virtualName)
							.append("$").toString(), padding);

			pieces1 = parseAPattern(virtualName, pieces1, new StringBuilder(
					"\\s").append(virtualName).append("\\s").toString(),
					padding);
			pieces1 = parseAPattern(virtualName, pieces1,
					new StringBuilder(".").append(virtualName).append("\\.")
							.toString(), padding);
			pieces1 = parseAPattern(virtualName, pieces1, new StringBuilder(
					"\\s").append(virtualName).append("\\(").toString(),
					padding);
			pieces1 = parseAPatternByCalcTable(virtualName, pieces1,
					new StringBuilder("//*+.*").append("_").append(virtualName)
							.append("_").append(".*/*/").toString(), padding);
			pieces1 = parseAPattern(virtualName, pieces1, new StringBuilder(
					"\\s").append(virtualName).append("\\,").toString(),
					padding);
			pieces1 = parseAPattern(virtualName, pieces1, new StringBuilder(
					"\\,").append(virtualName).append("\\s").toString(),
					padding);
			// 替换,tableName,
			pieces1 = parseAPattern(virtualName, pieces1, new StringBuilder(
					"\\,").append(virtualName).append("\\,").toString(),
					padding);
			sqlPieces = pieces1;
			sqlPieces = globalCache.setTableNameReplacementIfAbsent(
					originalSql, sqlPieces);
		}
		// 生成最终SQL
		StringBuilder buffer = new StringBuilder();
		boolean first = true;
		for (Object piece : sqlPieces) {
			if (!(piece instanceof String)) {
				throw new IllegalArgumentException(
						"should not be here ! table is " + piece);
			}
			if (!first) {
				if(!StringUtils.isBlank(t)&&t.equals("1")){
					buffer.append(ALL_PERF_TABLE_PREFIX);
				}
				
				buffer.append(actualName);
			} else {
				first = false;
			}
			buffer.append(piece);
		}

		return buffer.toString();
	}

	protected List<Object> parseAPattern_begin(String virtualName,
			String originalSql, String pattern, boolean padding) {
		Pattern pattern1 = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		List<Object> pieces1 = new LinkedList<Object>();
		Matcher matcher1 = pattern1.matcher(originalSql);
		int start1 = 0;
		while (matcher1.find(start1)) {
			pieces1.add(originalSql.substring(start1, matcher1.start() + 1));
			start1 = matcher1.end();
			if (padding) {
				// TODO: 大小写要验证一下
				pieces1.add(new LogicTable(virtualName));
			}
		}

		pieces1.add(originalSql.substring(start1));
		return pieces1;
	}

	protected List<Object> parseAPatternByCalcTable(String virtualName,
			List<Object> pieces, String pattern, boolean padding) {
		List<Object> pieces2 = new LinkedList<Object>();
		for (Object piece : pieces) {
			if (piece instanceof String) {
				String strpiece = (String) piece;
				Pattern pattern2 = Pattern.compile(pattern,
						Pattern.CASE_INSENSITIVE);
				Matcher matcher2 = pattern2.matcher(strpiece);
				int start2 = 0;
				while (matcher2.find(start2)) {
					int tableNameStart = matcher2.group().toUpperCase()
							.indexOf(virtualName.toUpperCase())
							//+ start2;
							+matcher2.start();
					int tableNameEnd = tableNameStart + virtualName.length();
					pieces2.add(strpiece.substring(start2, tableNameStart));
					start2 = tableNameEnd;
					if (padding) {
						pieces2.add(new LogicTable(virtualName));
					}
				}
				pieces2.add(strpiece.substring(start2));
			} else {
				pieces2.add(piece);
			}
		}
		return pieces2;
	}

	protected List<Object> parseAPattern(String virtualName,
			List<Object> pieces, String pattern, boolean padding) {
		List<Object> pieces2 = new LinkedList<Object>();
		for (Object piece : pieces) {
			if (piece instanceof String) {
				String strpiece = (String) piece;
				Pattern pattern2 = Pattern.compile(pattern,
						Pattern.CASE_INSENSITIVE);
				Matcher matcher2 = pattern2.matcher(strpiece);
				int start2 = 0;
				while (matcher2.find(start2)) {
					pieces2.add(strpiece.substring(start2 - 1 < 0 ? 0
							: start2 - 1, matcher2.start() + 1));
					start2 = matcher2.end();
					if (padding) {
						pieces2.add(new LogicTable(virtualName));
					}
				}
				pieces2.add(strpiece.substring(start2 - 1 < 0 ? 0 : start2 - 1));
			} else {
				pieces2.add(piece);
			}
		}
		return pieces2;
	}

	/**
	 * 多表替换，因为不常用，所以在那时没有和单表替换进行合并，而是单独拿出来一个新的方法。 与单表不同的是，在替换的过程中要记录下。
	 * 
	 * @param originalSql
	 * @param tableToBeReplaced
	 * @return
	 */
	protected String replaceMultiTableName(String originalSql,
			Map<String, String> tableToBeReplaced) {
		boolean padding = true;
		if (sqlLog.isDebugEnabled()) {
			StringBuilder buffer = new StringBuilder();
			buffer.append("table  = ").append(tableToBeReplaced).append(", ");
			sqlLog.debug(buffer.toString());
		}

		if (tableToBeReplaced.size() == 0) {
			String t = EagleEye.getUserData("t");
			if (!StringUtils.isBlank(t) && t.equals("1")) {
				String firstTable = SQLPreParser.findTableName(originalSql);
				return replaceTableName(originalSql, firstTable, firstTable,
						sqlLog);
			} else {
				return originalSql;
			}
		}
		
		if (tableToBeReplaced.size() == 1) {
			for (Entry<String, String> entry : tableToBeReplaced.entrySet()) {
				return replaceTableName(originalSql, entry.getKey(),
						entry.getValue(), sqlLog);
			}
		}
		List<Object> sqlPieces = globalCache
				.getTableNameReplacement(originalSql);
		if (sqlPieces == null) {
			for (Entry<String, String> entry : tableToBeReplaced.entrySet()) {
				String virtualName = entry.getKey();
				// tab$
				if (sqlPieces == null) {
					// 第一次进入，第二次以后进入就会有sqlPieces了
					sqlPieces = parseAPattern_begin(virtualName, originalSql,
							new StringBuilder("\\s").append(virtualName)
									.append("$").toString(), padding);
				} else {
					// tab$
					sqlPieces = parseAPattern(virtualName, sqlPieces,
							new StringBuilder("\\s").append(virtualName)
									.append("$").toString(), padding);
				}

				// tab
				sqlPieces = parseAPattern(
						virtualName,
						sqlPieces,
						new StringBuilder("\\s").append(virtualName)
								.append("\\s").toString(), padding);
				// table.
				sqlPieces = parseAPattern(virtualName, sqlPieces,
						new StringBuilder(".").append(virtualName)
								.append("\\.").toString(), padding);
				// tab(
				sqlPieces = parseAPattern(
						virtualName,
						sqlPieces,
						new StringBuilder("\\s").append(virtualName)
								.append("\\(").toString(), padding);
				// /*+ hint */
				sqlPieces = parseAPatternByCalcTable(
						virtualName,
						sqlPieces,
						new StringBuilder("//*+.*").append("_")
								.append(virtualName).append("_")
								.append(".*/*/").toString(), padding);
				sqlPieces = parseAPattern(
						virtualName,
						sqlPieces,
						new StringBuilder("\\s").append(virtualName)
								.append("\\,").toString(), padding);
				sqlPieces = parseAPattern(
						virtualName,
						sqlPieces,
						new StringBuilder("\\,").append(virtualName)
								.append("\\s").toString(), padding);
				// 替换,tableName,
				sqlPieces = parseAPattern(
						virtualName,
						sqlPieces,
						new StringBuilder("\\,").append(virtualName)
								.append("\\,").toString(), padding);
			}

			sqlPieces = globalCache.setTableNameReplacementIfAbsent(
					originalSql, sqlPieces);

		}

		// 生成最终SQL
		StringBuilder buffer = new StringBuilder();
		for (Object piece : sqlPieces) {
			if (piece instanceof String) {
				buffer.append(piece);
			} else if (piece instanceof LogicTable) {
				//全链路压测需求
				String t=EagleEye.getUserData("t");
				if(!StringUtils.isBlank(t)&&t.equals("1")){
					buffer.append(ALL_PERF_TABLE_PREFIX);
				}
				
				buffer.append(tableToBeReplaced
						.get(((LogicTable) piece).logictable));
			}
		}
		return buffer.toString();
	}

	static class LogicTable {
		public LogicTable(String logicTable) {
			this.logictable = logicTable;
		}

		public String logictable;

		@Override
		public String toString() {
			return "logictable:" + logictable;
		}
	}
	
	/**
	 * 流转类型，handler只对自己感兴趣的类型进行处理，否则略过
	 * 
	 * @author junyu
	 * 
	 */
	public enum FlowType {
		DIRECT, NOSQLPARSE, DEFAULT, BATCH,BATCH_DIRECT,BATCH_NOSQLPARSER,DBANDTAB_RC, // 只得到dispatchResult,基本测试用
		DBANDTAB_SQL
		// 只得到dispatchResult,基本测试用
	}
}
