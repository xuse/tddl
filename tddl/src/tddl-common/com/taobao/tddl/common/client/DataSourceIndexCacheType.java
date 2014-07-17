package com.taobao.tddl.common.client;

public class DataSourceIndexCacheType {
	public static final int CLEAR_DATASOURCE_INDEX_AT_CONNECTION_CLOSE = 1;
	public static final int CLEAR_DATASOURCE_INDEX_AT_STATEMENT_CLOSE = 2;

	private static int type = CLEAR_DATASOURCE_INDEX_AT_CONNECTION_CLOSE;


	public static void setType(int type) {
		DataSourceIndexCacheType.type = type;
	}

	public static boolean clearAtConnectionClose() {
		return type == CLEAR_DATASOURCE_INDEX_AT_CONNECTION_CLOSE;
	}

	public static boolean clearAtStatementClose() {
		return type == CLEAR_DATASOURCE_INDEX_AT_CONNECTION_CLOSE;
	}
}
