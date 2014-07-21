package com.taobao.tddl.interact.rule.bean;

/**
 * @author linxuan
 */
public enum DBType {
	ORACLE(0), MYSQL(1);
	private int i;

	private DBType(int i) {
		this.i = i;
	}

	public int value() {
		return this.i;
	}

	public static DBType valueOf(int i) {
		for (DBType t : values()) {
			if (t.value() == i) {
				return t;
			}
		}
		throw new IllegalArgumentException("Invalid SqlType:" + i);
	}
	/*
	public static DBType valueOf(String strType) {
		for (DBType t : values()) {
			if (t.toString().equals(strType)) {
				return t;
			}
		}
		throw new IllegalArgumentException("Invalid SqlType:" + strType);
	}
	*/
}
