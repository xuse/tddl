package com.taobao.tddl.common.config.atom;

import java.text.MessageFormat;

import com.taobao.tddl.common.util.StringUtils;

/**
 * atom ds里面跟获取配置相关的key
 * @author JIECHEN
 *
 */
public class TAtomConfConstants {

	/**
	 * 全局配置dataId模板
	 */
	private static final MessageFormat GLOBAL_FORMAT = new MessageFormat(
			"com.taobao.tddl.atom.global.{0}");

	/**
	 * 应用配置dataId模板
	 */
	private static final MessageFormat APP_FORMAT = new MessageFormat(
			"com.taobao.tddl.atom.app.{0}.{1}");

	private static final MessageFormat PASSWD_FORMAT = new MessageFormat(
			"com.taobao.tddl.atom.passwd.{0}.{1}.{2}");

	/**
	 * dbName模板
	 */
	private static final MessageFormat DB_NAME_FORMAT = new MessageFormat(
			"atom.dbkey.{0}^{1}^{2}");
	
	private static final String NULL_UNIT_NAME = "DEFAULT_UNIT";
	

	/**
	 * 根据dbKey获取全局配置dataId
	 *
	 * @param dbKey
	 *            数据库名KEY
	 * @return
	 */
	public static String getGlobalDataId(String dbKey) {
		return GLOBAL_FORMAT.format(new Object[] { dbKey });
	}

	/**
	 * 根据应用名和dbKey获取指定的应用配置dataId
	 *
	 * @param appName
	 * @param dbKey
	 * @return
	 */
	public static String getAppDataId(String appName, String dbKey) {
		return APP_FORMAT.format(new Object[] { appName, dbKey });
	}

	/**
	 * 根据dbKey和userName获得对应的passwd的dataId
	 *
	 * @param dbKey
	 * @param userName
	 * @return
	 */
	public static String getPasswdDataId(String dbName, String dbType,
			String userName) {
		return PASSWD_FORMAT.format(new Object[] { dbName, dbType, userName });
	}
	
	/**
	 * @param appName
	 * @param unitName
	 * @param dbkey
	 * @return
	 */
	public static String getDbNameStr(String unitName, String appName, String dbkey) {
		if(StringUtils.nullOrEmpty(unitName))
			return DB_NAME_FORMAT.format(new Object[] { NULL_UNIT_NAME, appName, dbkey });
		
		return DB_NAME_FORMAT.format(new Object[] { unitName , appName, dbkey });
	}

}
