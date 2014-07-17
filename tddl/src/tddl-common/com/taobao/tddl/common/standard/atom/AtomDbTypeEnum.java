package com.taobao.tddl.common.standard.atom;

import com.taobao.tddl.common.client.util.DataSourceType;


/**
 * 数据库类型枚举类型
 * 
 * @author qihao
 *
 */
public enum AtomDbTypeEnum {
	
	ORACLE,

	MYSQL;

//	ORACLE(TAtomConstants.DEFAULT_ORACLE_DRIVER_CLASS, TAtomConstants.DEFAULT_ORACLE_SORTER_CLASS),
//
//	MYSQL(TAtomConstants.DEFAULT_MYSQL_DRIVER_CLASS, TAtomConstants.DEFAULT_MYSQL_SORTER_CLASS);
	
//	ORACLE_DRUID(DruidConstants.DEFAULT_ORACLE_DRIVER_CLASS, DruidConstants.DEFAULT_DRUID_ORACLE_SORTER_CLASS),
//
//	MYSQL_DRUID(DruidConstants.DEFAULT_MYSQL_DRIVER_CLASS, DruidConstants.DEFAULT_DRUID_MYSQL_SORTER_CLASS);

	private String driverClass;
	private String sorterClass;

//	AtomDbTypeEnum(String driverClass, String sorterClass) {
//		this.driverClass = driverClass;
//		this.sorterClass = sorterClass;
//	}
	
	private void init(DataSourceType dataSourceType){
		if(dataSourceType == DataSourceType.DruidDataSource){
			if(this == AtomDbTypeEnum.ORACLE){
				this.driverClass = DruidConstants.DEFAULT_ORACLE_DRIVER_CLASS;
				this.sorterClass = DruidConstants.DEFAULT_DRUID_ORACLE_SORTER_CLASS;
			}
			else {
				this.driverClass = DruidConstants.DEFAULT_MYSQL_DRIVER_CLASS;
				this.sorterClass = DruidConstants.DEFAULT_DRUID_MYSQL_SORTER_CLASS;
			}
		}
		else if(dataSourceType == DataSourceType.TbDataSource){
			if(this == AtomDbTypeEnum.ORACLE){
				this.driverClass = TAtomConstants.DEFAULT_ORACLE_DRIVER_CLASS;
				this.sorterClass = TAtomConstants.DEFAULT_ORACLE_SORTER_CLASS;
			}
			else {
				this.driverClass = TAtomConstants.DEFAULT_MYSQL_DRIVER_CLASS;
				this.sorterClass = TAtomConstants.DEFAULT_MYSQL_SORTER_CLASS;
			}
		}
	}

//	public static AtomDbTypeEnum getAtomDbTypeEnum(String dbType) {
//		return getAtomDbTypeEnum(dbType, DataSourceType.TbDataSource);
//	}
	
	public static AtomDbTypeEnum getAtomDbTypeEnum(String dbType, DataSourceType dataSourceType) {
		try {
			AtomDbTypeEnum atomDbTypeEnum =  AtomDbTypeEnum.valueOf(dbType.trim().toUpperCase());
			atomDbTypeEnum.init(dataSourceType);
			return atomDbTypeEnum;
		} catch (Exception e) {
			return null;
		}
	}

	public String getDriverClass() {
		return driverClass;
	}

	public String getSorterClass() {
		return sorterClass;
	}
	
	
	
}
