//package com.taobao.tddl.sqlobjecttree;
//
//import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.ChangePageNumber;
//import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.ChangeTable;
//import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.ChangeTableAndPageNumber;
//import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.MySQL_ChangeTable_PageNumber_Version;
//import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.MySQL_ChangeTable_Version;
//import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.Normal;
//import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.Oracle_ChangeTable_PageNumber_Version;
//import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.Oracle_ChangeTable_Version;
//
//public class OutputHandlerConsist {
//	//TODO:使用builder模式重构此处逻辑
//	public final static OutputHandler NORMAL = new Normal();
//
//	public final static OutputHandler CHANGE_TABLE_AND_PAGENUMBER = new ChangeTableAndPageNumber();
//
//	public final static OutputHandler ORACLE_CHANGE_TABLE_PAGENUMBER_VERSION = new Oracle_ChangeTable_PageNumber_Version();
//
//	public final static OutputHandler MYSQL_CHANGE_TABLE_PAGENUMBER_VERSION = new MySQL_ChangeTable_PageNumber_Version();
//
//	public final static OutputHandler ORACLE_CHANGE_TABLE_VERSION = new Oracle_ChangeTable_Version();
//
//	public final static OutputHandler MYSQL_CHANGE_TABLE_VERSION = new MySQL_ChangeTable_Version();
//
//	public final static OutputHandler CHANGE_TABLE = new ChangeTable();
//
//	public final static OutputHandler CHANGE_PAGENUMBER = new ChangePageNumber();
//}
