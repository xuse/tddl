package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.sqlobjecttree.IndexWrapper;
import com.taobao.tddl.sqlobjecttree.TableWrapper;
import com.taobao.tddl.sqlobjecttree.VersionWrapper;

public class HandlerContainer {
	public static void main(String[] args) {
		
	}
	RangePlaceHandler rangePlaceHandler;
	Map<String, PlaceHolderReplaceHandler> placeHolderHandlerMap;
	@SuppressWarnings("unchecked")
	public HandlerContainer() {
		rangePlaceHandler = DEFAULT_RANGE_HANDLER;
		placeHolderHandlerMap = (Map<String, PlaceHolderReplaceHandler>)DEFAULT_PLACE_HOLDER_MAP.clone();
	}
	/**
	 * 用于一些直接修改sql的替换，比如index 表名，version
	 */
	static final HashMap<String, PlaceHolderReplaceHandler> DEFAULT_PLACE_HOLDER_MAP = new HashMap<String, PlaceHolderReplaceHandler>(4);
	/**
	 * 用于对oracle和mysql的range 数字进行替换
	 */
	public static RangePlaceHandler DEFAULT_RANGE_HANDLER = new NormalRangePlaceHandler();
	
	
	public static PlaceHolderReplaceHandler DEFAULT_INDEX_HANDLER = new DefaultIndexPlaceHolderHandler();
	public static PlaceHolderReplaceHandler DEFAULT_TABLE_HANDLER = new DefaultTablePlaceHolderHandler();
	public static PlaceHolderReplaceHandler DEFAULT_VERSION_HANDLER = new DefaultVersionReplaceHolderHandler();
	
	
	public static PlaceHolderReplaceHandler indexHandler = new IndexPlaceHolderHandler();
	public static PlaceHolderReplaceHandler tableHandler = new TablePlaceHolderHanlder();
	public static PlaceHolderReplaceHandler mysqlVersioinHandler = new MySQLVersionHolderReplaceHandler();
	public static PlaceHolderReplaceHandler oracleVersionHandler = new OracleVersionReplaceHolderHandler();
	
	
	static{
		DEFAULT_PLACE_HOLDER_MAP.put(IndexWrapper.class.getName(), DEFAULT_INDEX_HANDLER);
		DEFAULT_PLACE_HOLDER_MAP.put(TableWrapper.class.getName(), DEFAULT_TABLE_HANDLER);
		DEFAULT_PLACE_HOLDER_MAP.put(VersionWrapper.class.getName(), DEFAULT_VERSION_HANDLER);
	}
	boolean allowChangePageNumber;
	
	public boolean isAllowChangePageNumber() {
		return allowChangePageNumber;
	}

	public RangePlaceHandler getRangePlaceHandler(){
		return rangePlaceHandler;
	}
	public PlaceHolderReplaceHandler getPlaceHolderPlaceHandler(Object target){
		PlaceHolderReplaceHandler rangePlaceHandler = placeHolderHandlerMap.get(target.getClass().getName());
		if(rangePlaceHandler != null){
			return rangePlaceHandler;
		}else{
			throw new IllegalArgumentException("unknow !"+target.getClass().getName());
		}
	}
	public void setVersionHandler(PlaceHolderReplaceHandler versionHandler) {
		placeHolderHandlerMap.put(VersionWrapper.class.getName(), versionHandler);
	}
	public void changeOracleUpdateVersion(){
		setVersionHandler(oracleVersionHandler);
	}
	public void changeMySQLUpdateVersion(){
		setVersionHandler(mysqlVersioinHandler);
	}
	
	public void setTableHandler(PlaceHolderReplaceHandler tableHandler) {
		placeHolderHandlerMap.put(TableWrapper.class.getName(), tableHandler);
	}
	public void changeTable(){
		setTableHandler(tableHandler);
	}
	public void changeIndex(){
		setIndexHandler(indexHandler);
	}
	
	public void setIndexHandler(PlaceHolderReplaceHandler indexHandler) {
		placeHolderHandlerMap.put(IndexWrapper.class.getName(), indexHandler);
	}
	
	public void changeRange(Number limitFrom,Number limitTo){
		RangePlaceHandler rangeHandler = new RangePlaceHandler(limitFrom,limitTo);
		
		setRangeHandler(rangeHandler);
	}
	public void setRangeHandler(RangePlaceHandler rangePlaceHandler){
		
		this.rangePlaceHandler = rangePlaceHandler;
		if(this.rangePlaceHandler != DEFAULT_RANGE_HANDLER){
			this.allowChangePageNumber = true;
		}
	}
}
