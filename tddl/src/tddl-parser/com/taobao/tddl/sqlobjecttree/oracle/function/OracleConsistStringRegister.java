package com.taobao.tddl.sqlobjecttree.oracle.function;


import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.sqlobjecttree.Function;


public class OracleConsistStringRegister {
	public final static OracleConsistStringRegister reg=new OracleConsistStringRegister();
	private  final static Map<String, Class<? extends Function>> consistReg=new HashMap<String, Class<? extends Function>>();
	static{
		consistReg.put("SYSDATE",Sysdate.class);
	}
	public boolean containsKey(String key){
		return  consistReg.containsKey(key);
	}
	public Function get(String key){
		Function cls=null;
		try {
			if(key==null||key.trim().equals("")){
				throw new IllegalArgumentException("group function不能为空");
			}
			cls=consistReg.get(key.toUpperCase()).newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);	
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);	
		}
		return cls;
	}
	
}
