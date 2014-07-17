package com.taobao.tddl.sqlobjecttree.oracle.function;


import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.sqlobjecttree.HintSetter;
import com.taobao.tddl.sqlobjecttree.oracle.hint.Index;
import com.taobao.tddl.sqlobjecttree.oracle.hint.Ordered;
import com.taobao.tddl.sqlobjecttree.oracle.hint.UseNL;


public class OracleHintRegister {
	public final static OracleHintRegister reg=new OracleHintRegister();
	private  final static Map<String, Class<? extends HintSetter>> funcReg=new HashMap<String, Class<? extends HintSetter>>();
	static{
		//control
		funcReg.put("USE_NL", UseNL.class);
		funcReg.put("ORDERED", Ordered.class);
		funcReg.put("INDEX", Index.class);
		
	}
	public boolean containsKey(String key){
		return  funcReg.containsKey(key);
	}
	public HintSetter get(String key){
		HintSetter cls=null;
		try {
			if(key==null||key.trim().equals("")){
				throw new IllegalArgumentException("group function²»ÄÜÎª¿Õ");
			}
			cls=funcReg.get(key.toUpperCase()).newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);	
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);	
		}
		return cls;
	}
	
}
