package com.taobao.tddl.sqlobjecttree.oracle.function;

import java.util.List;

import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.value.OneArgFunction;

public class Str2NumList extends OneArgFunction{

	@Override
	public String getFuncName() {
		return "STR2NUMLIST";
	}
	@Override
	public Comparable<?> getVal(List<Object> args) {
		Comparable<?> var=Utils.getVal(args, arg1);
		return Str2NumberList(var);
	}
	private static Comparable<?> Str2NumberList(Comparable<?> val) {
		String[] argList;
		if(val instanceof String){
			argList=((String)val).split(",");
		}else{
			throw new IllegalArgumentException("不应该出现除string以外其他类型的参数");
		}
		return buildOr(argList);
	}
	private static Comparable<?> buildOr(String[] argList) {
		ComparativeOR or=new ComparativeOR();
		for(Comparable<?> arg:argList){
			Long tempint=Long.valueOf((String)arg);
			or.addComparative(new Comparative(Comparative.Equivalent,tempint));
		}
		return or;
	}

}
;