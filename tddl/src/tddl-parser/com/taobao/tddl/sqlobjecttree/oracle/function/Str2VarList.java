package com.taobao.tddl.sqlobjecttree.oracle.function;

import java.util.List;

import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.value.OneArgFunction;

public class Str2VarList extends OneArgFunction{

	@Override
	public String getFuncName() {
		return "STR2VARLIST";
	}
	@Override
	public Comparable<?> getVal(List<Object> args) {
		Comparable<?> var=Utils.getVal(args, arg1);
		return Str2StrList(var);
	}
	private static Comparable<?> Str2StrList(Comparable<?> val) {
		String[] argList;
		if(val instanceof String){
			argList=((String)val).split(",");
		}else{
			throw new IllegalArgumentException("不应该出现除string以外其他类型的参数,当前参数为:"+val);
		}
		return buildOr(argList);
	}
	private static Comparable<?> buildOr(String[] argList) {
		ComparativeOR or=new ComparativeOR();
		for(Comparable<?> arg:argList){
			or.addComparative(new Comparative(Comparative.Equivalent,arg));
		}
		return or;
	}

}
