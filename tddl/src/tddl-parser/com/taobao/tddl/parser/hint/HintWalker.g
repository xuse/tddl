tree grammar HintWalker;
options {
tokenVocab=HintParser; // reuse token types
ASTLabelType=CommonTree; // $label will have type CommonTree
language=Java;
output=AST;
}
@header{
	package com.taobao.tddl.parser.hint;
	
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.sqlobjecttree.common.value.BindVar;
}

@members{
	public int index=0;
}
beg	returns [Map map]
	@init{
	$map=new HashMap();
	}:
	hints[$map]*
	;
hints [Map map] :
	^(HINT ID value){
		$map.put($ID.text,$value.retVal);
	}
	;
value returns [Object retVal]
	:
	^(EXP_LIST exp_list)
	{
		$retVal=$exp_list.retVal;
	}
	|exp
	{	
		HintElement tempMap=null;
		if($exp.retKey!=null){
			tempMap=new HintElement($exp.retKey,$exp.retVal);
		}else{
			throw new IllegalArgumentException("key is null,value is "+ $exp.retVal);
		}
		$retVal=tempMap;
	}
	;
	
exp_list returns [Object retVal]
	@init{
		List tempList=new ArrayList();
	}:
		(exp{
			tempList.add(new HintElement($exp.retKey,$exp.retVal));
		})*
		{
			$retVal=tempList;
		}
	;
	
exp returns[Object retKey,Object retVal] 
	:^(EXP expKey args?)
	{
		$retKey=$expKey.retVal;
		if($args.list!=null){
			$retVal=$args.list;
		}
	}
	;
expKey returns[Object retVal]
	:ID{
		$retVal=$ID.text;
	}
	|^(BIND_VAR '?')
	{
		$retVal=new BindVar(index);
		index++;
	}
	;
args	returns [List list]
	@init{
		$list=new ArrayList();
	}:^(ARGS (arg{$list.add($arg.retVal);})*)
	|arg{
		$list.add($arg.retVal);
	}
	;
arg	returns [Object retVal]:	
	^(ARG ID)
	{
		$retVal=$ID.text;
	}
	|^(BIND_VAR '?')
	{
		$retVal=new BindVar(index);
		index++;
	}
	;