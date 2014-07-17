tree grammar OracleWalker;
options {
tokenVocab=OracleParser; // reuse token types
ASTLabelType=CommonTree; // $label will have type CommonTree
language=Java;
output=AST;
}
@header
{
 package  com.taobao.tddl.parser.oracle; 


import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.sqlobjecttree.*;
import static com.taobao.tddl.sqlobjecttree.Utils.*;
import com.taobao.tddl.sqlobjecttree.common.expression.ExpressionGroup;
import com.taobao.tddl.sqlobjecttree.JOIN_TYPE;
import com.taobao.tddl.sqlobjecttree.common.value.function.*;
import com.taobao.tddl.sqlobjecttree.common.value.*;

import com.taobao.tddl.sqlobjecttree.oracle.*;
import com.taobao.tddl.sqlobjecttree.JoinClause;
import com.taobao.tddl.sqlobjecttree.common.expression.OrExpressionGroup;
import com.taobao.tddl.sqlobjecttree.common.*;


import com.taobao.tddl.sqlobjecttree.oracle.function.*;

import com.taobao.tddl.sqlobjecttree.common.expression.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


}
@members{



		protected OracleConsistStringRegister consistStr;
		public OracleConsistStringRegister getConsist(){
				return consistStr;
		}
		public void setConsist(OracleConsistStringRegister consist){
				this.consistStr=consist;
		}
		protected  OracleFunctionRegister functionMap;

    	public OracleFunctionRegister getFunc() {
			return functionMap;
		}
		public void setFunc(OracleFunctionRegister funcreg) {
				this.functionMap = funcreg;
		}
		protected OracleHintRegister hintReg;
		public void setOracleHint(OracleHintRegister hintreg){
				this.hintReg=hintreg;
		}
		public boolean mismatchIsMissingToken(IntStream arg0, BitSet follow) {
		if ( follow==null ) {
		throw new IllegalArgumentException("can't know what's wrong...");
		}
		throw new IllegalArgumentException("LT(1)=="+((TokenStream)input).LT(1)+" is consistent with what follows; inserting...viable tokens="+follow.toString(getTokenNames())+"LT(1)="+((TokenStream)input).LT(1));
	}
	public boolean mismatchIsUnwantedToken(IntStream input, int ttype) {
    		throw new IllegalArgumentException(""+ttype);
    	}
}
beg	returns[DMLCommon obj]:
start_rule{$obj=$start_rule.obj;} 	
	;
	
start_rule returns[DMLCommon obj]
	:select_command[null]{$obj=$select_command.select;}
	|insert_command{$obj=$insert_command.ins;}
	|update_command{$obj=$update_command.update;}
	|delete_command{$obj=$delete_command.del;}
	;

setclause[Update update]
	:^(SET updateColumnSpecs[$update]+)	
	;
updateColumnSpecs[Update update]
	:^(SET_ELE updateColumnSpec[$update])	
	;
updateColumnSpec[Update update]
	:^(EQ table_alias? identifier expr[$update.getIndexHolder()])
	{
	$update.addSetElement($identifier.text,$table_alias.aText,$expr.valueObj);
	}
	;


values[Insert ins]	:^(INSERT_VAL (expr[$ins.getIndexHolder()]{ins.addValue($expr.valueObj);})*)
	;
column_specs[Insert ins]
	:^(COLUMNS column_spec[$ins]+)
	;
column_spec[Insert ins]
	:^(COLUMN identifier table_name?)
	{
		$ins.addColumnTandC($table_name.text,$identifier.text);
	}
	;



whereClause[WhereCondition where]
	:^(WHERE sqlCondition[$where])
	;
	
groupByClause[WhereCondition where]
	:^(GROUPBY columnNamesAfterWhere){
		$where.setGroupByColumns($columnNamesAfterWhere.ret);
	}
	;
orderByClause[WhereCondition where]
	:^(ORDERBY columnNamesAfterWhere){
		$where.setOrderByColumns($columnNamesAfterWhere.ret);
	}
	;
columnNamesAfterWhere returns[List<OrderByEle> ret]
@init{
	$ret = new ArrayList<OrderByEle>();
}
	:(columnNameAfterWhere[$ret])+
	;
columnNameAfterWhere[List<OrderByEle> orderByEles]
	:^(ASC  identifier table_alias?){
		$orderByEles.add(new OrderByEle($table_alias.aText,$identifier.text,true));
	}
	|^(DESC  identifier table_alias?){
		$orderByEles.add(new OrderByEle($table_alias.aText,$identifier.text,false));
	}
	;

sqlCondition[WhereCondition where]
	
	:^(CONDITION_OR_NOT condition[where.getHolder(),where.getExpGroup(),false])
	|^(CONDITION_OR condition[where.getHolder(),where.getExpGroup(),false])
	;

condition[BindIndexHolder where,ExpressionGroup eg,boolean isPriority]	



	:
	{
		OrExpressionGroup orExp=new OrExpressionGroup();
		$eg.addExpressionGroup(orExp);
	}^('OR' s1=condition[where,orExp,$isPriority]+)
	| 
	{
		ExpressionGroup andExp=new ExpressionGroup();
		$eg.addExpressionGroup(andExp);
	}^('AND' condition[where,andExp,$isPriority]+)




	|condition_PAREN[where,$eg]
	|^(PRIORITY condition[where,$eg,true])
	;

/*condition_and[OrExpressionGroup orGroup] returns[ExpressionGroup andExpGrp]
	:
	{
		$andExpGrp=new ExpressionGroup();
	}
	 ^('AND' condition_PAREN[$andExpGrp]+)
	 
	|condition_PAREN[$orGroup]
	
	|^('OR' s1=condition_and[orExp] s2=condition_and[orExp])
	
	{
	where.addAndExpression($s1.andExpGrp);
	where.addAndExpression($s2.andExpGrp);
	}
	;
	*/
condition_PAREN[BindIndexHolder where,ExpressionGroup exp]
	:condition_expr[$where,$exp]+
	;
condition_expr[BindIndexHolder where,ExpressionGroup exp]
	: comparisonCondition[$where,$exp]
	| inCondition[$where,$exp] 
	| isCondition [$where,$exp]
	| likeCondition[$where,$exp]
//	| betweenCondition^
	;
betweenCondition
	:^(NOT_BETWEEN between_and)
	|^(BETWEEN between_and)
	;
between_and
	:	
	;
likeCondition[BindIndexHolder where,ExpressionGroup exp]
	:^(NOT_LIKE expr[$where] left_cond[$where]){
			NotLike notlike=new NotLike();
		notlike.setLeft($left_cond.ret);
		notlike.setRight($expr.valueObj);
		$exp.addExpression(notlike);
	}
	|^(LIKE expr[$where] left_cond[$where]){
		Like like=new Like();
		like.setLeft($left_cond.ret);
		like.setRight($expr.valueObj);
		$exp.addExpression(like);
	}
	;
	
isCondition[BindIndexHolder where,ExpressionGroup exp]
	:^(ISNOT NULL left_cond[$where]){
		IsNot arg=new IsNot();
		arg.setLeft($left_cond.ret);
		arg.setRight(null);
		$exp.addExpression(arg);
	}
	|^(IS NULL left_cond[$where]){
		Is arg=new Is();
		arg.setLeft($left_cond.ret);
		arg.setRight(null);
		$exp.addExpression(arg);
	}	
	;


comparisonCondition[BindIndexHolder where,ExpressionGroup exp]
	:^(EQ expr[$where] left_cond[$where])
	{
		Equivalent arg=new Equivalent();
		arg.setLeft($left_cond.ret);
		arg.setRight($expr.valueObj);
		$exp.addExpression(arg);
	}
	|^(NOT_EQ expr[$where] left_cond[$where])
	{
		NotEquivalent arg=new NotEquivalent();
		arg.setLeft($left_cond.ret);
		arg.setRight($expr.valueObj);
		$exp.addExpression(arg);
	}
	|^(LTH expr[$where] left_cond[$where])
	{
		LessThan arg=new LessThan();
		arg.setLeft($left_cond.ret);
		arg.setRight($expr.valueObj);
		$exp.addExpression(arg);
	}
	|^(GTH expr[$where] left_cond[$where])
	{
		GreaterThan arg=new GreaterThan();
		arg.setLeft($left_cond.ret);
		arg.setRight($expr.valueObj);
		$exp.addExpression(arg);
	}
	|^(LEQ expr[$where] left_cond[$where])
	{
		LessThanOrEquivalent arg=new LessThanOrEquivalent();
		arg.setLeft($left_cond.ret);
		arg.setRight($expr.valueObj);
		$exp.addExpression(arg);
	}
	|^(GEQ expr[$where] left_cond[$where])
	{
		GreaterThanOrEquivalent arg=new GreaterThanOrEquivalent();
		arg.setLeft($left_cond.ret);
		arg.setRight($expr.valueObj);
		$exp.addExpression(arg);
	}

	;
left_cond[BindIndexHolder where] returns[Object ret]

	:^(CONDITION_LEFT expr[$where])
	{
		$ret=$expr.valueObj;
	}
	;
in_list[BindIndexHolder where] returns[List list]
	:^(IN_LISTS inCondition_expr_adds[$where])
	{
		$list=$inCondition_expr_adds.list;
	}	
	;
inCondition [BindIndexHolder where,ExpressionGroup exp]
	:^(IN not='NOT'? subquery[$where]? in_list[$where]?  left_cond[$where])
		{
		if($not.text!=null){
			if($subquery.subselect!=null){
				NotInExpression arg=new NotInExpression();
				arg.setLeft($left_cond.ret);
				arg.setRight($subquery.subselect);
				$exp.addExpression(arg);
			}
			else if($in_list.list!=null){
					NotInExpression arg=new NotInExpression();
					arg.setLeft($left_cond.ret);
					arg.setRight($in_list.list);
					$exp.addExpression(arg);
			}
		}else{
			if($subquery.subselect!=null){
				InExpression arg=new InExpression();
				arg.setLeft($left_cond.ret);
				arg.setRight($subquery.subselect);
				$exp.addExpression(arg);}
			else if($in_list.list!=null){
					InExpression arg=new InExpression();
					arg.setLeft($left_cond.ret);
					arg.setRight($in_list.list);
					$exp.addExpression(arg);
			}
		}
		;
		}
		
	;



	
inCondition_expr_adds[BindIndexHolder where] returns [List list]
	@init{$list=new ArrayList();}:
	(expr_add[$where]
	{
		$list.add($expr_add.valueObjExpr);
	})+

	;
	
expr[BindIndexHolder where] returns [Object valueObj]	
	:(expr_add[$where] {$valueObj=$expr_add.valueObjExpr;}
	|subquery[$where] {$valueObj=$subquery.subselect;}
	
	)	
	;
expr_add[BindIndexHolder where]  returns[Object valueObjExpr]
@init{
List<Object> list=new ArrayList<Object>(2);
}
	:^(NEGATIVE s1=expr_add[$where]) {
	$valueObjExpr=((BigDecimal)$s1.valueObjExpr).negate();
	}
	|^(POSITIVE s1=expr_add[$where]) {
	$valueObjExpr=$s1.valueObjExpr;
	}
	|^(PLUS s1=expr_add[$where] s2=expr_add[$where]) {
	Function func=new Add();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(MINUS s1=expr_add[$where] s2=expr_add[$where]){
		Function func=new Subtract();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(BITOR s1=expr_add[$where] s2=expr_add[$where]){
		Function func=new BitOr();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(BITAND s1=expr_add[$where] s2=expr_add[$where]){
		Function func=new BitAnd();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(BITXOR s1=expr_add[$where] s2=expr_add[$where]){
	Function func=new BitXOr();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(SHIFTLEFT s1=expr_add[$where] s2=expr_add[$where]){
	Function func=new ShiftLeft();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(SHIFTRIGHT s1=expr_add[$where] s2=expr_add[$where]){
	Function func=new ShiftRight();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(ASTERISK s1=expr_add[$where] s2=expr_add[$where]){
	Function func=new Multiply();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(DIVIDE s1=expr_add[$where] s2=expr_add[$where]){
	Function func=new Divide();
	list.add($s1.valueObjExpr);
	list.add($s2.valueObjExpr);
	func.setValue(list);
	$valueObjExpr=func;
	}
	|N {$valueObjExpr=new BigDecimal($N.text);}
	|NUMBER{$valueObjExpr=new BigDecimal($NUMBER.text);}
	|boolean_literal//not support true false yet
	|'NULL' {$valueObjExpr=null;}
	|'?'{
	BindVar val=new BindVar(where.selfAddAndGet());
	$valueObjExpr=val;
	}
	|^(QUTED_STR quoted_string){$valueObjExpr=$quoted_string.aText;}
	|^(COLUMN identifier table_name?)
	{
	Column col=new ColumnImp($table_name.text,$identifier.text,null);
	$valueObjExpr=col;
	}
	|^(COLUMNAST ASTERISK)
	{
	Column col=new ColumnImp(null,$ASTERISK.text,null);
	$valueObjExpr=col;
	}
	|^(ID 
	(expr[$where]{list.add($expr.valueObj);})*){
	Function func=functionMap.get(($ID.text).toUpperCase());
	func.setValue(list);
	$valueObjExpr=func;
	}
	|^(CONSIST ID){
	Function func=consistStr.get(($ID.text).toUpperCase());
	$valueObjExpr=func;
	}
	|^(CAST (expr[$where]{list.add($expr.valueObj);})* ID){
	//list.add("vartabletype");
	Cast func= new Cast();
	func.setType($ID.text);
	func.setValue(list);
	$valueObjExpr=func;
	}
	;
value	:
	N
	|NUMBER
	|'?'
	;
boolean_literal returns[Object valueObj]
	:s1='TRUE' {$valueObj=Boolean.parseBoolean($s1.text);}
	| s1='FALSE'{$valueObj=Boolean.parseBoolean($s1.text);}
	;	

	
select_list[Select select]
	:^(SELECT_LIST displayed_column[$select]+)
	;	
fromClause[Select select]
	:^(TABLENAMES table[$select]+)
	;
join_clause[DMLCommon common] returns [JoinClause joinClause]
	:^(JOIN table_name alias? temp1=join_column temp2=join_column join_type?)
	{
		$joinClause = new JoinClause();
		TableNameImp tableName = new TableNameImp();
		tableName.setTablename($table_name.text);
		tableName.setAlias($alias.aliText);
		$joinClause.setTableName(tableName);
		$joinClause.setLeftCondition($temp1.col);
		$joinClause.setRightCondition($temp2.col);
		$joinClause.setJoinType($join_type.joinType);
	}
	;

join_type returns [JOIN_TYPE joinType]
	:INNER
	{
		$joinType = JOIN_TYPE.INNER;
	} 
	|LEFT outer?
	{
		boolean outter = $outer.outter;
		if(outter){
			$joinType = JOIN_TYPE.LEFT_OUTER;
		}else{
			$joinType = JOIN_TYPE.LEFT;
		}
	}
	|RIGHT outer?
	{
		boolean outter = $outer.outter;
		if(outter){
			$joinType = JOIN_TYPE.RIGHT_OUTER;
		}else{
			$joinType = JOIN_TYPE.RIGHT;
		}
	}
	|FULL outer?
		{
		boolean outter = $outer.outter;
		if(outter){
			$joinType = JOIN_TYPE.FULL_OUTER;
		}else{
			$joinType = JOIN_TYPE.FULL;
		}
	}
	|UNION
	{
		$joinType = JOIN_TYPE.UNION;
	} 
	|CROSS
	{
		$joinType = JOIN_TYPE.CROSS;
	} 
	;
outer returns [boolean outter]
	:OUTER
	{
		$outter = true;
	}
	;
join_column returns[Column col]
	:^(COLUMN identifier table_name?)
	    {
		    $col=new ColumnImp($table_name.text,$identifier.text,null);
			
		}
	;
table[DMLCommon common]
	:^(TABLENAME table_spec[$common] join_clause[$common]?)
	{
		TableName tableName = $table_spec.tableName;
		tableName.setJoinClause($join_clause.joinClause);
		$common.addTable(tableName);
	}
	;
tables[DMLCommon common]
	:^(TABLENAMES table[$common]+)
	;
table_spec[DMLCommon common] returns [TableName tableName]
	@init{
	List<Object> list=new ArrayList<Object>(2);
	}
	:( schema_name)? table_name  alias?
	{
		$tableName = getTableNameAndSchemaName($table_name.text,$schema_name.text,$alias.aliText, true);
	}
	|subquery[$common.getIndexHolder()] alias?
	{
		$tableName = getTableSubQuery($subquery.subselect,$alias.aliText, true);

	}
	|^(ID (expr[$common.getIndexHolder()]{list.add($expr.valueObj);}) ) alias?
	{
		Function func=functionMap.get(($ID.text).toUpperCase());
		func.setValue(list);
		$tableName = getTableFunction(func,$alias.aliText, true);
	}
	;
	
schema_name
	:identifier
	;
subquery[BindIndexHolder holder] returns [Select subselect]:
	^(SUBQUERY select_command[$holder])
	{
	$subselect=$select_command.select;
	$subselect.setSubSelect(true);
	}	
	;
table_name
	:identifier
	;	
displayed_column[Select select]
@init{
List<Object> list=new ArrayList<Object>(10);
}
	:
	^(ID  (expr[$select.getIndexHolder()]{list.add($expr.valueObj);} alias? )* ){
	Function func=functionMap.get(($ID.text).toUpperCase());
	func.setValue(list);
	FunctionColumn funcolumn=new FunctionColumn();
	funcolumn.setFunction(func);
	funcolumn.setAlias($alias.aliText);
	$select.addColumn(funcolumn);
	}
	|^(CONSIST ID alias?){
	Function func=consistStr.get(($ID.text).toUpperCase());
	FunctionColumn funcolumn=new FunctionColumn();
	funcolumn.setFunction(func);
	funcolumn.setAlias($alias.aliText);
	$select.addColumn(funcolumn);
	}
	|^(COLUMN table_alias? columnAnt alias?)
	{$select.addColumn($table_alias.aText,$columnAnt.text,$alias.aliText);}

	;    
	
columnAnt returns[String aText]
	:ASTERISK {$aText=$ASTERISK.text;}
	|N {$aText=$N.text;}
	|identifier {$aText=$identifier.text;}
	;

quoted_string returns[String aText]
	: QUOTED_STRING {$aText = $QUOTED_STRING.text.substring(1, $QUOTED_STRING.text.length() - 1);}
	;

identifier
	:ID
   	;
table_alias returns [String aText]
	:^(COL_TAB identifier{$aText=$identifier.text;}	)
	;
	
alias returns [String aliText]
	:^(AS identifier){$aliText=$identifier.text;}
	;
		
/*-------------------------------------------the rules below are Redefined by SubParser 
.For Antlr v3 didn't have any method to reuse rules.So we wrote rules which had been modified by subParser here to make
the refactor easily.-----------------------------*/
selectClause[Select select]
    :^(SELECT indexClause[select]? select_list[$select])
    ;  
	
indexClause[DMLCommon dmlCommon]
	:hints[$dmlCommon]+
	;
	
hints[DMLCommon dmlCommon]
	@init{
	List<String> list=new ArrayList<String>();
	}
	:^(ID  (identifier{list.add(($identifier.text));})*){
	HintSetter hint=hintReg.get(($ID.text).toUpperCase());
	hint.addHint(list);
	dmlCommon.addHint(hint);
	}
	;

select_command[BindIndexHolder holder] returns[Select select]
@init{
if(holder ==null){
	$select =new OracleSelect();
	}
else{
	$select = new OracleSelect($holder);
}

}
     : selectClause[$select] (fromClause[$select])?  (whereClause[$select.getWhere()])? (groupByClause[$select.getWhere()])? (orderByClause[$select.getWhere()])? (for_update[$select])?
     ;
for_update[Select select]
	@init{
		OracleForUpdate update = new OracleForUpdate();
	}
	:^(FORUPDATE of_statement[update]? wait_statement[update]?)
	{
		$select.addForUpdate(update);
	}	
	;
of_statement[OracleForUpdate update]
	:^(OF expr[null])
	{
		$update.setOfColumn((Column)$expr.valueObj);
	}
	;
wait_statement[OracleForUpdate update]
	:^(NOWAIT N?)
	{
		$update.setWait(0);
	}
	|^(WAIT N)
	{
		$update.setWait(Integer.valueOf($N.text));
	}
	;
delete_command returns[Delete del]
@init{$del=new OracleDelete();}
	:^(DELETE indexClause[$del]? tables[$del] whereClause[$del.getWhere()]? orderByClause[$del.getWhere()]?)
	;
update_command returns[Update update]
@init{$update=new OracleUpdate();}
	:^(UPDATE indexClause[$update]? tables[$update] setclause[$update] whereClause[$update.getWhere()]?
	)
	;
	
insert_command returns[Insert ins]
@init{$ins=new Insert();}
	:^(INSERT indexClause[$ins]? tables[$ins] column_specs[$ins]* values[$ins])
	;
