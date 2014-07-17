grammar OracleParser;
options {
	output=AST;
	language=Java;
	ASTLabelType=CommonTree;
}

tokens{
	ALIAS;
	TABLENAME;
	TABLENAMES;
	SUBQUERY;
	COLUMN;
	AS;
	OUTER;
	SELECT;
	DISPLAYED_COUNT_COLUMN;
	DISPLAYED_COLUMN;
	IN;
	NOT;
	SELECT_LIST;
	QUTED_STR;
	WHERE;
	CONDITION_OR;
	CONDITION_LEFT;
	INNER;
	LEFT;
	RIGHT;
	FULL;
	UNION;
	CROSS;
	IN_LISTS;
	CONDITION_OR_NOT;
	AND;
	OR;
	ISNOT;
	IS;
	NULL;
	NAN;
	INFINITE;
	LIKE;
	NOT_LIKE;
	NOT_BETWEEN;
	BETWEEN;
	GROUPBY;
	ORDERBY;
	INSERT;
	INSERT_VAL;
	PRIORITY;
	COLUMNAST;
	COLUMNS;
	UPDATE;
	SET;
	SET_ELE;
	COL_TAB;
	DELETE;
	CONSIST;
	JOIN;
	FORUPDATE;
	NOWAIT;
	OF;
	WAIT;
	/*-------------------------------------------the rules below are Redefined by SubParser 
.For Antlr v3 didn't have any method to reuse rules.So we wrote rules which had been modified by subParser here to make
the refactor easily.-----------------------------*/
	CAST;
	NEGATIVE;
	POSITIVE;
}
@lexer::header{ package  com.taobao.tddl.parser.oracle; } 
@header{
package com.taobao.tddl.parser.oracle;

import java.util.Map;
import java.util.HashMap;
import com.taobao.tddl.sqlobjecttree.oracle.function.OracleConsistStringRegister;
import com.taobao.tddl.sqlobjecttree.oracle.function.OracleFunctionRegister;
import com.taobao.tddl.sqlobjecttree.oracle.function.OracleHintRegister;
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
	protected void mismatch(IntStream input, int ttype, BitSet follow)
			throws RecognitionException {
		throw new MismatchedTokenException(ttype, input);
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
// Alter code generation so catch-clauses get replace with
// this action.

}
@rulecatch {
catch (RecognitionException e) {
throw e;
}
}


beg	:
start_rule 
	;
	
start_rule
	:select_command
	|update_command
	|insert_command
	|delete_command
	;

	
setclause
	:'SET' updateColumnSpecs->^(SET updateColumnSpecs)
	;
	
updateColumnSpecs
	:updateColumnSpec (COMMA updateColumnSpec)*->^(SET_ELE updateColumnSpec)+
	;
updateColumnSpec
	:columnNameInUpdate EQ^ expr
	;

groupByClause
	:'GRUOP' 'BY' columnNamesAfterWhere->^(GROUPBY columnNamesAfterWhere)    
	;
orderByClause
	:'ORDER' 'BY' columnNamesAfterWhere->^(ORDERBY columnNamesAfterWhere)
//	|'ORDER' 'BY' columnNamesAfterWhere 'ASC'->^(ORDERBY columnNamesAfterWhere ASC)
//	|'ORDER' 'BY' columnNamesAfterWhere 'DESC'->^(ORDERBY columnNamesAfterWhere DESC)
	;
columnNamesAfterWhere
   	:columnNameAfterWhere (COMMA! columnNameAfterWhere)*	
   	;

whereClause
	:'WHERE' sqlCondition->^(WHERE sqlCondition)
	;

sqlCondition
	:'NOT' condition_or->^(CONDITION_OR_NOT  condition_or)
	|condition_or ->^(CONDITION_OR condition_or)
	;


condition_or
	:condition_and ( 'OR'^ condition_and )*
	;

condition_and
	:condition_PAREN ( 'AND'^ condition_PAREN )*
	;
condition_PAREN
	:(LPAREN condition_or RPAREN)=>LPAREN condition_or RPAREN->^(PRIORITY condition_or)
	|condition_expr
	;
condition_expr
	: condition_left^
	(comparisonCondition^
	| inCondition^
	| isCondition^ 
	| likeCondition^
	| betweenCondition^
	 )
	;
condition_left
	:expr->^(CONDITION_LEFT expr)
	;
betweenCondition
	: 'NOT' 'BETWEEN' between_and->^(NOT_BETWEEN between_and)
	| 'BETWEEN' between_and->^(BETWEEN between_and)
	;
	
between_and
	:between_and_expression 'AND'^ between_and_expression	
	;
	
between_and_expression
	:expr_bit
	;
	
isCondition
	: 'IS' 'NOT' condition_is_valobject->^(ISNOT condition_is_valobject)
	|'IS' condition_is_valobject->^(IS condition_is_valobject)
//	|LPAREN columnNameAfterWhere 'IS'^ ( 'NOT' )? condition_is_valobject RPAREN
	;

condition_is_valobject
	: 'NAN' ->NAN
	| 'INFINITE' ->INFINITE
	| 'NULL' ->NULL
	;

inCondition
	:(not='NOT')? 'IN' ( subquery
	|( LPAREN inCondition_expr_bits RPAREN))->^(IN $not? subquery? inCondition_expr_bits?)

	;

likeCondition
	:'NOT''LIKE'  value->^(NOT_LIKE value)
	|'LIKE' value->^(LIKE value)
	;

inCondition_expr_bits:
	expr(COMMA expr)*->^(IN_LISTS expr+)
	;	

identifiers
	:columnNameAfterWhere (COMMA identifier)*
	;

comparisonCondition
	:relational_op expr->^(relational_op expr)
	;
	
expr	:(expr_bit
	| subquery 
	)	
	;
	
subquery:
	LPAREN select_command RPAREN->^(SUBQUERY select_command)	
	;
	
expr_bit
	:expr_add ( ( BITOR^ | BITAND^|BITXOR^|SHIFTLEFT^|SHIFTRIGHT^) (expr_add) )*
	;
expr_add
	:expr_mul ( ( PLUS^ | MINUS^ ) (expr_mul) )*
	;
expr_mul
	:	expr_sign ( ( ASTERISK^ | DIVIDE^ ) expr_sign )*
	;
expr_sign
	:	PLUS expr_pow -> ^(POSITIVE expr_pow)
	|	MINUS expr_pow -> ^(NEGATIVE expr_pow)
	|	expr_pow
	;
expr_pow
	:	expr_expr ( EXPONENT^ expr_expr )*
	;

expr_expr
	:value
	|boolean_literal
	|'NULL'
//	|function
//	|{input.LT(1).getText().toUpperCase().equals("sdf")}? ID LPAREN values_func RPAREN->^(ID values_func)
	|{functionMap.containsKey(input.LT(1).getText().toUpperCase())}? ID (LPAREN values_func? RPAREN)->^(ID values_func?)
	|{consistStr.containsKey(input.LT(1).getText().toUpperCase())}? ID ->^(CONSIST ID)
	|'CAST' LPAREN values_func 'AS' ID RPAREN->^(CAST values_func ID)
	;
/*function
	:'TRUNC' LPAREN values	RPAREN
	;*/
sql_condition
	:	condition_or
	;
relational_op
	:	EQ | LTH | GTH | NOT_EQ | LEQ | GEQ
	;

fromClause
	:'FROM'! selected_table
	;

select_list
	:	displayed_column ( COMMA displayed_column )*->^(SELECT_LIST displayed_column+)
	;
	
displayed_column
	:
	{functionMap.containsKey(input.LT(1).getText().toUpperCase())}? ID ((LPAREN values_func? RPAREN)) alias?->^(ID values_func? alias?)
	|{consistStr.containsKey(input.LT(1).getText().toUpperCase())}? ID alias? ->^(CONSIST ID alias?)
	|table_alias?  column (alias)?->^(COLUMN table_alias? column alias?)
	;
	
columnNameAfterWhere
	:table_alias? identifier  ->^(ASC identifier table_alias?)
	|table_alias? identifier  ASC  ->^(ASC identifier table_alias?)
	|table_alias? identifier  DESC ->^(DESC identifier table_alias?)
	;
	
columnNameInUpdate 
	:table_alias? identifier 
	;
table_alias
	:identifier DOT->^(COL_TAB identifier)	
	;
column
	:ASTERISK
	|{"1".equals(input.LT(1).getText().toUpperCase())}? N
	|identifier
	;
values
	:expr ( COMMA expr )*->^(INSERT_VAL expr*)
	;
values_func
	:expr ( COMMA! expr )*
	;	
value	
	:N
	|NUMBER
	|'?'
	|LPAREN! expr RPAREN!
	|quoted_string ->^(QUTED_STR quoted_string)
	|column_spec
	;


column_specs
	:	column_spec ( COMMA column_spec )*->^(COLUMNS column_spec+)
	;
	
selected_table
	:a_table (COMMA a_table)*->^(TABLENAMES a_table+)
	;
a_table
	:table_spec  alias? join_claus?->^(TABLENAME table_spec alias? join_claus?)
	;
table_spec
	:( schema_name DOT!)? table_name 
	| subquery
	|{functionMap.containsKey(input.LT(1).getText().toUpperCase())}? ID LPAREN values_func RPAREN->^(ID values_func?)
	;
join_claus
	:join_type? 'JOIN' table_spec  alias? 'ON' column_spec EQ column_spec ->^(JOIN table_spec alias? column_spec column_spec join_type?)
	;

join_type
	:'INNER'->INNER
	|'LEFT' outer? ->LEFT outer?
	|'RIGHT' outer? -> RIGHT outer?
	|'FULL' outer? -> FULL outer?
	|'UNION' -> UNION
	|'CROSS' -> CROSS
	;
outer
	:'OUTER' -> OUTER
	;
table_name
	:identifier
	;
	
column_spec
	:(table_name DOT)? identifier ->^(COLUMN identifier table_name?)
	|ASTERISK->^(COLUMNAST ASTERISK)
	;

schema_name
	:identifier
	;
	
boolean_literal
	:	'TRUE' | 'FALSE'
	;
	
alias
	:	( 'AS')? identifier->^(AS identifier)
	;
	
identifier
	:	ID
//	|	DOUBLEQUOTED_STRING //del on 1-21 13:17
   	;
   N
   	: '0' .. '9' ( '0' .. '9' )*
	;  

	ASC:'ASC';
	DESC:'DESC';	
EXPONENT
	:	'**'
	;	
 ID 
    :	('A' .. 'Z'|'a'..'z') ( 'A' .. 'Z'|'a'..'z' | '0' .. '9' | '_' | '$' | '#' )*
//    |	DOUBLEQUOTED_STRING
    ;
PLUS
	:	'+'
	;

NUMBER
	:	
	(( N '.' N )
	|('.' N)
	)
    ;
    
 
MINUS
	:	'-'
	;
	DOT
	:	POINT
	;
	COMMA
	:	','
	;
	EQ
	:	'='
	;
	
DIVIDE
	:	'/'
	;
ASTERISK
	:	'*'
	;
ARROW
	:	'=>'
	;
DOUBLEVERTBAR
	:	'||'
	;
	
fragment
POINT
	:	'.'
	;	
RPAREN
	:	')'
	;
LPAREN
	:	'('
	;
	LTH
	:	'<'
	;
	NOT_EQ
	:	'<>' | '!=' | '^='
	;
	LEQ
	:	'<='
	;
	GEQ
	:	'>='
	;
	GTH
	:	'>'
	;
	
quoted_string
	: QUOTED_STRING
	;	

QUOTED_STRING
	:'\'' (~'\''|'\'\'')* '\''
	;

 fragment
DOUBLEQUOTED_STRING
	:	'"' ( ~('"') )* '"'
	;
	
WS  :   (   ' '
		|   '\t'
		|   '\r' '\n' 
		|   '\n'     
		|   '\r'      
		)
		{$channel=HIDDEN;}  //ignore this token
	;

BITOR
	:'|'
	;
BITAND
	:'&'
	;
BITXOR
	:'^'
	;
SHIFTLEFT
	:'<<'
	;
SHIFTRIGHT
	:'>>'
	;
	

/*-------------------------------------------the rules below are Redefined by SubParser 
.For Antlr v3 didn't have any method to reuse rules.So we wrote rules which had been modified by subParser here to make
the refactor easily.-----------------------------*/

selectClause
    :'SELECT'  indexClause? select_list->^(SELECT  indexClause? select_list)
    ;  
	
select_command
     : selectClause (fromClause)? (whereClause)? groupByClause? (orderByClause)? for_update?
     ;
 indexClause
 	:'/*' '+' hints+ '*/'->hints+
 	;
	
 hints	
 	:{hintReg.containsKey(input.LT(1).getText().toUpperCase())}? 
 	 ID (LPAREN identifier (COMMA? identifier)* RPAREN )?
 	 ->^(ID identifier* )
	;
	
insert_command
	:	'INSERT' indexClause? 'INTO' selected_table
		( LPAREN column_specs  RPAREN )?
		('VALUES' LPAREN values RPAREN
		)->^(INSERT indexClause? selected_table column_specs values)
	;
for_update
	:'FOR' 'UPDATE' of_statement? wait_statement?->^(FORUPDATE of_statement? wait_statement?)
	;
of_statement
	:'OF' expr->^(OF expr)
	;
wait_statement
	:'NOWAIT'->^(NOWAIT)
	|'WAIT' N ->^(WAIT N)
	;

delete_command
	:'DELETE' indexClause? fromClause whereClause? orderByClause?  ->^(DELETE indexClause? fromClause whereClause? orderByClause?) 
	;
update_command
	:'UPDATE' indexClause? selected_table  setclause whereClause?  ->^(UPDATE selected_table setclause whereClause?)
	;
