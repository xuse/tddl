grammar HintParser;
options {
	output=AST;
	language=Java;
	ASTLabelType=CommonTree;
}
tokens{
HINT;
EXP_LIST;
EXP;
ARGS;
BIND_VAR;
ARG;
}
@header{
	package com.taobao.tddl.parser.hint;
}

@lexer::header{ package  com.taobao.tddl.parser.hint; } 

hints	:
	DIVIDE! ASTERISK! hint* ASTERISK! DIVIDE!
	;
	
hint	:
	ID COLON value ->^(HINT ID value)
	;
value	:
	exp_list ->^(EXP_LIST exp_list)
	|exp
	;
exp_list:
	'{'! exps '}'!
	;
exps	:
	exp (SEMICOLON! exp)*
	;
exp	:expKey COLON? args?->^(EXP expKey args?)
	;
expKey:
	ID
	|'?'->^(BIND_VAR '?')
	;
args:
	'[' arg (COMMA arg)* ']'  ->^(ARGS arg*)
	|arg 
	;
	
arg 	:
	ID ->^(ARG ID)
	|'?' ->^(BIND_VAR '?')
	;
	
WS  :   (   ' '
		|   '\t'
		|   '\r' '\n' 
		|   '\n'     
		|   '\r'      
		)
		{skip();}  //ignore this token
	;
COLON	:':'	
	;
 ID 
    :	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
    ;
INT :	'0'..'9'+
    ;

FLOAT
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT
    ;
fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

SEMICOLON
	:';'	
	;
ASTERISK
	:'*'
	;
COMMA	:','	
	;
DIVIDE
	:'/'
	;