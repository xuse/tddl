// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\tools\\antlr\\test\\MySQLParser.g 2011-09-13 15:44:51

package com.taobao.tddl.parser.mysql;

import java.util.Map;
import java.util.HashMap;
import com.taobao.tddl.sqlobjecttree.Function;
import com.taobao.tddl.sqlobjecttree.mysql.function.*;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;

public class MySQLParserParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "ALIAS", "TABLENAME", "TABLENAMES", "SUBQUERY", "COLUMN", "AS", "SELECT", "DISTINCT", "DISPLAYED_COUNT_COLUMN", "DISPLAYED_COLUMN", "IN", "NOT", "SELECT_LIST", "QUTED_STR", "WHERE", "CONDITION_OR", "CONDITION_LEFT", "IN_LISTS", "REPLACE", "CONDITION_OR_NOT", "OUTER", "INNER", "LEFT", "RIGHT", "FULL", "JOIN", "UNION", "CROSS", "AND", "OR", "ISNOT", "IS", "NULL", "NAN", "INFINITE", "LIKE", "NOT_LIKE", "NOT_BETWEEN", "BETWEEN", "GROUPBY", "HAVING", "ORDERBY", "INSERT", "INSERT_VAL", "PRIORITY", "COLUMNAST", "COLUMNS", "UPDATE", "SET", "SET_ELE", "COL_TAB", "DELETE", "CONSIST", "UNIT", "SKIP", "RANGE", "SHAREMODE", "FORUPDATE", "NEGATIVE", "POSITIVE", "INTERVAL", "TIME_FUC_NAME", "TIME_FUC", "TIME", "COMMA", "EQ", "LPAREN", "RPAREN", "BITOR", "BITAND", "BITXOR", "SHIFTLEFT", "SHIFTRIGHT", "PLUS", "MINUS", "ASTERISK", "DIVIDE", "EXPONENT", "ID", "N", "LTH", "GTH", "NOT_EQ", "LEQ", "GEQ", "ASC", "DESC", "DOT", "NUMBER", "POINT", "ARROW", "DOUBLEVERTBAR", "QUOTED_STRING", "DOUBLEQUOTED_STRING", "WS", "'SET'", "'INSERT'", "'INTO'", "'VALUES'", "'REPLACE'", "'GROUP'", "'BY'", "'HAVING'", "'ORDER'", "'SELECT'", "'DISTINCT'", "'WHERE'", "'NOT'", "'OR'", "'AND'", "'BETWEEN'", "'IS'", "'NAN'", "'INFINITE'", "'NULL'", "'IN'", "'LIKE'", "'ROWNUM'", "'?'", "'FROM'", "'JOIN'", "'ON'", "'INNER'", "'LEFT'", "'RIGHT'", "'FULL'", "'UNION'", "'CROSS'", "'OUTER'", "'TRUE'", "'FALSE'", "'AS'", "'FORCE'", "'INDEX'", "'IGNORE'", "'DELETE'", "'UPDATE'", "'LIMIT'", "'FOR'", "'LOCK'", "'SHARE'", "'MODE'"
    };
    public static final int COLUMNAST=49;
    public static final int EXPONENT=81;
    public static final int CONDITION_LEFT=20;
    public static final int NOT=15;
    public static final int CONSIST=56;
    public static final int EOF=-1;
    public static final int DISPLAYED_COUNT_COLUMN=12;
    public static final int RPAREN=71;
    public static final int FULL=28;
    public static final int INSERT=46;
    public static final int GEQ=88;
    public static final int EQ=69;
    public static final int SELECT=10;
    public static final int T__99=99;
    public static final int GROUPBY=43;
    public static final int DIVIDE=80;
    public static final int FORUPDATE=61;
    public static final int T__139=139;
    public static final int POSITIVE=63;
    public static final int T__138=138;
    public static final int T__137=137;
    public static final int T__136=136;
    public static final int ASC=89;
    public static final int DISPLAYED_COLUMN=13;
    public static final int N=83;
    public static final int NULL=36;
    public static final int NUMBER=92;
    public static final int CONDITION_OR=19;
    public static final int DELETE=55;
    public static final int DOUBLEVERTBAR=95;
    public static final int TABLENAMES=6;
    public static final int T__141=141;
    public static final int T__142=142;
    public static final int T__140=140;
    public static final int T__145=145;
    public static final int T__143=143;
    public static final int SKIP=58;
    public static final int T__144=144;
    public static final int T__126=126;
    public static final int T__125=125;
    public static final int T__128=128;
    public static final int UNIT=57;
    public static final int T__127=127;
    public static final int WS=98;
    public static final int BITOR=72;
    public static final int T__129=129;
    public static final int PRIORITY=48;
    public static final int SELECT_LIST=16;
    public static final int OR=33;
    public static final int ALIAS=4;
    public static final int T__130=130;
    public static final int DISTINCT=11;
    public static final int T__131=131;
    public static final int T__132=132;
    public static final int T__133=133;
    public static final int T__134=134;
    public static final int NAN=37;
    public static final int T__135=135;
    public static final int WHERE=18;
    public static final int INNER=25;
    public static final int POINT=93;
    public static final int T__118=118;
    public static final int T__119=119;
    public static final int T__116=116;
    public static final int GTH=85;
    public static final int T__117=117;
    public static final int T__114=114;
    public static final int T__115=115;
    public static final int T__124=124;
    public static final int IN_LISTS=21;
    public static final int SHIFTLEFT=75;
    public static final int UPDATE=51;
    public static final int T__123=123;
    public static final int T__122=122;
    public static final int T__121=121;
    public static final int T__120=120;
    public static final int ORDERBY=45;
    public static final int ID=82;
    public static final int AND=32;
    public static final int CROSS=31;
    public static final int BITAND=73;
    public static final int INTERVAL=64;
    public static final int ASTERISK=79;
    public static final int LPAREN=70;
    public static final int SUBQUERY=7;
    public static final int TIME=67;
    public static final int AS=9;
    public static final int IN=14;
    public static final int QUTED_STR=17;
    public static final int T__107=107;
    public static final int COMMA=68;
    public static final int T__108=108;
    public static final int T__109=109;
    public static final int IS=35;
    public static final int REPLACE=22;
    public static final int LEFT=26;
    public static final int T__103=103;
    public static final int SHIFTRIGHT=76;
    public static final int T__104=104;
    public static final int BITXOR=74;
    public static final int T__105=105;
    public static final int T__106=106;
    public static final int COLUMN=8;
    public static final int T__111=111;
    public static final int T__110=110;
    public static final int T__113=113;
    public static final int QUOTED_STRING=96;
    public static final int PLUS=77;
    public static final int T__112=112;
    public static final int DOT=91;
    public static final int SHAREMODE=60;
    public static final int LIKE=39;
    public static final int OUTER=24;
    public static final int TIME_FUC_NAME=65;
    public static final int DOUBLEQUOTED_STRING=97;
    public static final int NEGATIVE=62;
    public static final int NOT_LIKE=40;
    public static final int TIME_FUC=66;
    public static final int RANGE=59;
    public static final int NOT_BETWEEN=41;
    public static final int SET=52;
    public static final int RIGHT=27;
    public static final int INFINITE=38;
    public static final int T__102=102;
    public static final int HAVING=44;
    public static final int INSERT_VAL=47;
    public static final int T__101=101;
    public static final int T__100=100;
    public static final int MINUS=78;
    public static final int CONDITION_OR_NOT=23;
    public static final int JOIN=29;
    public static final int UNION=30;
    public static final int NOT_EQ=86;
    public static final int LTH=84;
    public static final int COLUMNS=50;
    public static final int COL_TAB=54;
    public static final int ARROW=94;
    public static final int SET_ELE=53;
    public static final int ISNOT=34;
    public static final int DESC=90;
    public static final int TABLENAME=5;
    public static final int BETWEEN=42;
    public static final int LEQ=87;

    // delegates
    // delegators


        public MySQLParserParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public MySQLParserParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return MySQLParserParser.tokenNames; }
    public String getGrammarFileName() { return "D:\\tools\\antlr\\test\\MySQLParser.g"; }



    		protected MySQLConsistStringRegister consistStr;
    		public MySQLConsistStringRegister getConsist(){
    				return consistStr;
    		}
    		public void setConsist(MySQLConsistStringRegister consist){
    				this.consistStr=consist;
    		}
    		protected  MySQLFunctionRegister functionMap;
    		
    	

            	public MySQLFunctionRegister getFunc() {
    			return functionMap;
    		}
    		public void setFunc(MySQLFunctionRegister funcreg) {
    				this.functionMap = funcreg;
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



    public static class beg_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "beg"
    // D:\\tools\\antlr\\test\\MySQLParser.g:133:1: beg : start_rule ;
    public final MySQLParserParser.beg_return beg() throws RecognitionException {
        MySQLParserParser.beg_return retval = new MySQLParserParser.beg_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.start_rule_return start_rule1 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:134:9: ( start_rule )
            // D:\\tools\\antlr\\test\\MySQLParser.g:134:10: start_rule
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_start_rule_in_beg332);
            start_rule1=start_rule();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, start_rule1.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "beg"

    public static class start_rule_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "start_rule"
    // D:\\tools\\antlr\\test\\MySQLParser.g:137:1: start_rule : ( select_command | update_command | insert_command | delete_command | replace_command );
    public final MySQLParserParser.start_rule_return start_rule() throws RecognitionException {
        MySQLParserParser.start_rule_return retval = new MySQLParserParser.start_rule_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.select_command_return select_command2 = null;

        MySQLParserParser.update_command_return update_command3 = null;

        MySQLParserParser.insert_command_return insert_command4 = null;

        MySQLParserParser.delete_command_return delete_command5 = null;

        MySQLParserParser.replace_command_return replace_command6 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:138:2: ( select_command | update_command | insert_command | delete_command | replace_command )
            int alt1=5;
            switch ( input.LA(1) ) {
            case 108:
                {
                alt1=1;
                }
                break;
            case 140:
                {
                alt1=2;
                }
                break;
            case 100:
                {
                alt1=3;
                }
                break;
            case 139:
                {
                alt1=4;
                }
                break;
            case 103:
                {
                alt1=5;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }

            switch (alt1) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:138:3: select_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_select_command_in_start_rule344);
                    select_command2=select_command();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, select_command2.getTree());

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:139:3: update_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_update_command_in_start_rule348);
                    update_command3=update_command();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, update_command3.getTree());

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:140:3: insert_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_insert_command_in_start_rule352);
                    insert_command4=insert_command();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, insert_command4.getTree());

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:141:3: delete_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_delete_command_in_start_rule356);
                    delete_command5=delete_command();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, delete_command5.getTree());

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:142:3: replace_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_replace_command_in_start_rule360);
                    replace_command6=replace_command();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, replace_command6.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "start_rule"

    public static class setclause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "setclause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:145:1: setclause : 'SET' updateColumnSpecs -> ^( SET updateColumnSpecs ) ;
    public final MySQLParserParser.setclause_return setclause() throws RecognitionException {
        MySQLParserParser.setclause_return retval = new MySQLParserParser.setclause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal7=null;
        MySQLParserParser.updateColumnSpecs_return updateColumnSpecs8 = null;


        CommonTree string_literal7_tree=null;
        RewriteRuleTokenStream stream_99=new RewriteRuleTokenStream(adaptor,"token 99");
        RewriteRuleSubtreeStream stream_updateColumnSpecs=new RewriteRuleSubtreeStream(adaptor,"rule updateColumnSpecs");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:146:2: ( 'SET' updateColumnSpecs -> ^( SET updateColumnSpecs ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:146:3: 'SET' updateColumnSpecs
            {
            string_literal7=(Token)match(input,99,FOLLOW_99_in_setclause371); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_99.add(string_literal7);

            pushFollow(FOLLOW_updateColumnSpecs_in_setclause373);
            updateColumnSpecs8=updateColumnSpecs();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_updateColumnSpecs.add(updateColumnSpecs8.getTree());


            // AST REWRITE
            // elements: updateColumnSpecs
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 146:26: -> ^( SET updateColumnSpecs )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:146:28: ^( SET updateColumnSpecs )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SET, "SET"), root_1);

                adaptor.addChild(root_1, stream_updateColumnSpecs.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "setclause"

    public static class updateColumnSpecs_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "updateColumnSpecs"
    // D:\\tools\\antlr\\test\\MySQLParser.g:149:1: updateColumnSpecs : updateColumnSpec ( COMMA updateColumnSpec )* -> ( ^( SET_ELE updateColumnSpec ) )+ ;
    public final MySQLParserParser.updateColumnSpecs_return updateColumnSpecs() throws RecognitionException {
        MySQLParserParser.updateColumnSpecs_return retval = new MySQLParserParser.updateColumnSpecs_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA10=null;
        MySQLParserParser.updateColumnSpec_return updateColumnSpec9 = null;

        MySQLParserParser.updateColumnSpec_return updateColumnSpec11 = null;


        CommonTree COMMA10_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_updateColumnSpec=new RewriteRuleSubtreeStream(adaptor,"rule updateColumnSpec");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:150:2: ( updateColumnSpec ( COMMA updateColumnSpec )* -> ( ^( SET_ELE updateColumnSpec ) )+ )
            // D:\\tools\\antlr\\test\\MySQLParser.g:150:3: updateColumnSpec ( COMMA updateColumnSpec )*
            {
            pushFollow(FOLLOW_updateColumnSpec_in_updateColumnSpecs390);
            updateColumnSpec9=updateColumnSpec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_updateColumnSpec.add(updateColumnSpec9.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:150:20: ( COMMA updateColumnSpec )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( (LA2_0==COMMA) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:150:21: COMMA updateColumnSpec
            	    {
            	    COMMA10=(Token)match(input,COMMA,FOLLOW_COMMA_in_updateColumnSpecs393); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA10);

            	    pushFollow(FOLLOW_updateColumnSpec_in_updateColumnSpecs395);
            	    updateColumnSpec11=updateColumnSpec();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_updateColumnSpec.add(updateColumnSpec11.getTree());

            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);



            // AST REWRITE
            // elements: updateColumnSpec
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 150:45: -> ( ^( SET_ELE updateColumnSpec ) )+
            {
                if ( !(stream_updateColumnSpec.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_updateColumnSpec.hasNext() ) {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:150:47: ^( SET_ELE updateColumnSpec )
                    {
                    CommonTree root_1 = (CommonTree)adaptor.nil();
                    root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SET_ELE, "SET_ELE"), root_1);

                    adaptor.addChild(root_1, stream_updateColumnSpec.nextTree());

                    adaptor.addChild(root_0, root_1);
                    }

                }
                stream_updateColumnSpec.reset();

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "updateColumnSpecs"

    public static class updateColumnSpec_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "updateColumnSpec"
    // D:\\tools\\antlr\\test\\MySQLParser.g:153:1: updateColumnSpec : columnNameInUpdate EQ expr ;
    public final MySQLParserParser.updateColumnSpec_return updateColumnSpec() throws RecognitionException {
        MySQLParserParser.updateColumnSpec_return retval = new MySQLParserParser.updateColumnSpec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token EQ13=null;
        MySQLParserParser.columnNameInUpdate_return columnNameInUpdate12 = null;

        MySQLParserParser.expr_return expr14 = null;


        CommonTree EQ13_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:154:2: ( columnNameInUpdate EQ expr )
            // D:\\tools\\antlr\\test\\MySQLParser.g:154:3: columnNameInUpdate EQ expr
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_columnNameInUpdate_in_updateColumnSpec415);
            columnNameInUpdate12=columnNameInUpdate();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, columnNameInUpdate12.getTree());
            EQ13=(Token)match(input,EQ,FOLLOW_EQ_in_updateColumnSpec417); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            EQ13_tree = (CommonTree)adaptor.create(EQ13);
            root_0 = (CommonTree)adaptor.becomeRoot(EQ13_tree, root_0);
            }
            pushFollow(FOLLOW_expr_in_updateColumnSpec420);
            expr14=expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expr14.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "updateColumnSpec"

    public static class insert_command_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "insert_command"
    // D:\\tools\\antlr\\test\\MySQLParser.g:157:1: insert_command : 'INSERT' 'INTO' selected_table ( LPAREN column_specs RPAREN )? ( 'VALUES' LPAREN values RPAREN ) -> ^( INSERT selected_table column_specs values ) ;
    public final MySQLParserParser.insert_command_return insert_command() throws RecognitionException {
        MySQLParserParser.insert_command_return retval = new MySQLParserParser.insert_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal15=null;
        Token string_literal16=null;
        Token LPAREN18=null;
        Token RPAREN20=null;
        Token string_literal21=null;
        Token LPAREN22=null;
        Token RPAREN24=null;
        MySQLParserParser.selected_table_return selected_table17 = null;

        MySQLParserParser.column_specs_return column_specs19 = null;

        MySQLParserParser.values_return values23 = null;


        CommonTree string_literal15_tree=null;
        CommonTree string_literal16_tree=null;
        CommonTree LPAREN18_tree=null;
        CommonTree RPAREN20_tree=null;
        CommonTree string_literal21_tree=null;
        CommonTree LPAREN22_tree=null;
        CommonTree RPAREN24_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_102=new RewriteRuleTokenStream(adaptor,"token 102");
        RewriteRuleTokenStream stream_101=new RewriteRuleTokenStream(adaptor,"token 101");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_100=new RewriteRuleTokenStream(adaptor,"token 100");
        RewriteRuleSubtreeStream stream_selected_table=new RewriteRuleSubtreeStream(adaptor,"rule selected_table");
        RewriteRuleSubtreeStream stream_values=new RewriteRuleSubtreeStream(adaptor,"rule values");
        RewriteRuleSubtreeStream stream_column_specs=new RewriteRuleSubtreeStream(adaptor,"rule column_specs");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:158:2: ( 'INSERT' 'INTO' selected_table ( LPAREN column_specs RPAREN )? ( 'VALUES' LPAREN values RPAREN ) -> ^( INSERT selected_table column_specs values ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:158:3: 'INSERT' 'INTO' selected_table ( LPAREN column_specs RPAREN )? ( 'VALUES' LPAREN values RPAREN )
            {
            string_literal15=(Token)match(input,100,FOLLOW_100_in_insert_command431); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_100.add(string_literal15);

            string_literal16=(Token)match(input,101,FOLLOW_101_in_insert_command433); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_101.add(string_literal16);

            pushFollow(FOLLOW_selected_table_in_insert_command435);
            selected_table17=selected_table();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_selected_table.add(selected_table17.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:159:2: ( LPAREN column_specs RPAREN )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==LPAREN) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:159:4: LPAREN column_specs RPAREN
                    {
                    LPAREN18=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_insert_command440); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN18);

                    pushFollow(FOLLOW_column_specs_in_insert_command442);
                    column_specs19=column_specs();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_column_specs.add(column_specs19.getTree());
                    RPAREN20=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_insert_command445); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN20);


                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:160:2: ( 'VALUES' LPAREN values RPAREN )
            // D:\\tools\\antlr\\test\\MySQLParser.g:160:3: 'VALUES' LPAREN values RPAREN
            {
            string_literal21=(Token)match(input,102,FOLLOW_102_in_insert_command452); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_102.add(string_literal21);

            LPAREN22=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_insert_command454); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN22);

            pushFollow(FOLLOW_values_in_insert_command456);
            values23=values();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_values.add(values23.getTree());
            RPAREN24=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_insert_command458); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN24);


            }



            // AST REWRITE
            // elements: values, column_specs, selected_table
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 160:33: -> ^( INSERT selected_table column_specs values )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:160:35: ^( INSERT selected_table column_specs values )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(INSERT, "INSERT"), root_1);

                adaptor.addChild(root_1, stream_selected_table.nextTree());
                adaptor.addChild(root_1, stream_column_specs.nextTree());
                adaptor.addChild(root_1, stream_values.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "insert_command"

    public static class replace_command_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "replace_command"
    // D:\\tools\\antlr\\test\\MySQLParser.g:163:1: replace_command : 'REPLACE' 'INTO' selected_table ( LPAREN column_specs RPAREN )? ( 'VALUES' LPAREN values RPAREN ) -> ^( INSERT selected_table column_specs values ) ;
    public final MySQLParserParser.replace_command_return replace_command() throws RecognitionException {
        MySQLParserParser.replace_command_return retval = new MySQLParserParser.replace_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal25=null;
        Token string_literal26=null;
        Token LPAREN28=null;
        Token RPAREN30=null;
        Token string_literal31=null;
        Token LPAREN32=null;
        Token RPAREN34=null;
        MySQLParserParser.selected_table_return selected_table27 = null;

        MySQLParserParser.column_specs_return column_specs29 = null;

        MySQLParserParser.values_return values33 = null;


        CommonTree string_literal25_tree=null;
        CommonTree string_literal26_tree=null;
        CommonTree LPAREN28_tree=null;
        CommonTree RPAREN30_tree=null;
        CommonTree string_literal31_tree=null;
        CommonTree LPAREN32_tree=null;
        CommonTree RPAREN34_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_103=new RewriteRuleTokenStream(adaptor,"token 103");
        RewriteRuleTokenStream stream_102=new RewriteRuleTokenStream(adaptor,"token 102");
        RewriteRuleTokenStream stream_101=new RewriteRuleTokenStream(adaptor,"token 101");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_selected_table=new RewriteRuleSubtreeStream(adaptor,"rule selected_table");
        RewriteRuleSubtreeStream stream_values=new RewriteRuleSubtreeStream(adaptor,"rule values");
        RewriteRuleSubtreeStream stream_column_specs=new RewriteRuleSubtreeStream(adaptor,"rule column_specs");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:164:2: ( 'REPLACE' 'INTO' selected_table ( LPAREN column_specs RPAREN )? ( 'VALUES' LPAREN values RPAREN ) -> ^( INSERT selected_table column_specs values ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:164:3: 'REPLACE' 'INTO' selected_table ( LPAREN column_specs RPAREN )? ( 'VALUES' LPAREN values RPAREN )
            {
            string_literal25=(Token)match(input,103,FOLLOW_103_in_replace_command480); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_103.add(string_literal25);

            string_literal26=(Token)match(input,101,FOLLOW_101_in_replace_command482); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_101.add(string_literal26);

            pushFollow(FOLLOW_selected_table_in_replace_command484);
            selected_table27=selected_table();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_selected_table.add(selected_table27.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:165:2: ( LPAREN column_specs RPAREN )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==LPAREN) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:165:4: LPAREN column_specs RPAREN
                    {
                    LPAREN28=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_replace_command489); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN28);

                    pushFollow(FOLLOW_column_specs_in_replace_command491);
                    column_specs29=column_specs();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_column_specs.add(column_specs29.getTree());
                    RPAREN30=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_replace_command494); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN30);


                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:166:2: ( 'VALUES' LPAREN values RPAREN )
            // D:\\tools\\antlr\\test\\MySQLParser.g:166:3: 'VALUES' LPAREN values RPAREN
            {
            string_literal31=(Token)match(input,102,FOLLOW_102_in_replace_command501); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_102.add(string_literal31);

            LPAREN32=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_replace_command503); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN32);

            pushFollow(FOLLOW_values_in_replace_command505);
            values33=values();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_values.add(values33.getTree());
            RPAREN34=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_replace_command507); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN34);


            }



            // AST REWRITE
            // elements: column_specs, selected_table, values
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 166:33: -> ^( INSERT selected_table column_specs values )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:166:35: ^( INSERT selected_table column_specs values )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(INSERT, "INSERT"), root_1);

                adaptor.addChild(root_1, stream_selected_table.nextTree());
                adaptor.addChild(root_1, stream_column_specs.nextTree());
                adaptor.addChild(root_1, stream_values.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "replace_command"

    public static class groupByClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "groupByClause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:169:1: groupByClause : 'GROUP' 'BY' columnNamesAfterWhere -> ^( GROUPBY columnNamesAfterWhere ) ;
    public final MySQLParserParser.groupByClause_return groupByClause() throws RecognitionException {
        MySQLParserParser.groupByClause_return retval = new MySQLParserParser.groupByClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal35=null;
        Token string_literal36=null;
        MySQLParserParser.columnNamesAfterWhere_return columnNamesAfterWhere37 = null;


        CommonTree string_literal35_tree=null;
        CommonTree string_literal36_tree=null;
        RewriteRuleTokenStream stream_105=new RewriteRuleTokenStream(adaptor,"token 105");
        RewriteRuleTokenStream stream_104=new RewriteRuleTokenStream(adaptor,"token 104");
        RewriteRuleSubtreeStream stream_columnNamesAfterWhere=new RewriteRuleSubtreeStream(adaptor,"rule columnNamesAfterWhere");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:170:2: ( 'GROUP' 'BY' columnNamesAfterWhere -> ^( GROUPBY columnNamesAfterWhere ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:170:3: 'GROUP' 'BY' columnNamesAfterWhere
            {
            string_literal35=(Token)match(input,104,FOLLOW_104_in_groupByClause528); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_104.add(string_literal35);

            string_literal36=(Token)match(input,105,FOLLOW_105_in_groupByClause530); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_105.add(string_literal36);

            pushFollow(FOLLOW_columnNamesAfterWhere_in_groupByClause532);
            columnNamesAfterWhere37=columnNamesAfterWhere();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnNamesAfterWhere.add(columnNamesAfterWhere37.getTree());


            // AST REWRITE
            // elements: columnNamesAfterWhere
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 170:37: -> ^( GROUPBY columnNamesAfterWhere )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:170:39: ^( GROUPBY columnNamesAfterWhere )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(GROUPBY, "GROUPBY"), root_1);

                adaptor.addChild(root_1, stream_columnNamesAfterWhere.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "groupByClause"

    public static class havingClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "havingClause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:174:1: havingClause : 'HAVING' condition_expr -> ^( HAVING condition_expr ) ;
    public final MySQLParserParser.havingClause_return havingClause() throws RecognitionException {
        MySQLParserParser.havingClause_return retval = new MySQLParserParser.havingClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal38=null;
        MySQLParserParser.condition_expr_return condition_expr39 = null;


        CommonTree string_literal38_tree=null;
        RewriteRuleTokenStream stream_106=new RewriteRuleTokenStream(adaptor,"token 106");
        RewriteRuleSubtreeStream stream_condition_expr=new RewriteRuleSubtreeStream(adaptor,"rule condition_expr");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:175:9: ( 'HAVING' condition_expr -> ^( HAVING condition_expr ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:175:10: 'HAVING' condition_expr
            {
            string_literal38=(Token)match(input,106,FOLLOW_106_in_havingClause556); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_106.add(string_literal38);

            pushFollow(FOLLOW_condition_expr_in_havingClause558);
            condition_expr39=condition_expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_condition_expr.add(condition_expr39.getTree());


            // AST REWRITE
            // elements: condition_expr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 175:33: -> ^( HAVING condition_expr )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:175:35: ^( HAVING condition_expr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(HAVING, "HAVING"), root_1);

                adaptor.addChild(root_1, stream_condition_expr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "havingClause"

    public static class orderByClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orderByClause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:178:1: orderByClause : 'ORDER' 'BY' columnNamesAfterWhere -> ^( ORDERBY columnNamesAfterWhere ) ;
    public final MySQLParserParser.orderByClause_return orderByClause() throws RecognitionException {
        MySQLParserParser.orderByClause_return retval = new MySQLParserParser.orderByClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal40=null;
        Token string_literal41=null;
        MySQLParserParser.columnNamesAfterWhere_return columnNamesAfterWhere42 = null;


        CommonTree string_literal40_tree=null;
        CommonTree string_literal41_tree=null;
        RewriteRuleTokenStream stream_107=new RewriteRuleTokenStream(adaptor,"token 107");
        RewriteRuleTokenStream stream_105=new RewriteRuleTokenStream(adaptor,"token 105");
        RewriteRuleSubtreeStream stream_columnNamesAfterWhere=new RewriteRuleSubtreeStream(adaptor,"rule columnNamesAfterWhere");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:179:2: ( 'ORDER' 'BY' columnNamesAfterWhere -> ^( ORDERBY columnNamesAfterWhere ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:179:3: 'ORDER' 'BY' columnNamesAfterWhere
            {
            string_literal40=(Token)match(input,107,FOLLOW_107_in_orderByClause581); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_107.add(string_literal40);

            string_literal41=(Token)match(input,105,FOLLOW_105_in_orderByClause583); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_105.add(string_literal41);

            pushFollow(FOLLOW_columnNamesAfterWhere_in_orderByClause585);
            columnNamesAfterWhere42=columnNamesAfterWhere();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnNamesAfterWhere.add(columnNamesAfterWhere42.getTree());


            // AST REWRITE
            // elements: columnNamesAfterWhere
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 179:37: -> ^( ORDERBY columnNamesAfterWhere )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:179:39: ^( ORDERBY columnNamesAfterWhere )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ORDERBY, "ORDERBY"), root_1);

                adaptor.addChild(root_1, stream_columnNamesAfterWhere.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "orderByClause"

    public static class columnNamesAfterWhere_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnNamesAfterWhere"
    // D:\\tools\\antlr\\test\\MySQLParser.g:184:1: columnNamesAfterWhere : columnNameAfterWhere ( COMMA columnNameAfterWhere )* ;
    public final MySQLParserParser.columnNamesAfterWhere_return columnNamesAfterWhere() throws RecognitionException {
        MySQLParserParser.columnNamesAfterWhere_return retval = new MySQLParserParser.columnNamesAfterWhere_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA44=null;
        MySQLParserParser.columnNameAfterWhere_return columnNameAfterWhere43 = null;

        MySQLParserParser.columnNameAfterWhere_return columnNameAfterWhere45 = null;


        CommonTree COMMA44_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:185:5: ( columnNameAfterWhere ( COMMA columnNameAfterWhere )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:185:6: columnNameAfterWhere ( COMMA columnNameAfterWhere )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_columnNameAfterWhere_in_columnNamesAfterWhere607);
            columnNameAfterWhere43=columnNameAfterWhere();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, columnNameAfterWhere43.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:185:27: ( COMMA columnNameAfterWhere )*
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( (LA5_0==COMMA) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:185:28: COMMA columnNameAfterWhere
            	    {
            	    COMMA44=(Token)match(input,COMMA,FOLLOW_COMMA_in_columnNamesAfterWhere610); if (state.failed) return retval;
            	    pushFollow(FOLLOW_columnNameAfterWhere_in_columnNamesAfterWhere613);
            	    columnNameAfterWhere45=columnNameAfterWhere();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, columnNameAfterWhere45.getTree());

            	    }
            	    break;

            	default :
            	    break loop5;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnNamesAfterWhere"

    public static class selectClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "selectClause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:188:1: selectClause : 'SELECT' select_list -> ^( SELECT select_list ) ;
    public final MySQLParserParser.selectClause_return selectClause() throws RecognitionException {
        MySQLParserParser.selectClause_return retval = new MySQLParserParser.selectClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal46=null;
        MySQLParserParser.select_list_return select_list47 = null;


        CommonTree string_literal46_tree=null;
        RewriteRuleTokenStream stream_108=new RewriteRuleTokenStream(adaptor,"token 108");
        RewriteRuleSubtreeStream stream_select_list=new RewriteRuleSubtreeStream(adaptor,"rule select_list");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:189:9: ( 'SELECT' select_list -> ^( SELECT select_list ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:189:10: 'SELECT' select_list
            {
            string_literal46=(Token)match(input,108,FOLLOW_108_in_selectClause640); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_108.add(string_literal46);

            pushFollow(FOLLOW_select_list_in_selectClause643);
            select_list47=select_list();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_select_list.add(select_list47.getTree());


            // AST REWRITE
            // elements: select_list
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 189:31: -> ^( SELECT select_list )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:189:33: ^( SELECT select_list )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SELECT, "SELECT"), root_1);

                adaptor.addChild(root_1, stream_select_list.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "selectClause"

    public static class distinct_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "distinct"
    // D:\\tools\\antlr\\test\\MySQLParser.g:192:1: distinct : 'DISTINCT' -> DISTINCT ;
    public final MySQLParserParser.distinct_return distinct() throws RecognitionException {
        MySQLParserParser.distinct_return retval = new MySQLParserParser.distinct_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal48=null;

        CommonTree string_literal48_tree=null;
        RewriteRuleTokenStream stream_109=new RewriteRuleTokenStream(adaptor,"token 109");

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:193:6: ( 'DISTINCT' -> DISTINCT )
            // D:\\tools\\antlr\\test\\MySQLParser.g:193:8: 'DISTINCT'
            {
            string_literal48=(Token)match(input,109,FOLLOW_109_in_distinct678); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_109.add(string_literal48);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 193:18: -> DISTINCT
            {
                adaptor.addChild(root_0, (CommonTree)adaptor.create(DISTINCT, "DISTINCT"));

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "distinct"

    public static class whereClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "whereClause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:196:1: whereClause : 'WHERE' sqlCondition -> ^( WHERE sqlCondition ) ;
    public final MySQLParserParser.whereClause_return whereClause() throws RecognitionException {
        MySQLParserParser.whereClause_return retval = new MySQLParserParser.whereClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal49=null;
        MySQLParserParser.sqlCondition_return sqlCondition50 = null;


        CommonTree string_literal49_tree=null;
        RewriteRuleTokenStream stream_110=new RewriteRuleTokenStream(adaptor,"token 110");
        RewriteRuleSubtreeStream stream_sqlCondition=new RewriteRuleSubtreeStream(adaptor,"rule sqlCondition");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:197:2: ( 'WHERE' sqlCondition -> ^( WHERE sqlCondition ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:197:3: 'WHERE' sqlCondition
            {
            string_literal49=(Token)match(input,110,FOLLOW_110_in_whereClause699); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_110.add(string_literal49);

            pushFollow(FOLLOW_sqlCondition_in_whereClause701);
            sqlCondition50=sqlCondition();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_sqlCondition.add(sqlCondition50.getTree());


            // AST REWRITE
            // elements: sqlCondition
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 197:23: -> ^( WHERE sqlCondition )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:197:25: ^( WHERE sqlCondition )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(WHERE, "WHERE"), root_1);

                adaptor.addChild(root_1, stream_sqlCondition.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "whereClause"

    public static class sqlCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "sqlCondition"
    // D:\\tools\\antlr\\test\\MySQLParser.g:200:1: sqlCondition : ( 'NOT' condition_or -> ^( CONDITION_OR_NOT condition_or ) | condition_or -> ^( CONDITION_OR condition_or ) );
    public final MySQLParserParser.sqlCondition_return sqlCondition() throws RecognitionException {
        MySQLParserParser.sqlCondition_return retval = new MySQLParserParser.sqlCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal51=null;
        MySQLParserParser.condition_or_return condition_or52 = null;

        MySQLParserParser.condition_or_return condition_or53 = null;


        CommonTree string_literal51_tree=null;
        RewriteRuleTokenStream stream_111=new RewriteRuleTokenStream(adaptor,"token 111");
        RewriteRuleSubtreeStream stream_condition_or=new RewriteRuleSubtreeStream(adaptor,"rule condition_or");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:201:2: ( 'NOT' condition_or -> ^( CONDITION_OR_NOT condition_or ) | condition_or -> ^( CONDITION_OR condition_or ) )
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==111) ) {
                alt6=1;
            }
            else if ( (LA6_0==INTERVAL||LA6_0==LPAREN||(LA6_0>=PLUS && LA6_0<=ASTERISK)||(LA6_0>=ID && LA6_0<=N)||LA6_0==NUMBER||LA6_0==QUOTED_STRING||LA6_0==118||(LA6_0>=121 && LA6_0<=122)||(LA6_0>=133 && LA6_0<=134)) ) {
                alt6=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;
            }
            switch (alt6) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:201:3: 'NOT' condition_or
                    {
                    string_literal51=(Token)match(input,111,FOLLOW_111_in_sqlCondition717); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_111.add(string_literal51);

                    pushFollow(FOLLOW_condition_or_in_sqlCondition719);
                    condition_or52=condition_or();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_condition_or.add(condition_or52.getTree());


                    // AST REWRITE
                    // elements: condition_or
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 201:21: -> ^( CONDITION_OR_NOT condition_or )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:201:23: ^( CONDITION_OR_NOT condition_or )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(CONDITION_OR_NOT, "CONDITION_OR_NOT"), root_1);

                        adaptor.addChild(root_1, stream_condition_or.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:202:3: condition_or
                    {
                    pushFollow(FOLLOW_condition_or_in_sqlCondition730);
                    condition_or53=condition_or();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_condition_or.add(condition_or53.getTree());


                    // AST REWRITE
                    // elements: condition_or
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 202:16: -> ^( CONDITION_OR condition_or )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:202:18: ^( CONDITION_OR condition_or )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(CONDITION_OR, "CONDITION_OR"), root_1);

                        adaptor.addChild(root_1, stream_condition_or.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "sqlCondition"

    public static class condition_or_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition_or"
    // D:\\tools\\antlr\\test\\MySQLParser.g:205:1: condition_or : condition_and ( 'OR' condition_and )* ;
    public final MySQLParserParser.condition_or_return condition_or() throws RecognitionException {
        MySQLParserParser.condition_or_return retval = new MySQLParserParser.condition_or_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal55=null;
        MySQLParserParser.condition_and_return condition_and54 = null;

        MySQLParserParser.condition_and_return condition_and56 = null;


        CommonTree string_literal55_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:206:2: ( condition_and ( 'OR' condition_and )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:206:3: condition_and ( 'OR' condition_and )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_condition_and_in_condition_or747);
            condition_and54=condition_and();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_and54.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:206:17: ( 'OR' condition_and )*
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( (LA7_0==112) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:206:19: 'OR' condition_and
            	    {
            	    string_literal55=(Token)match(input,112,FOLLOW_112_in_condition_or751); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    string_literal55_tree = (CommonTree)adaptor.create(string_literal55);
            	    root_0 = (CommonTree)adaptor.becomeRoot(string_literal55_tree, root_0);
            	    }
            	    pushFollow(FOLLOW_condition_and_in_condition_or754);
            	    condition_and56=condition_and();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_and56.getTree());

            	    }
            	    break;

            	default :
            	    break loop7;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition_or"

    public static class condition_and_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition_and"
    // D:\\tools\\antlr\\test\\MySQLParser.g:209:1: condition_and : condition_PAREN ( 'AND' condition_PAREN )* ;
    public final MySQLParserParser.condition_and_return condition_and() throws RecognitionException {
        MySQLParserParser.condition_and_return retval = new MySQLParserParser.condition_and_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal58=null;
        MySQLParserParser.condition_PAREN_return condition_PAREN57 = null;

        MySQLParserParser.condition_PAREN_return condition_PAREN59 = null;


        CommonTree string_literal58_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:210:2: ( condition_PAREN ( 'AND' condition_PAREN )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:210:3: condition_PAREN ( 'AND' condition_PAREN )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_condition_PAREN_in_condition_and767);
            condition_PAREN57=condition_PAREN();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_PAREN57.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:210:19: ( 'AND' condition_PAREN )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==113) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:210:21: 'AND' condition_PAREN
            	    {
            	    string_literal58=(Token)match(input,113,FOLLOW_113_in_condition_and771); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    string_literal58_tree = (CommonTree)adaptor.create(string_literal58);
            	    root_0 = (CommonTree)adaptor.becomeRoot(string_literal58_tree, root_0);
            	    }
            	    pushFollow(FOLLOW_condition_PAREN_in_condition_and774);
            	    condition_PAREN59=condition_PAREN();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_PAREN59.getTree());

            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition_and"

    public static class condition_PAREN_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition_PAREN"
    // D:\\tools\\antlr\\test\\MySQLParser.g:213:1: condition_PAREN : ( ( LPAREN condition_or RPAREN )=> LPAREN condition_or RPAREN -> ^( PRIORITY condition_or ) | condition_expr );
    public final MySQLParserParser.condition_PAREN_return condition_PAREN() throws RecognitionException {
        MySQLParserParser.condition_PAREN_return retval = new MySQLParserParser.condition_PAREN_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LPAREN60=null;
        Token RPAREN62=null;
        MySQLParserParser.condition_or_return condition_or61 = null;

        MySQLParserParser.condition_expr_return condition_expr63 = null;


        CommonTree LPAREN60_tree=null;
        CommonTree RPAREN62_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_condition_or=new RewriteRuleSubtreeStream(adaptor,"rule condition_or");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:214:2: ( ( LPAREN condition_or RPAREN )=> LPAREN condition_or RPAREN -> ^( PRIORITY condition_or ) | condition_expr )
            int alt9=2;
            alt9 = dfa9.predict(input);
            switch (alt9) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:214:3: ( LPAREN condition_or RPAREN )=> LPAREN condition_or RPAREN
                    {
                    LPAREN60=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_condition_PAREN796); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN60);

                    pushFollow(FOLLOW_condition_or_in_condition_PAREN798);
                    condition_or61=condition_or();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_condition_or.add(condition_or61.getTree());
                    RPAREN62=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_condition_PAREN800); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN62);



                    // AST REWRITE
                    // elements: condition_or
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 214:59: -> ^( PRIORITY condition_or )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:214:61: ^( PRIORITY condition_or )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(PRIORITY, "PRIORITY"), root_1);

                        adaptor.addChild(root_1, stream_condition_or.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:215:3: condition_expr
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_condition_expr_in_condition_PAREN810);
                    condition_expr63=condition_expr();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_expr63.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition_PAREN"

    public static class condition_expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition_expr"
    // D:\\tools\\antlr\\test\\MySQLParser.g:218:1: condition_expr : condition_left ( comparisonCondition | inCondition | isCondition | likeCondition | betweenCondition ) ;
    public final MySQLParserParser.condition_expr_return condition_expr() throws RecognitionException {
        MySQLParserParser.condition_expr_return retval = new MySQLParserParser.condition_expr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.condition_left_return condition_left64 = null;

        MySQLParserParser.comparisonCondition_return comparisonCondition65 = null;

        MySQLParserParser.inCondition_return inCondition66 = null;

        MySQLParserParser.isCondition_return isCondition67 = null;

        MySQLParserParser.likeCondition_return likeCondition68 = null;

        MySQLParserParser.betweenCondition_return betweenCondition69 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:219:2: ( condition_left ( comparisonCondition | inCondition | isCondition | likeCondition | betweenCondition ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:219:4: condition_left ( comparisonCondition | inCondition | isCondition | likeCondition | betweenCondition )
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_condition_left_in_condition_expr822);
            condition_left64=condition_left();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) root_0 = (CommonTree)adaptor.becomeRoot(condition_left64.getTree(), root_0);
            // D:\\tools\\antlr\\test\\MySQLParser.g:220:2: ( comparisonCondition | inCondition | isCondition | likeCondition | betweenCondition )
            int alt10=5;
            switch ( input.LA(1) ) {
            case EQ:
            case LTH:
            case GTH:
            case NOT_EQ:
            case LEQ:
            case GEQ:
                {
                alt10=1;
                }
                break;
            case 111:
                {
                switch ( input.LA(2) ) {
                case 120:
                    {
                    alt10=4;
                    }
                    break;
                case 114:
                    {
                    alt10=5;
                    }
                    break;
                case 119:
                    {
                    alt10=2;
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 2, input);

                    throw nvae;
                }

                }
                break;
            case 119:
                {
                alt10=2;
                }
                break;
            case 115:
                {
                alt10=3;
                }
                break;
            case 120:
                {
                alt10=4;
                }
                break;
            case 114:
                {
                alt10=5;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;
            }

            switch (alt10) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:220:3: comparisonCondition
                    {
                    pushFollow(FOLLOW_comparisonCondition_in_condition_expr827);
                    comparisonCondition65=comparisonCondition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) root_0 = (CommonTree)adaptor.becomeRoot(comparisonCondition65.getTree(), root_0);

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:221:4: inCondition
                    {
                    pushFollow(FOLLOW_inCondition_in_condition_expr833);
                    inCondition66=inCondition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) root_0 = (CommonTree)adaptor.becomeRoot(inCondition66.getTree(), root_0);

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:222:4: isCondition
                    {
                    pushFollow(FOLLOW_isCondition_in_condition_expr839);
                    isCondition67=isCondition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) root_0 = (CommonTree)adaptor.becomeRoot(isCondition67.getTree(), root_0);

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:223:4: likeCondition
                    {
                    pushFollow(FOLLOW_likeCondition_in_condition_expr846);
                    likeCondition68=likeCondition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) root_0 = (CommonTree)adaptor.becomeRoot(likeCondition68.getTree(), root_0);

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:224:4: betweenCondition
                    {
                    pushFollow(FOLLOW_betweenCondition_in_condition_expr852);
                    betweenCondition69=betweenCondition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) root_0 = (CommonTree)adaptor.becomeRoot(betweenCondition69.getTree(), root_0);

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition_expr"

    public static class condition_left_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition_left"
    // D:\\tools\\antlr\\test\\MySQLParser.g:228:1: condition_left : expr -> ^( CONDITION_LEFT expr ) ;
    public final MySQLParserParser.condition_left_return condition_left() throws RecognitionException {
        MySQLParserParser.condition_left_return retval = new MySQLParserParser.condition_left_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.expr_return expr70 = null;


        RewriteRuleSubtreeStream stream_expr=new RewriteRuleSubtreeStream(adaptor,"rule expr");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:229:2: ( expr -> ^( CONDITION_LEFT expr ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:229:3: expr
            {
            pushFollow(FOLLOW_expr_in_condition_left867);
            expr70=expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expr.add(expr70.getTree());


            // AST REWRITE
            // elements: expr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 229:7: -> ^( CONDITION_LEFT expr )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:229:9: ^( CONDITION_LEFT expr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(CONDITION_LEFT, "CONDITION_LEFT"), root_1);

                adaptor.addChild(root_1, stream_expr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition_left"

    public static class betweenCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "betweenCondition"
    // D:\\tools\\antlr\\test\\MySQLParser.g:232:1: betweenCondition : ( 'NOT' 'BETWEEN' between_and -> ^( NOT_BETWEEN between_and ) | 'BETWEEN' between_and -> ^( BETWEEN between_and ) );
    public final MySQLParserParser.betweenCondition_return betweenCondition() throws RecognitionException {
        MySQLParserParser.betweenCondition_return retval = new MySQLParserParser.betweenCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal71=null;
        Token string_literal72=null;
        Token string_literal74=null;
        MySQLParserParser.between_and_return between_and73 = null;

        MySQLParserParser.between_and_return between_and75 = null;


        CommonTree string_literal71_tree=null;
        CommonTree string_literal72_tree=null;
        CommonTree string_literal74_tree=null;
        RewriteRuleTokenStream stream_114=new RewriteRuleTokenStream(adaptor,"token 114");
        RewriteRuleTokenStream stream_111=new RewriteRuleTokenStream(adaptor,"token 111");
        RewriteRuleSubtreeStream stream_between_and=new RewriteRuleSubtreeStream(adaptor,"rule between_and");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:233:2: ( 'NOT' 'BETWEEN' between_and -> ^( NOT_BETWEEN between_and ) | 'BETWEEN' between_and -> ^( BETWEEN between_and ) )
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0==111) ) {
                alt11=1;
            }
            else if ( (LA11_0==114) ) {
                alt11=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;
            }
            switch (alt11) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:233:4: 'NOT' 'BETWEEN' between_and
                    {
                    string_literal71=(Token)match(input,111,FOLLOW_111_in_betweenCondition885); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_111.add(string_literal71);

                    string_literal72=(Token)match(input,114,FOLLOW_114_in_betweenCondition887); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_114.add(string_literal72);

                    pushFollow(FOLLOW_between_and_in_betweenCondition889);
                    between_and73=between_and();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_between_and.add(between_and73.getTree());


                    // AST REWRITE
                    // elements: between_and
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 233:31: -> ^( NOT_BETWEEN between_and )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:233:33: ^( NOT_BETWEEN between_and )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NOT_BETWEEN, "NOT_BETWEEN"), root_1);

                        adaptor.addChild(root_1, stream_between_and.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:234:4: 'BETWEEN' between_and
                    {
                    string_literal74=(Token)match(input,114,FOLLOW_114_in_betweenCondition900); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_114.add(string_literal74);

                    pushFollow(FOLLOW_between_and_in_betweenCondition902);
                    between_and75=between_and();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_between_and.add(between_and75.getTree());


                    // AST REWRITE
                    // elements: between_and
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 234:25: -> ^( BETWEEN between_and )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:234:27: ^( BETWEEN between_and )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(BETWEEN, "BETWEEN"), root_1);

                        adaptor.addChild(root_1, stream_between_and.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "betweenCondition"

    public static class between_and_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "between_and"
    // D:\\tools\\antlr\\test\\MySQLParser.g:237:1: between_and : between_and_expression 'AND' between_and_expression ;
    public final MySQLParserParser.between_and_return between_and() throws RecognitionException {
        MySQLParserParser.between_and_return retval = new MySQLParserParser.between_and_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal77=null;
        MySQLParserParser.between_and_expression_return between_and_expression76 = null;

        MySQLParserParser.between_and_expression_return between_and_expression78 = null;


        CommonTree string_literal77_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:238:2: ( between_and_expression 'AND' between_and_expression )
            // D:\\tools\\antlr\\test\\MySQLParser.g:238:3: between_and_expression 'AND' between_and_expression
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_between_and_expression_in_between_and919);
            between_and_expression76=between_and_expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, between_and_expression76.getTree());
            string_literal77=(Token)match(input,113,FOLLOW_113_in_between_and921); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            string_literal77_tree = (CommonTree)adaptor.create(string_literal77);
            root_0 = (CommonTree)adaptor.becomeRoot(string_literal77_tree, root_0);
            }
            pushFollow(FOLLOW_between_and_expression_in_between_and924);
            between_and_expression78=between_and_expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, between_and_expression78.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "between_and"

    public static class between_and_expression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "between_and_expression"
    // D:\\tools\\antlr\\test\\MySQLParser.g:241:1: between_and_expression : expr_bit ;
    public final MySQLParserParser.between_and_expression_return between_and_expression() throws RecognitionException {
        MySQLParserParser.between_and_expression_return retval = new MySQLParserParser.between_and_expression_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.expr_bit_return expr_bit79 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:242:2: ( expr_bit )
            // D:\\tools\\antlr\\test\\MySQLParser.g:242:3: expr_bit
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_expr_bit_in_between_and_expression936);
            expr_bit79=expr_bit();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_bit79.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "between_and_expression"

    public static class isCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "isCondition"
    // D:\\tools\\antlr\\test\\MySQLParser.g:245:1: isCondition : ( 'IS' 'NOT' condition_is_valobject -> ^( ISNOT condition_is_valobject ) | 'IS' condition_is_valobject -> ^( IS condition_is_valobject ) );
    public final MySQLParserParser.isCondition_return isCondition() throws RecognitionException {
        MySQLParserParser.isCondition_return retval = new MySQLParserParser.isCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal80=null;
        Token string_literal81=null;
        Token string_literal83=null;
        MySQLParserParser.condition_is_valobject_return condition_is_valobject82 = null;

        MySQLParserParser.condition_is_valobject_return condition_is_valobject84 = null;


        CommonTree string_literal80_tree=null;
        CommonTree string_literal81_tree=null;
        CommonTree string_literal83_tree=null;
        RewriteRuleTokenStream stream_115=new RewriteRuleTokenStream(adaptor,"token 115");
        RewriteRuleTokenStream stream_111=new RewriteRuleTokenStream(adaptor,"token 111");
        RewriteRuleSubtreeStream stream_condition_is_valobject=new RewriteRuleSubtreeStream(adaptor,"rule condition_is_valobject");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:246:2: ( 'IS' 'NOT' condition_is_valobject -> ^( ISNOT condition_is_valobject ) | 'IS' condition_is_valobject -> ^( IS condition_is_valobject ) )
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0==115) ) {
                int LA12_1 = input.LA(2);

                if ( (LA12_1==111) ) {
                    alt12=1;
                }
                else if ( ((LA12_1>=116 && LA12_1<=118)) ) {
                    alt12=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 1, input);

                    throw nvae;
                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }
            switch (alt12) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:246:3: 'IS' 'NOT' condition_is_valobject
                    {
                    string_literal80=(Token)match(input,115,FOLLOW_115_in_isCondition947); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_115.add(string_literal80);

                    string_literal81=(Token)match(input,111,FOLLOW_111_in_isCondition949); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_111.add(string_literal81);

                    pushFollow(FOLLOW_condition_is_valobject_in_isCondition951);
                    condition_is_valobject82=condition_is_valobject();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_condition_is_valobject.add(condition_is_valobject82.getTree());


                    // AST REWRITE
                    // elements: condition_is_valobject
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 246:36: -> ^( ISNOT condition_is_valobject )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:246:38: ^( ISNOT condition_is_valobject )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ISNOT, "ISNOT"), root_1);

                        adaptor.addChild(root_1, stream_condition_is_valobject.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:247:3: 'IS' condition_is_valobject
                    {
                    string_literal83=(Token)match(input,115,FOLLOW_115_in_isCondition961); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_115.add(string_literal83);

                    pushFollow(FOLLOW_condition_is_valobject_in_isCondition963);
                    condition_is_valobject84=condition_is_valobject();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_condition_is_valobject.add(condition_is_valobject84.getTree());


                    // AST REWRITE
                    // elements: condition_is_valobject
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 247:30: -> ^( IS condition_is_valobject )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:247:32: ^( IS condition_is_valobject )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(IS, "IS"), root_1);

                        adaptor.addChild(root_1, stream_condition_is_valobject.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "isCondition"

    public static class condition_is_valobject_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition_is_valobject"
    // D:\\tools\\antlr\\test\\MySQLParser.g:251:1: condition_is_valobject : ( 'NAN' -> NAN | 'INFINITE' -> INFINITE | 'NULL' -> NULL );
    public final MySQLParserParser.condition_is_valobject_return condition_is_valobject() throws RecognitionException {
        MySQLParserParser.condition_is_valobject_return retval = new MySQLParserParser.condition_is_valobject_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal85=null;
        Token string_literal86=null;
        Token string_literal87=null;

        CommonTree string_literal85_tree=null;
        CommonTree string_literal86_tree=null;
        CommonTree string_literal87_tree=null;
        RewriteRuleTokenStream stream_116=new RewriteRuleTokenStream(adaptor,"token 116");
        RewriteRuleTokenStream stream_117=new RewriteRuleTokenStream(adaptor,"token 117");
        RewriteRuleTokenStream stream_118=new RewriteRuleTokenStream(adaptor,"token 118");

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:252:2: ( 'NAN' -> NAN | 'INFINITE' -> INFINITE | 'NULL' -> NULL )
            int alt13=3;
            switch ( input.LA(1) ) {
            case 116:
                {
                alt13=1;
                }
                break;
            case 117:
                {
                alt13=2;
                }
                break;
            case 118:
                {
                alt13=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 13, 0, input);

                throw nvae;
            }

            switch (alt13) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:252:4: 'NAN'
                    {
                    string_literal85=(Token)match(input,116,FOLLOW_116_in_condition_is_valobject981); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_116.add(string_literal85);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 252:10: -> NAN
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(NAN, "NAN"));

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:253:4: 'INFINITE'
                    {
                    string_literal86=(Token)match(input,117,FOLLOW_117_in_condition_is_valobject989); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_117.add(string_literal86);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 253:15: -> INFINITE
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(INFINITE, "INFINITE"));

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:254:4: 'NULL'
                    {
                    string_literal87=(Token)match(input,118,FOLLOW_118_in_condition_is_valobject997); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_118.add(string_literal87);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 254:11: -> NULL
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(NULL, "NULL"));

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition_is_valobject"

    public static class inCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "inCondition"
    // D:\\tools\\antlr\\test\\MySQLParser.g:257:1: inCondition : (not= 'NOT' )? 'IN' ( subquery | ( LPAREN inCondition_expr_bits RPAREN ) ) -> ^( IN ( $not)? ( subquery )? ( inCondition_expr_bits )? ) ;
    public final MySQLParserParser.inCondition_return inCondition() throws RecognitionException {
        MySQLParserParser.inCondition_return retval = new MySQLParserParser.inCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token not=null;
        Token string_literal88=null;
        Token LPAREN90=null;
        Token RPAREN92=null;
        MySQLParserParser.subquery_return subquery89 = null;

        MySQLParserParser.inCondition_expr_bits_return inCondition_expr_bits91 = null;


        CommonTree not_tree=null;
        CommonTree string_literal88_tree=null;
        CommonTree LPAREN90_tree=null;
        CommonTree RPAREN92_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_111=new RewriteRuleTokenStream(adaptor,"token 111");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_119=new RewriteRuleTokenStream(adaptor,"token 119");
        RewriteRuleSubtreeStream stream_inCondition_expr_bits=new RewriteRuleSubtreeStream(adaptor,"rule inCondition_expr_bits");
        RewriteRuleSubtreeStream stream_subquery=new RewriteRuleSubtreeStream(adaptor,"rule subquery");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:258:2: ( (not= 'NOT' )? 'IN' ( subquery | ( LPAREN inCondition_expr_bits RPAREN ) ) -> ^( IN ( $not)? ( subquery )? ( inCondition_expr_bits )? ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:258:3: (not= 'NOT' )? 'IN' ( subquery | ( LPAREN inCondition_expr_bits RPAREN ) )
            {
            // D:\\tools\\antlr\\test\\MySQLParser.g:258:3: (not= 'NOT' )?
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( (LA14_0==111) ) {
                alt14=1;
            }
            switch (alt14) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:258:4: not= 'NOT'
                    {
                    not=(Token)match(input,111,FOLLOW_111_in_inCondition1013); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_111.add(not);


                    }
                    break;

            }

            string_literal88=(Token)match(input,119,FOLLOW_119_in_inCondition1017); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_119.add(string_literal88);

            // D:\\tools\\antlr\\test\\MySQLParser.g:258:21: ( subquery | ( LPAREN inCondition_expr_bits RPAREN ) )
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( (LA15_0==LPAREN) ) {
                int LA15_1 = input.LA(2);

                if ( (LA15_1==INTERVAL||LA15_1==LPAREN||(LA15_1>=PLUS && LA15_1<=ASTERISK)||(LA15_1>=ID && LA15_1<=N)||LA15_1==NUMBER||LA15_1==QUOTED_STRING||LA15_1==118||(LA15_1>=121 && LA15_1<=122)||(LA15_1>=133 && LA15_1<=134)) ) {
                    alt15=2;
                }
                else if ( (LA15_1==108) ) {
                    alt15=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 15, 1, input);

                    throw nvae;
                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 15, 0, input);

                throw nvae;
            }
            switch (alt15) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:258:22: subquery
                    {
                    pushFollow(FOLLOW_subquery_in_inCondition1020);
                    subquery89=subquery();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_subquery.add(subquery89.getTree());

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:259:3: ( LPAREN inCondition_expr_bits RPAREN )
                    {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:259:3: ( LPAREN inCondition_expr_bits RPAREN )
                    // D:\\tools\\antlr\\test\\MySQLParser.g:259:5: LPAREN inCondition_expr_bits RPAREN
                    {
                    LPAREN90=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_inCondition1026); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN90);

                    pushFollow(FOLLOW_inCondition_expr_bits_in_inCondition1028);
                    inCondition_expr_bits91=inCondition_expr_bits();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_inCondition_expr_bits.add(inCondition_expr_bits91.getTree());
                    RPAREN92=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_inCondition1030); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN92);


                    }


                    }
                    break;

            }



            // AST REWRITE
            // elements: not, inCondition_expr_bits, subquery
            // token labels: not
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleTokenStream stream_not=new RewriteRuleTokenStream(adaptor,"token not",not);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 259:42: -> ^( IN ( $not)? ( subquery )? ( inCondition_expr_bits )? )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:259:44: ^( IN ( $not)? ( subquery )? ( inCondition_expr_bits )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(IN, "IN"), root_1);

                // D:\\tools\\antlr\\test\\MySQLParser.g:259:49: ( $not)?
                if ( stream_not.hasNext() ) {
                    adaptor.addChild(root_1, stream_not.nextNode());

                }
                stream_not.reset();
                // D:\\tools\\antlr\\test\\MySQLParser.g:259:55: ( subquery )?
                if ( stream_subquery.hasNext() ) {
                    adaptor.addChild(root_1, stream_subquery.nextTree());

                }
                stream_subquery.reset();
                // D:\\tools\\antlr\\test\\MySQLParser.g:259:65: ( inCondition_expr_bits )?
                if ( stream_inCondition_expr_bits.hasNext() ) {
                    adaptor.addChild(root_1, stream_inCondition_expr_bits.nextTree());

                }
                stream_inCondition_expr_bits.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "inCondition"

    public static class likeCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "likeCondition"
    // D:\\tools\\antlr\\test\\MySQLParser.g:263:1: likeCondition : ( 'NOT' 'LIKE' value -> ^( NOT_LIKE value ) | 'LIKE' value -> ^( LIKE value ) );
    public final MySQLParserParser.likeCondition_return likeCondition() throws RecognitionException {
        MySQLParserParser.likeCondition_return retval = new MySQLParserParser.likeCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal93=null;
        Token string_literal94=null;
        Token string_literal96=null;
        MySQLParserParser.value_return value95 = null;

        MySQLParserParser.value_return value97 = null;


        CommonTree string_literal93_tree=null;
        CommonTree string_literal94_tree=null;
        CommonTree string_literal96_tree=null;
        RewriteRuleTokenStream stream_111=new RewriteRuleTokenStream(adaptor,"token 111");
        RewriteRuleTokenStream stream_120=new RewriteRuleTokenStream(adaptor,"token 120");
        RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:264:2: ( 'NOT' 'LIKE' value -> ^( NOT_LIKE value ) | 'LIKE' value -> ^( LIKE value ) )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==111) ) {
                alt16=1;
            }
            else if ( (LA16_0==120) ) {
                alt16=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }
            switch (alt16) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:264:3: 'NOT' 'LIKE' value
                    {
                    string_literal93=(Token)match(input,111,FOLLOW_111_in_likeCondition1057); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_111.add(string_literal93);

                    string_literal94=(Token)match(input,120,FOLLOW_120_in_likeCondition1058); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_120.add(string_literal94);

                    pushFollow(FOLLOW_value_in_likeCondition1061);
                    value95=value();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_value.add(value95.getTree());


                    // AST REWRITE
                    // elements: value
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 264:21: -> ^( NOT_LIKE value )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:264:23: ^( NOT_LIKE value )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NOT_LIKE, "NOT_LIKE"), root_1);

                        adaptor.addChild(root_1, stream_value.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:265:3: 'LIKE' value
                    {
                    string_literal96=(Token)match(input,120,FOLLOW_120_in_likeCondition1071); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_120.add(string_literal96);

                    pushFollow(FOLLOW_value_in_likeCondition1073);
                    value97=value();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_value.add(value97.getTree());


                    // AST REWRITE
                    // elements: value
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 265:15: -> ^( LIKE value )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:265:17: ^( LIKE value )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(LIKE, "LIKE"), root_1);

                        adaptor.addChild(root_1, stream_value.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "likeCondition"

    public static class inCondition_expr_bits_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "inCondition_expr_bits"
    // D:\\tools\\antlr\\test\\MySQLParser.g:268:1: inCondition_expr_bits : expr_bit ( COMMA expr_bit )* -> ^( IN_LISTS ( expr_bit )+ ) ;
    public final MySQLParserParser.inCondition_expr_bits_return inCondition_expr_bits() throws RecognitionException {
        MySQLParserParser.inCondition_expr_bits_return retval = new MySQLParserParser.inCondition_expr_bits_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA99=null;
        MySQLParserParser.expr_bit_return expr_bit98 = null;

        MySQLParserParser.expr_bit_return expr_bit100 = null;


        CommonTree COMMA99_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_expr_bit=new RewriteRuleSubtreeStream(adaptor,"rule expr_bit");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:268:22: ( expr_bit ( COMMA expr_bit )* -> ^( IN_LISTS ( expr_bit )+ ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:269:2: expr_bit ( COMMA expr_bit )*
            {
            pushFollow(FOLLOW_expr_bit_in_inCondition_expr_bits1089);
            expr_bit98=expr_bit();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expr_bit.add(expr_bit98.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:269:10: ( COMMA expr_bit )*
            loop17:
            do {
                int alt17=2;
                int LA17_0 = input.LA(1);

                if ( (LA17_0==COMMA) ) {
                    alt17=1;
                }


                switch (alt17) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:269:11: COMMA expr_bit
            	    {
            	    COMMA99=(Token)match(input,COMMA,FOLLOW_COMMA_in_inCondition_expr_bits1091); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA99);

            	    pushFollow(FOLLOW_expr_bit_in_inCondition_expr_bits1093);
            	    expr_bit100=expr_bit();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expr_bit.add(expr_bit100.getTree());

            	    }
            	    break;

            	default :
            	    break loop17;
                }
            } while (true);



            // AST REWRITE
            // elements: expr_bit
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 269:27: -> ^( IN_LISTS ( expr_bit )+ )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:269:29: ^( IN_LISTS ( expr_bit )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(IN_LISTS, "IN_LISTS"), root_1);

                if ( !(stream_expr_bit.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_expr_bit.hasNext() ) {
                    adaptor.addChild(root_1, stream_expr_bit.nextTree());

                }
                stream_expr_bit.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "inCondition_expr_bits"

    public static class identifiers_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "identifiers"
    // D:\\tools\\antlr\\test\\MySQLParser.g:272:1: identifiers : columnNameAfterWhere ( COMMA identifier )* ;
    public final MySQLParserParser.identifiers_return identifiers() throws RecognitionException {
        MySQLParserParser.identifiers_return retval = new MySQLParserParser.identifiers_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA102=null;
        MySQLParserParser.columnNameAfterWhere_return columnNameAfterWhere101 = null;

        MySQLParserParser.identifier_return identifier103 = null;


        CommonTree COMMA102_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:273:2: ( columnNameAfterWhere ( COMMA identifier )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:273:3: columnNameAfterWhere ( COMMA identifier )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_columnNameAfterWhere_in_identifiers1113);
            columnNameAfterWhere101=columnNameAfterWhere();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, columnNameAfterWhere101.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:273:24: ( COMMA identifier )*
            loop18:
            do {
                int alt18=2;
                int LA18_0 = input.LA(1);

                if ( (LA18_0==COMMA) ) {
                    alt18=1;
                }


                switch (alt18) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:273:25: COMMA identifier
            	    {
            	    COMMA102=(Token)match(input,COMMA,FOLLOW_COMMA_in_identifiers1116); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    COMMA102_tree = (CommonTree)adaptor.create(COMMA102);
            	    adaptor.addChild(root_0, COMMA102_tree);
            	    }
            	    pushFollow(FOLLOW_identifier_in_identifiers1118);
            	    identifier103=identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier103.getTree());

            	    }
            	    break;

            	default :
            	    break loop18;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "identifiers"

    public static class comparisonCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparisonCondition"
    // D:\\tools\\antlr\\test\\MySQLParser.g:276:1: comparisonCondition : relational_op expr -> ^( relational_op expr ) ;
    public final MySQLParserParser.comparisonCondition_return comparisonCondition() throws RecognitionException {
        MySQLParserParser.comparisonCondition_return retval = new MySQLParserParser.comparisonCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.relational_op_return relational_op104 = null;

        MySQLParserParser.expr_return expr105 = null;


        RewriteRuleSubtreeStream stream_relational_op=new RewriteRuleSubtreeStream(adaptor,"rule relational_op");
        RewriteRuleSubtreeStream stream_expr=new RewriteRuleSubtreeStream(adaptor,"rule expr");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:277:2: ( relational_op expr -> ^( relational_op expr ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:277:3: relational_op expr
            {
            pushFollow(FOLLOW_relational_op_in_comparisonCondition1130);
            relational_op104=relational_op();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_relational_op.add(relational_op104.getTree());
            pushFollow(FOLLOW_expr_in_comparisonCondition1132);
            expr105=expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expr.add(expr105.getTree());


            // AST REWRITE
            // elements: expr, relational_op
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 277:21: -> ^( relational_op expr )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:277:23: ^( relational_op expr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(stream_relational_op.nextNode(), root_1);

                adaptor.addChild(root_1, stream_expr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "comparisonCondition"

    public static class expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr"
    // D:\\tools\\antlr\\test\\MySQLParser.g:280:1: expr : ( expr_bit | subquery ) ;
    public final MySQLParserParser.expr_return expr() throws RecognitionException {
        MySQLParserParser.expr_return retval = new MySQLParserParser.expr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.expr_bit_return expr_bit106 = null;

        MySQLParserParser.subquery_return subquery107 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:280:6: ( ( expr_bit | subquery ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:280:7: ( expr_bit | subquery )
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\tools\\antlr\\test\\MySQLParser.g:280:7: ( expr_bit | subquery )
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==INTERVAL||(LA19_0>=PLUS && LA19_0<=ASTERISK)||(LA19_0>=ID && LA19_0<=N)||LA19_0==NUMBER||LA19_0==QUOTED_STRING||LA19_0==118||(LA19_0>=121 && LA19_0<=122)||(LA19_0>=133 && LA19_0<=134)) ) {
                alt19=1;
            }
            else if ( (LA19_0==LPAREN) ) {
                int LA19_2 = input.LA(2);

                if ( (LA19_2==INTERVAL||LA19_2==LPAREN||(LA19_2>=PLUS && LA19_2<=ASTERISK)||(LA19_2>=ID && LA19_2<=N)||LA19_2==NUMBER||LA19_2==QUOTED_STRING||LA19_2==118||(LA19_2>=121 && LA19_2<=122)||(LA19_2>=133 && LA19_2<=134)) ) {
                    alt19=1;
                }
                else if ( (LA19_2==108) ) {
                    alt19=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 19, 2, input);

                    throw nvae;
                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 19, 0, input);

                throw nvae;
            }
            switch (alt19) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:280:8: expr_bit
                    {
                    pushFollow(FOLLOW_expr_bit_in_expr1149);
                    expr_bit106=expr_bit();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_bit106.getTree());

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:281:3: subquery
                    {
                    pushFollow(FOLLOW_subquery_in_expr1153);
                    subquery107=subquery();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, subquery107.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr"

    public static class subquery_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "subquery"
    // D:\\tools\\antlr\\test\\MySQLParser.g:285:1: subquery : LPAREN select_command RPAREN -> ^( SUBQUERY select_command ) ;
    public final MySQLParserParser.subquery_return subquery() throws RecognitionException {
        MySQLParserParser.subquery_return retval = new MySQLParserParser.subquery_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LPAREN108=null;
        Token RPAREN110=null;
        MySQLParserParser.select_command_return select_command109 = null;


        CommonTree LPAREN108_tree=null;
        CommonTree RPAREN110_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_select_command=new RewriteRuleSubtreeStream(adaptor,"rule select_command");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:285:9: ( LPAREN select_command RPAREN -> ^( SUBQUERY select_command ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:286:2: LPAREN select_command RPAREN
            {
            LPAREN108=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_subquery1168); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN108);

            pushFollow(FOLLOW_select_command_in_subquery1170);
            select_command109=select_command();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_select_command.add(select_command109.getTree());
            RPAREN110=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_subquery1172); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN110);



            // AST REWRITE
            // elements: select_command
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 286:30: -> ^( SUBQUERY select_command )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:286:32: ^( SUBQUERY select_command )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SUBQUERY, "SUBQUERY"), root_1);

                adaptor.addChild(root_1, stream_select_command.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "subquery"

    public static class expr_bit_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr_bit"
    // D:\\tools\\antlr\\test\\MySQLParser.g:289:1: expr_bit : expr_add ( ( BITOR | BITAND | BITXOR | SHIFTLEFT | SHIFTRIGHT ) ( expr_add ) )* ;
    public final MySQLParserParser.expr_bit_return expr_bit() throws RecognitionException {
        MySQLParserParser.expr_bit_return retval = new MySQLParserParser.expr_bit_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token BITOR112=null;
        Token BITAND113=null;
        Token BITXOR114=null;
        Token SHIFTLEFT115=null;
        Token SHIFTRIGHT116=null;
        MySQLParserParser.expr_add_return expr_add111 = null;

        MySQLParserParser.expr_add_return expr_add117 = null;


        CommonTree BITOR112_tree=null;
        CommonTree BITAND113_tree=null;
        CommonTree BITXOR114_tree=null;
        CommonTree SHIFTLEFT115_tree=null;
        CommonTree SHIFTRIGHT116_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:290:2: ( expr_add ( ( BITOR | BITAND | BITXOR | SHIFTLEFT | SHIFTRIGHT ) ( expr_add ) )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:290:3: expr_add ( ( BITOR | BITAND | BITXOR | SHIFTLEFT | SHIFTRIGHT ) ( expr_add ) )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_expr_add_in_expr_bit1190);
            expr_add111=expr_add();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_add111.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:290:12: ( ( BITOR | BITAND | BITXOR | SHIFTLEFT | SHIFTRIGHT ) ( expr_add ) )*
            loop21:
            do {
                int alt21=2;
                int LA21_0 = input.LA(1);

                if ( ((LA21_0>=BITOR && LA21_0<=SHIFTRIGHT)) ) {
                    alt21=1;
                }


                switch (alt21) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:290:14: ( BITOR | BITAND | BITXOR | SHIFTLEFT | SHIFTRIGHT ) ( expr_add )
            	    {
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:290:14: ( BITOR | BITAND | BITXOR | SHIFTLEFT | SHIFTRIGHT )
            	    int alt20=5;
            	    switch ( input.LA(1) ) {
            	    case BITOR:
            	        {
            	        alt20=1;
            	        }
            	        break;
            	    case BITAND:
            	        {
            	        alt20=2;
            	        }
            	        break;
            	    case BITXOR:
            	        {
            	        alt20=3;
            	        }
            	        break;
            	    case SHIFTLEFT:
            	        {
            	        alt20=4;
            	        }
            	        break;
            	    case SHIFTRIGHT:
            	        {
            	        alt20=5;
            	        }
            	        break;
            	    default:
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 20, 0, input);

            	        throw nvae;
            	    }

            	    switch (alt20) {
            	        case 1 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:290:16: BITOR
            	            {
            	            BITOR112=(Token)match(input,BITOR,FOLLOW_BITOR_in_expr_bit1196); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            BITOR112_tree = (CommonTree)adaptor.create(BITOR112);
            	            root_0 = (CommonTree)adaptor.becomeRoot(BITOR112_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 2 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:290:25: BITAND
            	            {
            	            BITAND113=(Token)match(input,BITAND,FOLLOW_BITAND_in_expr_bit1201); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            BITAND113_tree = (CommonTree)adaptor.create(BITAND113);
            	            root_0 = (CommonTree)adaptor.becomeRoot(BITAND113_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 3 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:290:33: BITXOR
            	            {
            	            BITXOR114=(Token)match(input,BITXOR,FOLLOW_BITXOR_in_expr_bit1204); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            BITXOR114_tree = (CommonTree)adaptor.create(BITXOR114);
            	            root_0 = (CommonTree)adaptor.becomeRoot(BITXOR114_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 4 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:290:41: SHIFTLEFT
            	            {
            	            SHIFTLEFT115=(Token)match(input,SHIFTLEFT,FOLLOW_SHIFTLEFT_in_expr_bit1207); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            SHIFTLEFT115_tree = (CommonTree)adaptor.create(SHIFTLEFT115);
            	            root_0 = (CommonTree)adaptor.becomeRoot(SHIFTLEFT115_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 5 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:290:52: SHIFTRIGHT
            	            {
            	            SHIFTRIGHT116=(Token)match(input,SHIFTRIGHT,FOLLOW_SHIFTRIGHT_in_expr_bit1210); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            SHIFTRIGHT116_tree = (CommonTree)adaptor.create(SHIFTRIGHT116);
            	            root_0 = (CommonTree)adaptor.becomeRoot(SHIFTRIGHT116_tree, root_0);
            	            }

            	            }
            	            break;

            	    }

            	    // D:\\tools\\antlr\\test\\MySQLParser.g:290:65: ( expr_add )
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:290:66: expr_add
            	    {
            	    pushFollow(FOLLOW_expr_add_in_expr_bit1215);
            	    expr_add117=expr_add();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_add117.getTree());

            	    }


            	    }
            	    break;

            	default :
            	    break loop21;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr_bit"

    public static class expr_add_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr_add"
    // D:\\tools\\antlr\\test\\MySQLParser.g:293:1: expr_add : expr_mul ( ( PLUS | MINUS ) ( expr_mul ) )* ;
    public final MySQLParserParser.expr_add_return expr_add() throws RecognitionException {
        MySQLParserParser.expr_add_return retval = new MySQLParserParser.expr_add_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token PLUS119=null;
        Token MINUS120=null;
        MySQLParserParser.expr_mul_return expr_mul118 = null;

        MySQLParserParser.expr_mul_return expr_mul121 = null;


        CommonTree PLUS119_tree=null;
        CommonTree MINUS120_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:294:2: ( expr_mul ( ( PLUS | MINUS ) ( expr_mul ) )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:294:3: expr_mul ( ( PLUS | MINUS ) ( expr_mul ) )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_expr_mul_in_expr_add1230);
            expr_mul118=expr_mul();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_mul118.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:294:12: ( ( PLUS | MINUS ) ( expr_mul ) )*
            loop23:
            do {
                int alt23=2;
                int LA23_0 = input.LA(1);

                if ( ((LA23_0>=PLUS && LA23_0<=MINUS)) ) {
                    alt23=1;
                }


                switch (alt23) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:294:14: ( PLUS | MINUS ) ( expr_mul )
            	    {
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:294:14: ( PLUS | MINUS )
            	    int alt22=2;
            	    int LA22_0 = input.LA(1);

            	    if ( (LA22_0==PLUS) ) {
            	        alt22=1;
            	    }
            	    else if ( (LA22_0==MINUS) ) {
            	        alt22=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 22, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt22) {
            	        case 1 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:294:16: PLUS
            	            {
            	            PLUS119=(Token)match(input,PLUS,FOLLOW_PLUS_in_expr_add1236); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            PLUS119_tree = (CommonTree)adaptor.create(PLUS119);
            	            root_0 = (CommonTree)adaptor.becomeRoot(PLUS119_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 2 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:294:24: MINUS
            	            {
            	            MINUS120=(Token)match(input,MINUS,FOLLOW_MINUS_in_expr_add1241); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            MINUS120_tree = (CommonTree)adaptor.create(MINUS120);
            	            root_0 = (CommonTree)adaptor.becomeRoot(MINUS120_tree, root_0);
            	            }

            	            }
            	            break;

            	    }

            	    // D:\\tools\\antlr\\test\\MySQLParser.g:294:33: ( expr_mul )
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:294:34: expr_mul
            	    {
            	    pushFollow(FOLLOW_expr_mul_in_expr_add1247);
            	    expr_mul121=expr_mul();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_mul121.getTree());

            	    }


            	    }
            	    break;

            	default :
            	    break loop23;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr_add"

    public static class expr_mul_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr_mul"
    // D:\\tools\\antlr\\test\\MySQLParser.g:297:1: expr_mul : expr_sign ( ( ASTERISK | DIVIDE ) expr_sign )* ;
    public final MySQLParserParser.expr_mul_return expr_mul() throws RecognitionException {
        MySQLParserParser.expr_mul_return retval = new MySQLParserParser.expr_mul_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ASTERISK123=null;
        Token DIVIDE124=null;
        MySQLParserParser.expr_sign_return expr_sign122 = null;

        MySQLParserParser.expr_sign_return expr_sign125 = null;


        CommonTree ASTERISK123_tree=null;
        CommonTree DIVIDE124_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:298:2: ( expr_sign ( ( ASTERISK | DIVIDE ) expr_sign )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:298:3: expr_sign ( ( ASTERISK | DIVIDE ) expr_sign )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_expr_sign_in_expr_mul1262);
            expr_sign122=expr_sign();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_sign122.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:298:13: ( ( ASTERISK | DIVIDE ) expr_sign )*
            loop25:
            do {
                int alt25=2;
                int LA25_0 = input.LA(1);

                if ( ((LA25_0>=ASTERISK && LA25_0<=DIVIDE)) ) {
                    alt25=1;
                }


                switch (alt25) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:298:15: ( ASTERISK | DIVIDE ) expr_sign
            	    {
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:298:15: ( ASTERISK | DIVIDE )
            	    int alt24=2;
            	    int LA24_0 = input.LA(1);

            	    if ( (LA24_0==ASTERISK) ) {
            	        alt24=1;
            	    }
            	    else if ( (LA24_0==DIVIDE) ) {
            	        alt24=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 24, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt24) {
            	        case 1 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:298:17: ASTERISK
            	            {
            	            ASTERISK123=(Token)match(input,ASTERISK,FOLLOW_ASTERISK_in_expr_mul1268); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            ASTERISK123_tree = (CommonTree)adaptor.create(ASTERISK123);
            	            root_0 = (CommonTree)adaptor.becomeRoot(ASTERISK123_tree, root_0);
            	            }

            	            }
            	            break;
            	        case 2 :
            	            // D:\\tools\\antlr\\test\\MySQLParser.g:298:29: DIVIDE
            	            {
            	            DIVIDE124=(Token)match(input,DIVIDE,FOLLOW_DIVIDE_in_expr_mul1273); if (state.failed) return retval;
            	            if ( state.backtracking==0 ) {
            	            DIVIDE124_tree = (CommonTree)adaptor.create(DIVIDE124);
            	            root_0 = (CommonTree)adaptor.becomeRoot(DIVIDE124_tree, root_0);
            	            }

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_expr_sign_in_expr_mul1278);
            	    expr_sign125=expr_sign();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_sign125.getTree());

            	    }
            	    break;

            	default :
            	    break loop25;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr_mul"

    public static class expr_sign_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr_sign"
    // D:\\tools\\antlr\\test\\MySQLParser.g:301:1: expr_sign : ( PLUS expr_pow -> ^( POSITIVE expr_pow ) | MINUS expr_pow -> ^( NEGATIVE expr_pow ) | expr_pow );
    public final MySQLParserParser.expr_sign_return expr_sign() throws RecognitionException {
        MySQLParserParser.expr_sign_return retval = new MySQLParserParser.expr_sign_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token PLUS126=null;
        Token MINUS128=null;
        MySQLParserParser.expr_pow_return expr_pow127 = null;

        MySQLParserParser.expr_pow_return expr_pow129 = null;

        MySQLParserParser.expr_pow_return expr_pow130 = null;


        CommonTree PLUS126_tree=null;
        CommonTree MINUS128_tree=null;
        RewriteRuleTokenStream stream_PLUS=new RewriteRuleTokenStream(adaptor,"token PLUS");
        RewriteRuleTokenStream stream_MINUS=new RewriteRuleTokenStream(adaptor,"token MINUS");
        RewriteRuleSubtreeStream stream_expr_pow=new RewriteRuleSubtreeStream(adaptor,"rule expr_pow");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:302:2: ( PLUS expr_pow -> ^( POSITIVE expr_pow ) | MINUS expr_pow -> ^( NEGATIVE expr_pow ) | expr_pow )
            int alt26=3;
            switch ( input.LA(1) ) {
            case PLUS:
                {
                alt26=1;
                }
                break;
            case MINUS:
                {
                alt26=2;
                }
                break;
            case INTERVAL:
            case LPAREN:
            case ASTERISK:
            case ID:
            case N:
            case NUMBER:
            case QUOTED_STRING:
            case 118:
            case 121:
            case 122:
            case 133:
            case 134:
                {
                alt26=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 26, 0, input);

                throw nvae;
            }

            switch (alt26) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:302:3: PLUS expr_pow
                    {
                    PLUS126=(Token)match(input,PLUS,FOLLOW_PLUS_in_expr_sign1292); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_PLUS.add(PLUS126);

                    pushFollow(FOLLOW_expr_pow_in_expr_sign1294);
                    expr_pow127=expr_pow();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expr_pow.add(expr_pow127.getTree());


                    // AST REWRITE
                    // elements: expr_pow
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 302:16: -> ^( POSITIVE expr_pow )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:302:18: ^( POSITIVE expr_pow )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(POSITIVE, "POSITIVE"), root_1);

                        adaptor.addChild(root_1, stream_expr_pow.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:303:3: MINUS expr_pow
                    {
                    MINUS128=(Token)match(input,MINUS,FOLLOW_MINUS_in_expr_sign1304); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_MINUS.add(MINUS128);

                    pushFollow(FOLLOW_expr_pow_in_expr_sign1306);
                    expr_pow129=expr_pow();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expr_pow.add(expr_pow129.getTree());


                    // AST REWRITE
                    // elements: expr_pow
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 303:17: -> ^( NEGATIVE expr_pow )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:303:19: ^( NEGATIVE expr_pow )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NEGATIVE, "NEGATIVE"), root_1);

                        adaptor.addChild(root_1, stream_expr_pow.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:304:3: expr_pow
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_expr_pow_in_expr_sign1316);
                    expr_pow130=expr_pow();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_pow130.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr_sign"

    public static class expr_pow_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr_pow"
    // D:\\tools\\antlr\\test\\MySQLParser.g:307:1: expr_pow : expr_expr ( EXPONENT expr_expr )* ;
    public final MySQLParserParser.expr_pow_return expr_pow() throws RecognitionException {
        MySQLParserParser.expr_pow_return retval = new MySQLParserParser.expr_pow_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token EXPONENT132=null;
        MySQLParserParser.expr_expr_return expr_expr131 = null;

        MySQLParserParser.expr_expr_return expr_expr133 = null;


        CommonTree EXPONENT132_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:308:2: ( expr_expr ( EXPONENT expr_expr )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:308:3: expr_expr ( EXPONENT expr_expr )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_expr_expr_in_expr_pow1327);
            expr_expr131=expr_expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_expr131.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:308:13: ( EXPONENT expr_expr )*
            loop27:
            do {
                int alt27=2;
                int LA27_0 = input.LA(1);

                if ( (LA27_0==EXPONENT) ) {
                    alt27=1;
                }


                switch (alt27) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:308:15: EXPONENT expr_expr
            	    {
            	    EXPONENT132=(Token)match(input,EXPONENT,FOLLOW_EXPONENT_in_expr_pow1331); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    EXPONENT132_tree = (CommonTree)adaptor.create(EXPONENT132);
            	    root_0 = (CommonTree)adaptor.becomeRoot(EXPONENT132_tree, root_0);
            	    }
            	    pushFollow(FOLLOW_expr_expr_in_expr_pow1334);
            	    expr_expr133=expr_expr();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_expr133.getTree());

            	    }
            	    break;

            	default :
            	    break loop27;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr_pow"

    public static class expr_expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr_expr"
    // D:\\tools\\antlr\\test\\MySQLParser.g:311:1: expr_expr : ( interval_clause | {...}? ID ( LPAREN ( values_func )? RPAREN ) -> ^( ID ( values_func )? ) | {...}? ID -> ^( CONSIST ID ) | value | boolean_literal | 'NULL' | 'ROWNUM' );
    public final MySQLParserParser.expr_expr_return expr_expr() throws RecognitionException {
        MySQLParserParser.expr_expr_return retval = new MySQLParserParser.expr_expr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID135=null;
        Token LPAREN136=null;
        Token RPAREN138=null;
        Token ID139=null;
        Token string_literal142=null;
        Token string_literal143=null;
        MySQLParserParser.interval_clause_return interval_clause134 = null;

        MySQLParserParser.values_func_return values_func137 = null;

        MySQLParserParser.value_return value140 = null;

        MySQLParserParser.boolean_literal_return boolean_literal141 = null;


        CommonTree ID135_tree=null;
        CommonTree LPAREN136_tree=null;
        CommonTree RPAREN138_tree=null;
        CommonTree ID139_tree=null;
        CommonTree string_literal142_tree=null;
        CommonTree string_literal143_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_values_func=new RewriteRuleSubtreeStream(adaptor,"rule values_func");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:312:9: ( interval_clause | {...}? ID ( LPAREN ( values_func )? RPAREN ) -> ^( ID ( values_func )? ) | {...}? ID -> ^( CONSIST ID ) | value | boolean_literal | 'NULL' | 'ROWNUM' )
            int alt29=7;
            switch ( input.LA(1) ) {
            case INTERVAL:
                {
                alt29=1;
                }
                break;
            case ID:
                {
                int LA29_2 = input.LA(2);

                if ( (LA29_2==LPAREN) ) {
                    alt29=2;
                }
                else if ( ((consistStr.containsKey(input.LT(1).getText().toUpperCase()))) ) {
                    alt29=3;
                }
                else if ( (true) ) {
                    alt29=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 29, 2, input);

                    throw nvae;
                }
                }
                break;
            case LPAREN:
            case ASTERISK:
            case N:
            case NUMBER:
            case QUOTED_STRING:
            case 122:
                {
                alt29=4;
                }
                break;
            case 133:
            case 134:
                {
                alt29=5;
                }
                break;
            case 118:
                {
                alt29=6;
                }
                break;
            case 121:
                {
                alt29=7;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 29, 0, input);

                throw nvae;
            }

            switch (alt29) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:312:10: interval_clause
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_interval_clause_in_expr_expr1354);
                    interval_clause134=interval_clause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, interval_clause134.getTree());

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:316:3: {...}? ID ( LPAREN ( values_func )? RPAREN )
                    {
                    if ( !((functionMap.containsKey(input.LT(1).getText().toUpperCase()))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "expr_expr", "functionMap.containsKey(input.LT(1).getText().toUpperCase())");
                    }
                    ID135=(Token)match(input,ID,FOLLOW_ID_in_expr_expr1364); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID135);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:316:70: ( LPAREN ( values_func )? RPAREN )
                    // D:\\tools\\antlr\\test\\MySQLParser.g:316:71: LPAREN ( values_func )? RPAREN
                    {
                    LPAREN136=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_expr_expr1367); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN136);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:316:78: ( values_func )?
                    int alt28=2;
                    int LA28_0 = input.LA(1);

                    if ( (LA28_0==INTERVAL||LA28_0==LPAREN||(LA28_0>=PLUS && LA28_0<=ASTERISK)||(LA28_0>=ID && LA28_0<=N)||LA28_0==NUMBER||LA28_0==QUOTED_STRING||LA28_0==118||(LA28_0>=121 && LA28_0<=122)||(LA28_0>=133 && LA28_0<=134)) ) {
                        alt28=1;
                    }
                    switch (alt28) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:316:78: values_func
                            {
                            pushFollow(FOLLOW_values_func_in_expr_expr1369);
                            values_func137=values_func();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_values_func.add(values_func137.getTree());

                            }
                            break;

                    }

                    RPAREN138=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_expr_expr1372); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN138);


                    }



                    // AST REWRITE
                    // elements: ID, values_func
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 316:99: -> ^( ID ( values_func )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:316:101: ^( ID ( values_func )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_ID.nextNode(), root_1);

                        // D:\\tools\\antlr\\test\\MySQLParser.g:316:106: ( values_func )?
                        if ( stream_values_func.hasNext() ) {
                            adaptor.addChild(root_1, stream_values_func.nextTree());

                        }
                        stream_values_func.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:317:3: {...}? ID
                    {
                    if ( !((consistStr.containsKey(input.LT(1).getText().toUpperCase()))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "expr_expr", "consistStr.containsKey(input.LT(1).getText().toUpperCase())");
                    }
                    ID139=(Token)match(input,ID,FOLLOW_ID_in_expr_expr1387); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID139);



                    // AST REWRITE
                    // elements: ID
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 317:69: -> ^( CONSIST ID )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:317:71: ^( CONSIST ID )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(CONSIST, "CONSIST"), root_1);

                        adaptor.addChild(root_1, stream_ID.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:319:10: value
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_value_in_expr_expr1406);
                    value140=value();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, value140.getTree());

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:320:3: boolean_literal
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_boolean_literal_in_expr_expr1410);
                    boolean_literal141=boolean_literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, boolean_literal141.getTree());

                    }
                    break;
                case 6 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:321:3: 'NULL'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    string_literal142=(Token)match(input,118,FOLLOW_118_in_expr_expr1414); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal142_tree = (CommonTree)adaptor.create(string_literal142);
                    adaptor.addChild(root_0, string_literal142_tree);
                    }

                    }
                    break;
                case 7 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:322:3: 'ROWNUM'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    string_literal143=(Token)match(input,121,FOLLOW_121_in_expr_expr1418); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal143_tree = (CommonTree)adaptor.create(string_literal143);
                    adaptor.addChild(root_0, string_literal143_tree);
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr_expr"

    public static class interval_clause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "interval_clause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:338:1: interval_clause : INTERVAL inner_value unit -> ^( INTERVAL inner_value unit ) ;
    public final MySQLParserParser.interval_clause_return interval_clause() throws RecognitionException {
        MySQLParserParser.interval_clause_return retval = new MySQLParserParser.interval_clause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token INTERVAL144=null;
        MySQLParserParser.inner_value_return inner_value145 = null;

        MySQLParserParser.unit_return unit146 = null;


        CommonTree INTERVAL144_tree=null;
        RewriteRuleTokenStream stream_INTERVAL=new RewriteRuleTokenStream(adaptor,"token INTERVAL");
        RewriteRuleSubtreeStream stream_unit=new RewriteRuleSubtreeStream(adaptor,"rule unit");
        RewriteRuleSubtreeStream stream_inner_value=new RewriteRuleSubtreeStream(adaptor,"rule inner_value");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:338:16: ( INTERVAL inner_value unit -> ^( INTERVAL inner_value unit ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:338:17: INTERVAL inner_value unit
            {
            INTERVAL144=(Token)match(input,INTERVAL,FOLLOW_INTERVAL_in_interval_clause1455); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_INTERVAL.add(INTERVAL144);

            pushFollow(FOLLOW_inner_value_in_interval_clause1457);
            inner_value145=inner_value();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_inner_value.add(inner_value145.getTree());
            pushFollow(FOLLOW_unit_in_interval_clause1459);
            unit146=unit();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_unit.add(unit146.getTree());


            // AST REWRITE
            // elements: inner_value, INTERVAL, unit
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 338:42: -> ^( INTERVAL inner_value unit )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:338:44: ^( INTERVAL inner_value unit )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(stream_INTERVAL.nextNode(), root_1);

                adaptor.addChild(root_1, stream_inner_value.nextTree());
                adaptor.addChild(root_1, stream_unit.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "interval_clause"

    public static class unit_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unit"
    // D:\\tools\\antlr\\test\\MySQLParser.g:342:1: unit : {...}? ID -> ^( UNIT ID ) ;
    public final MySQLParserParser.unit_return unit() throws RecognitionException {
        MySQLParserParser.unit_return retval = new MySQLParserParser.unit_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID147=null;

        CommonTree ID147_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:346:6: ({...}? ID -> ^( UNIT ID ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:346:7: {...}? ID
            {
            if ( !((consistStr.containsKey(input.LT(1).getText().toUpperCase()))) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "unit", "consistStr.containsKey(input.LT(1).getText().toUpperCase())");
            }
            ID147=(Token)match(input,ID,FOLLOW_ID_in_unit1500); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(ID147);



            // AST REWRITE
            // elements: ID
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 346:72: -> ^( UNIT ID )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:346:74: ^( UNIT ID )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(UNIT, "UNIT"), root_1);

                adaptor.addChild(root_1, stream_ID.nextNode());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "unit"

    public static class inner_value_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "inner_value"
    // D:\\tools\\antlr\\test\\MySQLParser.g:349:1: inner_value : ( '?' | MINUS N | N );
    public final MySQLParserParser.inner_value_return inner_value() throws RecognitionException {
        MySQLParserParser.inner_value_return retval = new MySQLParserParser.inner_value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal148=null;
        Token MINUS149=null;
        Token N150=null;
        Token N151=null;

        CommonTree char_literal148_tree=null;
        CommonTree MINUS149_tree=null;
        CommonTree N150_tree=null;
        CommonTree N151_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:350:2: ( '?' | MINUS N | N )
            int alt30=3;
            switch ( input.LA(1) ) {
            case 122:
                {
                alt30=1;
                }
                break;
            case MINUS:
                {
                alt30=2;
                }
                break;
            case N:
                {
                alt30=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 30, 0, input);

                throw nvae;
            }

            switch (alt30) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:350:3: '?'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    char_literal148=(Token)match(input,122,FOLLOW_122_in_inner_value1524); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    char_literal148_tree = (CommonTree)adaptor.create(char_literal148);
                    adaptor.addChild(root_0, char_literal148_tree);
                    }

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:351:3: MINUS N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    MINUS149=(Token)match(input,MINUS,FOLLOW_MINUS_in_inner_value1528); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    MINUS149_tree = (CommonTree)adaptor.create(MINUS149);
                    adaptor.addChild(root_0, MINUS149_tree);
                    }
                    N150=(Token)match(input,N,FOLLOW_N_in_inner_value1530); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    N150_tree = (CommonTree)adaptor.create(N150);
                    adaptor.addChild(root_0, N150_tree);
                    }

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:352:3: N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    N151=(Token)match(input,N,FOLLOW_N_in_inner_value1534); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    N151_tree = (CommonTree)adaptor.create(N151);
                    adaptor.addChild(root_0, N151_tree);
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "inner_value"

    public static class sql_condition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "sql_condition"
    // D:\\tools\\antlr\\test\\MySQLParser.g:355:1: sql_condition : condition_or ;
    public final MySQLParserParser.sql_condition_return sql_condition() throws RecognitionException {
        MySQLParserParser.sql_condition_return retval = new MySQLParserParser.sql_condition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.condition_or_return condition_or152 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:356:2: ( condition_or )
            // D:\\tools\\antlr\\test\\MySQLParser.g:356:3: condition_or
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_condition_or_in_sql_condition1545);
            condition_or152=condition_or();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_or152.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "sql_condition"

    public static class relational_op_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "relational_op"
    // D:\\tools\\antlr\\test\\MySQLParser.g:359:1: relational_op : ( EQ | LTH | GTH | NOT_EQ | LEQ | GEQ );
    public final MySQLParserParser.relational_op_return relational_op() throws RecognitionException {
        MySQLParserParser.relational_op_return retval = new MySQLParserParser.relational_op_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set153=null;

        CommonTree set153_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:360:2: ( EQ | LTH | GTH | NOT_EQ | LEQ | GEQ )
            // D:\\tools\\antlr\\test\\MySQLParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set153=(Token)input.LT(1);
            if ( input.LA(1)==EQ||(input.LA(1)>=LTH && input.LA(1)<=GEQ) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set153));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "relational_op"

    public static class fromClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fromClause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:363:1: fromClause : 'FROM' selected_table ;
    public final MySQLParserParser.fromClause_return fromClause() throws RecognitionException {
        MySQLParserParser.fromClause_return retval = new MySQLParserParser.fromClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal154=null;
        MySQLParserParser.selected_table_return selected_table155 = null;


        CommonTree string_literal154_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:364:2: ( 'FROM' selected_table )
            // D:\\tools\\antlr\\test\\MySQLParser.g:364:3: 'FROM' selected_table
            {
            root_0 = (CommonTree)adaptor.nil();

            string_literal154=(Token)match(input,123,FOLLOW_123_in_fromClause1586); if (state.failed) return retval;
            pushFollow(FOLLOW_selected_table_in_fromClause1589);
            selected_table155=selected_table();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, selected_table155.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "fromClause"

    public static class select_list_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "select_list"
    // D:\\tools\\antlr\\test\\MySQLParser.g:367:1: select_list : ( displayed_column ( COMMA displayed_column )* -> ^( SELECT_LIST ( displayed_column )+ ) | distinct_col -> ^( SELECT_LIST distinct_col ) );
    public final MySQLParserParser.select_list_return select_list() throws RecognitionException {
        MySQLParserParser.select_list_return retval = new MySQLParserParser.select_list_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA157=null;
        MySQLParserParser.displayed_column_return displayed_column156 = null;

        MySQLParserParser.displayed_column_return displayed_column158 = null;

        MySQLParserParser.distinct_col_return distinct_col159 = null;


        CommonTree COMMA157_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_displayed_column=new RewriteRuleSubtreeStream(adaptor,"rule displayed_column");
        RewriteRuleSubtreeStream stream_distinct_col=new RewriteRuleSubtreeStream(adaptor,"rule distinct_col");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:368:2: ( displayed_column ( COMMA displayed_column )* -> ^( SELECT_LIST ( displayed_column )+ ) | distinct_col -> ^( SELECT_LIST distinct_col ) )
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==ASTERISK||(LA32_0>=ID && LA32_0<=N)) ) {
                alt32=1;
            }
            else if ( (LA32_0==109) ) {
                alt32=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 32, 0, input);

                throw nvae;
            }
            switch (alt32) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:368:3: displayed_column ( COMMA displayed_column )*
                    {
                    pushFollow(FOLLOW_displayed_column_in_select_list1599);
                    displayed_column156=displayed_column();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_displayed_column.add(displayed_column156.getTree());
                    // D:\\tools\\antlr\\test\\MySQLParser.g:368:20: ( COMMA displayed_column )*
                    loop31:
                    do {
                        int alt31=2;
                        int LA31_0 = input.LA(1);

                        if ( (LA31_0==COMMA) ) {
                            alt31=1;
                        }


                        switch (alt31) {
                    	case 1 :
                    	    // D:\\tools\\antlr\\test\\MySQLParser.g:368:22: COMMA displayed_column
                    	    {
                    	    COMMA157=(Token)match(input,COMMA,FOLLOW_COMMA_in_select_list1603); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA157);

                    	    pushFollow(FOLLOW_displayed_column_in_select_list1605);
                    	    displayed_column158=displayed_column();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_displayed_column.add(displayed_column158.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop31;
                        }
                    } while (true);



                    // AST REWRITE
                    // elements: displayed_column
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 368:47: -> ^( SELECT_LIST ( displayed_column )+ )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:368:49: ^( SELECT_LIST ( displayed_column )+ )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SELECT_LIST, "SELECT_LIST"), root_1);

                        if ( !(stream_displayed_column.hasNext()) ) {
                            throw new RewriteEarlyExitException();
                        }
                        while ( stream_displayed_column.hasNext() ) {
                            adaptor.addChild(root_1, stream_displayed_column.nextTree());

                        }
                        stream_displayed_column.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:369:3: distinct_col
                    {
                    pushFollow(FOLLOW_distinct_col_in_select_list1619);
                    distinct_col159=distinct_col();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_distinct_col.add(distinct_col159.getTree());


                    // AST REWRITE
                    // elements: distinct_col
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 369:16: -> ^( SELECT_LIST distinct_col )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:369:18: ^( SELECT_LIST distinct_col )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SELECT_LIST, "SELECT_LIST"), root_1);

                        adaptor.addChild(root_1, stream_distinct_col.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "select_list"

    public static class displayed_column_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "displayed_column"
    // D:\\tools\\antlr\\test\\MySQLParser.g:372:1: displayed_column : ({...}? ID ( LPAREN ( values_func )? RPAREN ) ( alias )? -> ^( ID ( values_func )? ( alias )? ) | {...}? ID ( alias )? -> ^( CONSIST ID ( alias )? ) | {...}? ID LPAREN distinct_col RPAREN ( alias )? -> ^( ID distinct_col ( alias )? ) | ( table_alias )? column ( alias )? -> ^( COLUMN ( table_alias )? column ( alias )? ) );
    public final MySQLParserParser.displayed_column_return displayed_column() throws RecognitionException {
        MySQLParserParser.displayed_column_return retval = new MySQLParserParser.displayed_column_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID160=null;
        Token LPAREN161=null;
        Token RPAREN163=null;
        Token ID165=null;
        Token ID167=null;
        Token LPAREN168=null;
        Token RPAREN170=null;
        MySQLParserParser.values_func_return values_func162 = null;

        MySQLParserParser.alias_return alias164 = null;

        MySQLParserParser.alias_return alias166 = null;

        MySQLParserParser.distinct_col_return distinct_col169 = null;

        MySQLParserParser.alias_return alias171 = null;

        MySQLParserParser.table_alias_return table_alias172 = null;

        MySQLParserParser.column_return column173 = null;

        MySQLParserParser.alias_return alias174 = null;


        CommonTree ID160_tree=null;
        CommonTree LPAREN161_tree=null;
        CommonTree RPAREN163_tree=null;
        CommonTree ID165_tree=null;
        CommonTree ID167_tree=null;
        CommonTree LPAREN168_tree=null;
        CommonTree RPAREN170_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_alias=new RewriteRuleSubtreeStream(adaptor,"rule alias");
        RewriteRuleSubtreeStream stream_column=new RewriteRuleSubtreeStream(adaptor,"rule column");
        RewriteRuleSubtreeStream stream_values_func=new RewriteRuleSubtreeStream(adaptor,"rule values_func");
        RewriteRuleSubtreeStream stream_table_alias=new RewriteRuleSubtreeStream(adaptor,"rule table_alias");
        RewriteRuleSubtreeStream stream_distinct_col=new RewriteRuleSubtreeStream(adaptor,"rule distinct_col");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:373:2: ({...}? ID ( LPAREN ( values_func )? RPAREN ) ( alias )? -> ^( ID ( values_func )? ( alias )? ) | {...}? ID ( alias )? -> ^( CONSIST ID ( alias )? ) | {...}? ID LPAREN distinct_col RPAREN ( alias )? -> ^( ID distinct_col ( alias )? ) | ( table_alias )? column ( alias )? -> ^( COLUMN ( table_alias )? column ( alias )? ) )
            int alt39=4;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==ID) ) {
                int LA39_1 = input.LA(2);

                if ( (LA39_1==LPAREN) ) {
                    int LA39_3 = input.LA(3);

                    if ( (LA39_3==INTERVAL||(LA39_3>=LPAREN && LA39_3<=RPAREN)||(LA39_3>=PLUS && LA39_3<=ASTERISK)||(LA39_3>=ID && LA39_3<=N)||LA39_3==NUMBER||LA39_3==QUOTED_STRING||LA39_3==118||(LA39_3>=121 && LA39_3<=122)||(LA39_3>=133 && LA39_3<=134)) ) {
                        alt39=1;
                    }
                    else if ( (LA39_3==109) ) {
                        alt39=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 39, 3, input);

                        throw nvae;
                    }
                }
                else if ( ((consistStr.containsKey(input.LT(1).getText().toUpperCase()))) ) {
                    alt39=2;
                }
                else if ( (true) ) {
                    alt39=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 39, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA39_0==ASTERISK||LA39_0==N) ) {
                alt39=4;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 39, 0, input);

                throw nvae;
            }
            switch (alt39) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:373:3: {...}? ID ( LPAREN ( values_func )? RPAREN ) ( alias )?
                    {
                    if ( !((functionMap.containsKey(input.LT(1).getText().toUpperCase()))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "displayed_column", "functionMap.containsKey(input.LT(1).getText().toUpperCase())");
                    }
                    ID160=(Token)match(input,ID,FOLLOW_ID_in_displayed_column1640); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID160);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:373:70: ( LPAREN ( values_func )? RPAREN )
                    // D:\\tools\\antlr\\test\\MySQLParser.g:373:71: LPAREN ( values_func )? RPAREN
                    {
                    LPAREN161=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_displayed_column1643); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN161);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:373:78: ( values_func )?
                    int alt33=2;
                    int LA33_0 = input.LA(1);

                    if ( (LA33_0==INTERVAL||LA33_0==LPAREN||(LA33_0>=PLUS && LA33_0<=ASTERISK)||(LA33_0>=ID && LA33_0<=N)||LA33_0==NUMBER||LA33_0==QUOTED_STRING||LA33_0==118||(LA33_0>=121 && LA33_0<=122)||(LA33_0>=133 && LA33_0<=134)) ) {
                        alt33=1;
                    }
                    switch (alt33) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:373:78: values_func
                            {
                            pushFollow(FOLLOW_values_func_in_displayed_column1645);
                            values_func162=values_func();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_values_func.add(values_func162.getTree());

                            }
                            break;

                    }

                    RPAREN163=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_displayed_column1648); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN163);


                    }

                    // D:\\tools\\antlr\\test\\MySQLParser.g:373:99: ( alias )?
                    int alt34=2;
                    int LA34_0 = input.LA(1);

                    if ( (LA34_0==ID||LA34_0==135) ) {
                        alt34=1;
                    }
                    switch (alt34) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:373:99: alias
                            {
                            pushFollow(FOLLOW_alias_in_displayed_column1651);
                            alias164=alias();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_alias.add(alias164.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: alias, ID, values_func
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 373:105: -> ^( ID ( values_func )? ( alias )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:373:107: ^( ID ( values_func )? ( alias )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_ID.nextNode(), root_1);

                        // D:\\tools\\antlr\\test\\MySQLParser.g:373:112: ( values_func )?
                        if ( stream_values_func.hasNext() ) {
                            adaptor.addChild(root_1, stream_values_func.nextTree());

                        }
                        stream_values_func.reset();
                        // D:\\tools\\antlr\\test\\MySQLParser.g:373:125: ( alias )?
                        if ( stream_alias.hasNext() ) {
                            adaptor.addChild(root_1, stream_alias.nextTree());

                        }
                        stream_alias.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:374:3: {...}? ID ( alias )?
                    {
                    if ( !((consistStr.containsKey(input.LT(1).getText().toUpperCase()))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "displayed_column", "consistStr.containsKey(input.LT(1).getText().toUpperCase())");
                    }
                    ID165=(Token)match(input,ID,FOLLOW_ID_in_displayed_column1668); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID165);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:374:69: ( alias )?
                    int alt35=2;
                    int LA35_0 = input.LA(1);

                    if ( (LA35_0==ID||LA35_0==135) ) {
                        alt35=1;
                    }
                    switch (alt35) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:374:69: alias
                            {
                            pushFollow(FOLLOW_alias_in_displayed_column1670);
                            alias166=alias();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_alias.add(alias166.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: ID, alias
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 374:76: -> ^( CONSIST ID ( alias )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:374:78: ^( CONSIST ID ( alias )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(CONSIST, "CONSIST"), root_1);

                        adaptor.addChild(root_1, stream_ID.nextNode());
                        // D:\\tools\\antlr\\test\\MySQLParser.g:374:91: ( alias )?
                        if ( stream_alias.hasNext() ) {
                            adaptor.addChild(root_1, stream_alias.nextTree());

                        }
                        stream_alias.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:375:3: {...}? ID LPAREN distinct_col RPAREN ( alias )?
                    {
                    if ( !((functionMap.containsKey(input.LT(1).getText().toUpperCase()))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "displayed_column", "functionMap.containsKey(input.LT(1).getText().toUpperCase())");
                    }
                    ID167=(Token)match(input,ID,FOLLOW_ID_in_displayed_column1687); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID167);

                    LPAREN168=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_displayed_column1689); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN168);

                    pushFollow(FOLLOW_distinct_col_in_displayed_column1691);
                    distinct_col169=distinct_col();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_distinct_col.add(distinct_col169.getTree());
                    RPAREN170=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_displayed_column1693); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN170);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:375:97: ( alias )?
                    int alt36=2;
                    int LA36_0 = input.LA(1);

                    if ( (LA36_0==ID||LA36_0==135) ) {
                        alt36=1;
                    }
                    switch (alt36) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:375:97: alias
                            {
                            pushFollow(FOLLOW_alias_in_displayed_column1695);
                            alias171=alias();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_alias.add(alias171.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: alias, distinct_col, ID
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 375:103: -> ^( ID distinct_col ( alias )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:375:105: ^( ID distinct_col ( alias )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_ID.nextNode(), root_1);

                        adaptor.addChild(root_1, stream_distinct_col.nextTree());
                        // D:\\tools\\antlr\\test\\MySQLParser.g:375:123: ( alias )?
                        if ( stream_alias.hasNext() ) {
                            adaptor.addChild(root_1, stream_alias.nextTree());

                        }
                        stream_alias.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:380:3: ( table_alias )? column ( alias )?
                    {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:380:3: ( table_alias )?
                    int alt37=2;
                    int LA37_0 = input.LA(1);

                    if ( (LA37_0==ID) ) {
                        int LA37_1 = input.LA(2);

                        if ( (LA37_1==DOT) ) {
                            alt37=1;
                        }
                    }
                    switch (alt37) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:380:3: table_alias
                            {
                            pushFollow(FOLLOW_table_alias_in_displayed_column1713);
                            table_alias172=table_alias();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_table_alias.add(table_alias172.getTree());

                            }
                            break;

                    }

                    pushFollow(FOLLOW_column_in_displayed_column1717);
                    column173=column();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_column.add(column173.getTree());
                    // D:\\tools\\antlr\\test\\MySQLParser.g:380:24: ( alias )?
                    int alt38=2;
                    int LA38_0 = input.LA(1);

                    if ( (LA38_0==ID||LA38_0==135) ) {
                        alt38=1;
                    }
                    switch (alt38) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:380:25: alias
                            {
                            pushFollow(FOLLOW_alias_in_displayed_column1720);
                            alias174=alias();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_alias.add(alias174.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: table_alias, alias, column
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 380:32: -> ^( COLUMN ( table_alias )? column ( alias )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:380:34: ^( COLUMN ( table_alias )? column ( alias )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(COLUMN, "COLUMN"), root_1);

                        // D:\\tools\\antlr\\test\\MySQLParser.g:380:43: ( table_alias )?
                        if ( stream_table_alias.hasNext() ) {
                            adaptor.addChild(root_1, stream_table_alias.nextTree());

                        }
                        stream_table_alias.reset();
                        adaptor.addChild(root_1, stream_column.nextTree());
                        // D:\\tools\\antlr\\test\\MySQLParser.g:380:63: ( alias )?
                        if ( stream_alias.hasNext() ) {
                            adaptor.addChild(root_1, stream_alias.nextTree());

                        }
                        stream_alias.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "displayed_column"

    public static class distinct_col_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "distinct_col"
    // D:\\tools\\antlr\\test\\MySQLParser.g:384:1: distinct_col : distinct displayed_column ( COMMA displayed_column )* -> ^( DISTINCT ( displayed_column )+ ) ;
    public final MySQLParserParser.distinct_col_return distinct_col() throws RecognitionException {
        MySQLParserParser.distinct_col_return retval = new MySQLParserParser.distinct_col_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA177=null;
        MySQLParserParser.distinct_return distinct175 = null;

        MySQLParserParser.displayed_column_return displayed_column176 = null;

        MySQLParserParser.displayed_column_return displayed_column178 = null;


        CommonTree COMMA177_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_displayed_column=new RewriteRuleSubtreeStream(adaptor,"rule displayed_column");
        RewriteRuleSubtreeStream stream_distinct=new RewriteRuleSubtreeStream(adaptor,"rule distinct");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:385:2: ( distinct displayed_column ( COMMA displayed_column )* -> ^( DISTINCT ( displayed_column )+ ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:385:3: distinct displayed_column ( COMMA displayed_column )*
            {
            pushFollow(FOLLOW_distinct_in_distinct_col1747);
            distinct175=distinct();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_distinct.add(distinct175.getTree());
            pushFollow(FOLLOW_displayed_column_in_distinct_col1749);
            displayed_column176=displayed_column();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_displayed_column.add(displayed_column176.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:385:29: ( COMMA displayed_column )*
            loop40:
            do {
                int alt40=2;
                int LA40_0 = input.LA(1);

                if ( (LA40_0==COMMA) ) {
                    alt40=1;
                }


                switch (alt40) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:385:31: COMMA displayed_column
            	    {
            	    COMMA177=(Token)match(input,COMMA,FOLLOW_COMMA_in_distinct_col1753); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA177);

            	    pushFollow(FOLLOW_displayed_column_in_distinct_col1755);
            	    displayed_column178=displayed_column();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_displayed_column.add(displayed_column178.getTree());

            	    }
            	    break;

            	default :
            	    break loop40;
                }
            } while (true);



            // AST REWRITE
            // elements: displayed_column
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 385:56: -> ^( DISTINCT ( displayed_column )+ )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:385:58: ^( DISTINCT ( displayed_column )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(DISTINCT, "DISTINCT"), root_1);

                if ( !(stream_displayed_column.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_displayed_column.hasNext() ) {
                    adaptor.addChild(root_1, stream_displayed_column.nextTree());

                }
                stream_displayed_column.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "distinct_col"

    public static class columnNameAfterWhere_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnNameAfterWhere"
    // D:\\tools\\antlr\\test\\MySQLParser.g:388:1: columnNameAfterWhere : ( ( table_alias )? identifier -> ^( ASC identifier ( table_alias )? ) | ( table_alias )? identifier ASC -> ^( ASC identifier ( table_alias )? ) | ( table_alias )? identifier DESC -> ^( DESC identifier ( table_alias )? ) );
    public final MySQLParserParser.columnNameAfterWhere_return columnNameAfterWhere() throws RecognitionException {
        MySQLParserParser.columnNameAfterWhere_return retval = new MySQLParserParser.columnNameAfterWhere_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ASC183=null;
        Token DESC186=null;
        MySQLParserParser.table_alias_return table_alias179 = null;

        MySQLParserParser.identifier_return identifier180 = null;

        MySQLParserParser.table_alias_return table_alias181 = null;

        MySQLParserParser.identifier_return identifier182 = null;

        MySQLParserParser.table_alias_return table_alias184 = null;

        MySQLParserParser.identifier_return identifier185 = null;


        CommonTree ASC183_tree=null;
        CommonTree DESC186_tree=null;
        RewriteRuleTokenStream stream_DESC=new RewriteRuleTokenStream(adaptor,"token DESC");
        RewriteRuleTokenStream stream_ASC=new RewriteRuleTokenStream(adaptor,"token ASC");
        RewriteRuleSubtreeStream stream_table_alias=new RewriteRuleSubtreeStream(adaptor,"rule table_alias");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:389:2: ( ( table_alias )? identifier -> ^( ASC identifier ( table_alias )? ) | ( table_alias )? identifier ASC -> ^( ASC identifier ( table_alias )? ) | ( table_alias )? identifier DESC -> ^( DESC identifier ( table_alias )? ) )
            int alt44=3;
            int LA44_0 = input.LA(1);

            if ( (LA44_0==ID) ) {
                switch ( input.LA(2) ) {
                case ASC:
                    {
                    alt44=2;
                    }
                    break;
                case DOT:
                    {
                    int LA44_3 = input.LA(3);

                    if ( (LA44_3==ID) ) {
                        switch ( input.LA(4) ) {
                        case ASC:
                            {
                            alt44=2;
                            }
                            break;
                        case DESC:
                            {
                            alt44=3;
                            }
                            break;
                        case EOF:
                        case COMMA:
                        case RPAREN:
                        case 106:
                        case 107:
                        case 136:
                        case 138:
                        case 141:
                        case 142:
                        case 143:
                            {
                            alt44=1;
                            }
                            break;
                        default:
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 44, 6, input);

                            throw nvae;
                        }

                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 44, 3, input);

                        throw nvae;
                    }
                    }
                    break;
                case DESC:
                    {
                    alt44=3;
                    }
                    break;
                case EOF:
                case COMMA:
                case RPAREN:
                case 106:
                case 107:
                case 136:
                case 138:
                case 141:
                case 142:
                case 143:
                    {
                    alt44=1;
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 44, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 44, 0, input);

                throw nvae;
            }
            switch (alt44) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:389:3: ( table_alias )? identifier
                    {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:389:3: ( table_alias )?
                    int alt41=2;
                    int LA41_0 = input.LA(1);

                    if ( (LA41_0==ID) ) {
                        int LA41_1 = input.LA(2);

                        if ( (LA41_1==DOT) ) {
                            alt41=1;
                        }
                    }
                    switch (alt41) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:389:3: table_alias
                            {
                            pushFollow(FOLLOW_table_alias_in_columnNameAfterWhere1777);
                            table_alias179=table_alias();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_table_alias.add(table_alias179.getTree());

                            }
                            break;

                    }

                    pushFollow(FOLLOW_identifier_in_columnNameAfterWhere1780);
                    identifier180=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_identifier.add(identifier180.getTree());


                    // AST REWRITE
                    // elements: table_alias, identifier
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 389:28: -> ^( ASC identifier ( table_alias )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:389:30: ^( ASC identifier ( table_alias )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ASC, "ASC"), root_1);

                        adaptor.addChild(root_1, stream_identifier.nextTree());
                        // D:\\tools\\antlr\\test\\MySQLParser.g:389:47: ( table_alias )?
                        if ( stream_table_alias.hasNext() ) {
                            adaptor.addChild(root_1, stream_table_alias.nextTree());

                        }
                        stream_table_alias.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:390:3: ( table_alias )? identifier ASC
                    {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:390:3: ( table_alias )?
                    int alt42=2;
                    int LA42_0 = input.LA(1);

                    if ( (LA42_0==ID) ) {
                        int LA42_1 = input.LA(2);

                        if ( (LA42_1==DOT) ) {
                            alt42=1;
                        }
                    }
                    switch (alt42) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:390:3: table_alias
                            {
                            pushFollow(FOLLOW_table_alias_in_columnNameAfterWhere1795);
                            table_alias181=table_alias();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_table_alias.add(table_alias181.getTree());

                            }
                            break;

                    }

                    pushFollow(FOLLOW_identifier_in_columnNameAfterWhere1798);
                    identifier182=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_identifier.add(identifier182.getTree());
                    ASC183=(Token)match(input,ASC,FOLLOW_ASC_in_columnNameAfterWhere1801); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ASC.add(ASC183);



                    // AST REWRITE
                    // elements: identifier, ASC, table_alias
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 390:33: -> ^( ASC identifier ( table_alias )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:390:35: ^( ASC identifier ( table_alias )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_ASC.nextNode(), root_1);

                        adaptor.addChild(root_1, stream_identifier.nextTree());
                        // D:\\tools\\antlr\\test\\MySQLParser.g:390:52: ( table_alias )?
                        if ( stream_table_alias.hasNext() ) {
                            adaptor.addChild(root_1, stream_table_alias.nextTree());

                        }
                        stream_table_alias.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:391:3: ( table_alias )? identifier DESC
                    {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:391:3: ( table_alias )?
                    int alt43=2;
                    int LA43_0 = input.LA(1);

                    if ( (LA43_0==ID) ) {
                        int LA43_1 = input.LA(2);

                        if ( (LA43_1==DOT) ) {
                            alt43=1;
                        }
                    }
                    switch (alt43) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:391:3: table_alias
                            {
                            pushFollow(FOLLOW_table_alias_in_columnNameAfterWhere1816);
                            table_alias184=table_alias();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_table_alias.add(table_alias184.getTree());

                            }
                            break;

                    }

                    pushFollow(FOLLOW_identifier_in_columnNameAfterWhere1819);
                    identifier185=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_identifier.add(identifier185.getTree());
                    DESC186=(Token)match(input,DESC,FOLLOW_DESC_in_columnNameAfterWhere1822); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DESC.add(DESC186);



                    // AST REWRITE
                    // elements: table_alias, DESC, identifier
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 391:33: -> ^( DESC identifier ( table_alias )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:391:35: ^( DESC identifier ( table_alias )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_DESC.nextNode(), root_1);

                        adaptor.addChild(root_1, stream_identifier.nextTree());
                        // D:\\tools\\antlr\\test\\MySQLParser.g:391:53: ( table_alias )?
                        if ( stream_table_alias.hasNext() ) {
                            adaptor.addChild(root_1, stream_table_alias.nextTree());

                        }
                        stream_table_alias.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnNameAfterWhere"

    public static class columnNameInUpdate_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnNameInUpdate"
    // D:\\tools\\antlr\\test\\MySQLParser.g:394:1: columnNameInUpdate : ( table_alias )? identifier ;
    public final MySQLParserParser.columnNameInUpdate_return columnNameInUpdate() throws RecognitionException {
        MySQLParserParser.columnNameInUpdate_return retval = new MySQLParserParser.columnNameInUpdate_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.table_alias_return table_alias187 = null;

        MySQLParserParser.identifier_return identifier188 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:395:2: ( ( table_alias )? identifier )
            // D:\\tools\\antlr\\test\\MySQLParser.g:395:3: ( table_alias )? identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\tools\\antlr\\test\\MySQLParser.g:395:3: ( table_alias )?
            int alt45=2;
            int LA45_0 = input.LA(1);

            if ( (LA45_0==ID) ) {
                int LA45_1 = input.LA(2);

                if ( (LA45_1==DOT) ) {
                    alt45=1;
                }
            }
            switch (alt45) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:395:3: table_alias
                    {
                    pushFollow(FOLLOW_table_alias_in_columnNameInUpdate1843);
                    table_alias187=table_alias();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, table_alias187.getTree());

                    }
                    break;

            }

            pushFollow(FOLLOW_identifier_in_columnNameInUpdate1846);
            identifier188=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier188.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnNameInUpdate"

    public static class table_alias_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "table_alias"
    // D:\\tools\\antlr\\test\\MySQLParser.g:398:1: table_alias : identifier DOT -> ^( COL_TAB identifier ) ;
    public final MySQLParserParser.table_alias_return table_alias() throws RecognitionException {
        MySQLParserParser.table_alias_return retval = new MySQLParserParser.table_alias_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DOT190=null;
        MySQLParserParser.identifier_return identifier189 = null;


        CommonTree DOT190_tree=null;
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:399:2: ( identifier DOT -> ^( COL_TAB identifier ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:399:3: identifier DOT
            {
            pushFollow(FOLLOW_identifier_in_table_alias1858);
            identifier189=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(identifier189.getTree());
            DOT190=(Token)match(input,DOT,FOLLOW_DOT_in_table_alias1860); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DOT.add(DOT190);



            // AST REWRITE
            // elements: identifier
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 399:17: -> ^( COL_TAB identifier )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:399:19: ^( COL_TAB identifier )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(COL_TAB, "COL_TAB"), root_1);

                adaptor.addChild(root_1, stream_identifier.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "table_alias"

    public static class column_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "column"
    // D:\\tools\\antlr\\test\\MySQLParser.g:401:1: column : ( ASTERISK | {...}? N | identifier );
    public final MySQLParserParser.column_return column() throws RecognitionException {
        MySQLParserParser.column_return retval = new MySQLParserParser.column_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ASTERISK191=null;
        Token N192=null;
        MySQLParserParser.identifier_return identifier193 = null;


        CommonTree ASTERISK191_tree=null;
        CommonTree N192_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:402:2: ( ASTERISK | {...}? N | identifier )
            int alt46=3;
            switch ( input.LA(1) ) {
            case ASTERISK:
                {
                alt46=1;
                }
                break;
            case N:
                {
                alt46=2;
                }
                break;
            case ID:
                {
                alt46=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 46, 0, input);

                throw nvae;
            }

            switch (alt46) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:402:3: ASTERISK
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    ASTERISK191=(Token)match(input,ASTERISK,FOLLOW_ASTERISK_in_column1876); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    ASTERISK191_tree = (CommonTree)adaptor.create(ASTERISK191);
                    adaptor.addChild(root_0, ASTERISK191_tree);
                    }

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:403:3: {...}? N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    if ( !(("1".equals(input.LT(1).getText()))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "column", "\"1\".equals(input.LT(1).getText())");
                    }
                    N192=(Token)match(input,N,FOLLOW_N_in_column1882); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    N192_tree = (CommonTree)adaptor.create(N192);
                    adaptor.addChild(root_0, N192_tree);
                    }

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:404:3: identifier
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_identifier_in_column1886);
                    identifier193=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier193.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "column"

    public static class values_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "values"
    // D:\\tools\\antlr\\test\\MySQLParser.g:407:1: values : expr ( COMMA expr )* -> ^( INSERT_VAL ( expr )* ) ;
    public final MySQLParserParser.values_return values() throws RecognitionException {
        MySQLParserParser.values_return retval = new MySQLParserParser.values_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA195=null;
        MySQLParserParser.expr_return expr194 = null;

        MySQLParserParser.expr_return expr196 = null;


        CommonTree COMMA195_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_expr=new RewriteRuleSubtreeStream(adaptor,"rule expr");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:408:2: ( expr ( COMMA expr )* -> ^( INSERT_VAL ( expr )* ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:408:3: expr ( COMMA expr )*
            {
            pushFollow(FOLLOW_expr_in_values1898);
            expr194=expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expr.add(expr194.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:408:8: ( COMMA expr )*
            loop47:
            do {
                int alt47=2;
                int LA47_0 = input.LA(1);

                if ( (LA47_0==COMMA) ) {
                    alt47=1;
                }


                switch (alt47) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:408:10: COMMA expr
            	    {
            	    COMMA195=(Token)match(input,COMMA,FOLLOW_COMMA_in_values1902); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA195);

            	    pushFollow(FOLLOW_expr_in_values1904);
            	    expr196=expr();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expr.add(expr196.getTree());

            	    }
            	    break;

            	default :
            	    break loop47;
                }
            } while (true);



            // AST REWRITE
            // elements: expr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 408:23: -> ^( INSERT_VAL ( expr )* )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:408:25: ^( INSERT_VAL ( expr )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(INSERT_VAL, "INSERT_VAL"), root_1);

                // D:\\tools\\antlr\\test\\MySQLParser.g:408:38: ( expr )*
                while ( stream_expr.hasNext() ) {
                    adaptor.addChild(root_1, stream_expr.nextTree());

                }
                stream_expr.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "values"

    public static class values_func_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "values_func"
    // D:\\tools\\antlr\\test\\MySQLParser.g:411:1: values_func : expr ( COMMA expr )* ;
    public final MySQLParserParser.values_func_return values_func() throws RecognitionException {
        MySQLParserParser.values_func_return retval = new MySQLParserParser.values_func_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA198=null;
        MySQLParserParser.expr_return expr197 = null;

        MySQLParserParser.expr_return expr199 = null;


        CommonTree COMMA198_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:412:2: ( expr ( COMMA expr )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:412:3: expr ( COMMA expr )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_expr_in_values_func1925);
            expr197=expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expr197.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:412:8: ( COMMA expr )*
            loop48:
            do {
                int alt48=2;
                int LA48_0 = input.LA(1);

                if ( (LA48_0==COMMA) ) {
                    alt48=1;
                }


                switch (alt48) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:412:10: COMMA expr
            	    {
            	    COMMA198=(Token)match(input,COMMA,FOLLOW_COMMA_in_values_func1929); if (state.failed) return retval;
            	    pushFollow(FOLLOW_expr_in_values_func1932);
            	    expr199=expr();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, expr199.getTree());

            	    }
            	    break;

            	default :
            	    break loop48;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "values_func"

    public static class value_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "value"
    // D:\\tools\\antlr\\test\\MySQLParser.g:415:1: value : ( N | NUMBER | '?' | LPAREN expr RPAREN | quoted_string -> ^( QUTED_STR quoted_string ) | column_spec );
    public final MySQLParserParser.value_return value() throws RecognitionException {
        MySQLParserParser.value_return retval = new MySQLParserParser.value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token N200=null;
        Token NUMBER201=null;
        Token char_literal202=null;
        Token LPAREN203=null;
        Token RPAREN205=null;
        MySQLParserParser.expr_return expr204 = null;

        MySQLParserParser.quoted_string_return quoted_string206 = null;

        MySQLParserParser.column_spec_return column_spec207 = null;


        CommonTree N200_tree=null;
        CommonTree NUMBER201_tree=null;
        CommonTree char_literal202_tree=null;
        CommonTree LPAREN203_tree=null;
        CommonTree RPAREN205_tree=null;
        RewriteRuleSubtreeStream stream_quoted_string=new RewriteRuleSubtreeStream(adaptor,"rule quoted_string");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:415:7: ( N | NUMBER | '?' | LPAREN expr RPAREN | quoted_string -> ^( QUTED_STR quoted_string ) | column_spec )
            int alt49=6;
            switch ( input.LA(1) ) {
            case N:
                {
                alt49=1;
                }
                break;
            case NUMBER:
                {
                alt49=2;
                }
                break;
            case 122:
                {
                alt49=3;
                }
                break;
            case LPAREN:
                {
                alt49=4;
                }
                break;
            case QUOTED_STRING:
                {
                alt49=5;
                }
                break;
            case ASTERISK:
            case ID:
                {
                alt49=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 49, 0, input);

                throw nvae;
            }

            switch (alt49) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:416:2: N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    N200=(Token)match(input,N,FOLLOW_N_in_value1948); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    N200_tree = (CommonTree)adaptor.create(N200);
                    adaptor.addChild(root_0, N200_tree);
                    }

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:417:3: NUMBER
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    NUMBER201=(Token)match(input,NUMBER,FOLLOW_NUMBER_in_value1952); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    NUMBER201_tree = (CommonTree)adaptor.create(NUMBER201);
                    adaptor.addChild(root_0, NUMBER201_tree);
                    }

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:418:3: '?'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    char_literal202=(Token)match(input,122,FOLLOW_122_in_value1956); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    char_literal202_tree = (CommonTree)adaptor.create(char_literal202);
                    adaptor.addChild(root_0, char_literal202_tree);
                    }

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:419:3: LPAREN expr RPAREN
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    LPAREN203=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_value1960); if (state.failed) return retval;
                    pushFollow(FOLLOW_expr_in_value1963);
                    expr204=expr();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, expr204.getTree());
                    RPAREN205=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_value1965); if (state.failed) return retval;

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:420:3: quoted_string
                    {
                    pushFollow(FOLLOW_quoted_string_in_value1970);
                    quoted_string206=quoted_string();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_quoted_string.add(quoted_string206.getTree());


                    // AST REWRITE
                    // elements: quoted_string
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 420:17: -> ^( QUTED_STR quoted_string )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:420:19: ^( QUTED_STR quoted_string )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(QUTED_STR, "QUTED_STR"), root_1);

                        adaptor.addChild(root_1, stream_quoted_string.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:421:3: column_spec
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_column_spec_in_value1981);
                    column_spec207=column_spec();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, column_spec207.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "value"

    public static class column_specs_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "column_specs"
    // D:\\tools\\antlr\\test\\MySQLParser.g:425:1: column_specs : column_spec ( COMMA column_spec )* -> ^( COLUMNS ( column_spec )+ ) ;
    public final MySQLParserParser.column_specs_return column_specs() throws RecognitionException {
        MySQLParserParser.column_specs_return retval = new MySQLParserParser.column_specs_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA209=null;
        MySQLParserParser.column_spec_return column_spec208 = null;

        MySQLParserParser.column_spec_return column_spec210 = null;


        CommonTree COMMA209_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_column_spec=new RewriteRuleSubtreeStream(adaptor,"rule column_spec");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:426:2: ( column_spec ( COMMA column_spec )* -> ^( COLUMNS ( column_spec )+ ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:426:4: column_spec ( COMMA column_spec )*
            {
            pushFollow(FOLLOW_column_spec_in_column_specs1993);
            column_spec208=column_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_column_spec.add(column_spec208.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:426:16: ( COMMA column_spec )*
            loop50:
            do {
                int alt50=2;
                int LA50_0 = input.LA(1);

                if ( (LA50_0==COMMA) ) {
                    alt50=1;
                }


                switch (alt50) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:426:18: COMMA column_spec
            	    {
            	    COMMA209=(Token)match(input,COMMA,FOLLOW_COMMA_in_column_specs1997); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA209);

            	    pushFollow(FOLLOW_column_spec_in_column_specs1999);
            	    column_spec210=column_spec();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_column_spec.add(column_spec210.getTree());

            	    }
            	    break;

            	default :
            	    break loop50;
                }
            } while (true);



            // AST REWRITE
            // elements: column_spec
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 426:38: -> ^( COLUMNS ( column_spec )+ )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:426:40: ^( COLUMNS ( column_spec )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(COLUMNS, "COLUMNS"), root_1);

                if ( !(stream_column_spec.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_column_spec.hasNext() ) {
                    adaptor.addChild(root_1, stream_column_spec.nextTree());

                }
                stream_column_spec.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "column_specs"

    public static class selected_table_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "selected_table"
    // D:\\tools\\antlr\\test\\MySQLParser.g:429:1: selected_table : a_table ( COMMA a_table )* -> ^( TABLENAMES ( a_table )+ ) ;
    public final MySQLParserParser.selected_table_return selected_table() throws RecognitionException {
        MySQLParserParser.selected_table_return retval = new MySQLParserParser.selected_table_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMMA212=null;
        MySQLParserParser.a_table_return a_table211 = null;

        MySQLParserParser.a_table_return a_table213 = null;


        CommonTree COMMA212_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_a_table=new RewriteRuleSubtreeStream(adaptor,"rule a_table");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:430:2: ( a_table ( COMMA a_table )* -> ^( TABLENAMES ( a_table )+ ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:430:3: a_table ( COMMA a_table )*
            {
            pushFollow(FOLLOW_a_table_in_selected_table2020);
            a_table211=a_table();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_a_table.add(a_table211.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:430:11: ( COMMA a_table )*
            loop51:
            do {
                int alt51=2;
                int LA51_0 = input.LA(1);

                if ( (LA51_0==COMMA) ) {
                    alt51=1;
                }


                switch (alt51) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:430:12: COMMA a_table
            	    {
            	    COMMA212=(Token)match(input,COMMA,FOLLOW_COMMA_in_selected_table2023); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_COMMA.add(COMMA212);

            	    pushFollow(FOLLOW_a_table_in_selected_table2025);
            	    a_table213=a_table();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_a_table.add(a_table213.getTree());

            	    }
            	    break;

            	default :
            	    break loop51;
                }
            } while (true);



            // AST REWRITE
            // elements: a_table
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 430:27: -> ^( TABLENAMES ( a_table )+ )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:430:29: ^( TABLENAMES ( a_table )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TABLENAMES, "TABLENAMES"), root_1);

                if ( !(stream_a_table.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_a_table.hasNext() ) {
                    adaptor.addChild(root_1, stream_a_table.nextTree());

                }
                stream_a_table.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "selected_table"

    public static class a_table_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "a_table"
    // D:\\tools\\antlr\\test\\MySQLParser.g:433:1: a_table : table_spec ( alias )? ( join_claus )? -> ^( TABLENAME table_spec ( alias )? ( join_claus )? ) ;
    public final MySQLParserParser.a_table_return a_table() throws RecognitionException {
        MySQLParserParser.a_table_return retval = new MySQLParserParser.a_table_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.table_spec_return table_spec214 = null;

        MySQLParserParser.alias_return alias215 = null;

        MySQLParserParser.join_claus_return join_claus216 = null;


        RewriteRuleSubtreeStream stream_join_claus=new RewriteRuleSubtreeStream(adaptor,"rule join_claus");
        RewriteRuleSubtreeStream stream_alias=new RewriteRuleSubtreeStream(adaptor,"rule alias");
        RewriteRuleSubtreeStream stream_table_spec=new RewriteRuleSubtreeStream(adaptor,"rule table_spec");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:434:2: ( table_spec ( alias )? ( join_claus )? -> ^( TABLENAME table_spec ( alias )? ( join_claus )? ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:434:3: table_spec ( alias )? ( join_claus )?
            {
            pushFollow(FOLLOW_table_spec_in_a_table2045);
            table_spec214=table_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_table_spec.add(table_spec214.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:434:15: ( alias )?
            int alt52=2;
            int LA52_0 = input.LA(1);

            if ( (LA52_0==ID||LA52_0==135) ) {
                alt52=1;
            }
            switch (alt52) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:434:15: alias
                    {
                    pushFollow(FOLLOW_alias_in_a_table2048);
                    alias215=alias();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_alias.add(alias215.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:434:22: ( join_claus )?
            int alt53=2;
            int LA53_0 = input.LA(1);

            if ( (LA53_0==124||(LA53_0>=126 && LA53_0<=131)) ) {
                alt53=1;
            }
            switch (alt53) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:434:22: join_claus
                    {
                    pushFollow(FOLLOW_join_claus_in_a_table2051);
                    join_claus216=join_claus();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_join_claus.add(join_claus216.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: join_claus, alias, table_spec
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 434:33: -> ^( TABLENAME table_spec ( alias )? ( join_claus )? )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:434:35: ^( TABLENAME table_spec ( alias )? ( join_claus )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TABLENAME, "TABLENAME"), root_1);

                adaptor.addChild(root_1, stream_table_spec.nextTree());
                // D:\\tools\\antlr\\test\\MySQLParser.g:434:58: ( alias )?
                if ( stream_alias.hasNext() ) {
                    adaptor.addChild(root_1, stream_alias.nextTree());

                }
                stream_alias.reset();
                // D:\\tools\\antlr\\test\\MySQLParser.g:434:65: ( join_claus )?
                if ( stream_join_claus.hasNext() ) {
                    adaptor.addChild(root_1, stream_join_claus.nextTree());

                }
                stream_join_claus.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "a_table"

    public static class table_spec_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "table_spec"
    // D:\\tools\\antlr\\test\\MySQLParser.g:436:1: table_spec : ( ( schema_name DOT )? table_name | subquery );
    public final MySQLParserParser.table_spec_return table_spec() throws RecognitionException {
        MySQLParserParser.table_spec_return retval = new MySQLParserParser.table_spec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DOT218=null;
        MySQLParserParser.schema_name_return schema_name217 = null;

        MySQLParserParser.table_name_return table_name219 = null;

        MySQLParserParser.subquery_return subquery220 = null;


        CommonTree DOT218_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:437:2: ( ( schema_name DOT )? table_name | subquery )
            int alt55=2;
            int LA55_0 = input.LA(1);

            if ( (LA55_0==ID) ) {
                alt55=1;
            }
            else if ( (LA55_0==LPAREN) ) {
                alt55=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 55, 0, input);

                throw nvae;
            }
            switch (alt55) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:437:4: ( schema_name DOT )? table_name
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    // D:\\tools\\antlr\\test\\MySQLParser.g:437:4: ( schema_name DOT )?
                    int alt54=2;
                    int LA54_0 = input.LA(1);

                    if ( (LA54_0==ID) ) {
                        int LA54_1 = input.LA(2);

                        if ( (LA54_1==DOT) ) {
                            alt54=1;
                        }
                    }
                    switch (alt54) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:437:6: schema_name DOT
                            {
                            pushFollow(FOLLOW_schema_name_in_table_spec2076);
                            schema_name217=schema_name();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) adaptor.addChild(root_0, schema_name217.getTree());
                            DOT218=(Token)match(input,DOT,FOLLOW_DOT_in_table_spec2078); if (state.failed) return retval;

                            }
                            break;

                    }

                    pushFollow(FOLLOW_table_name_in_table_spec2083);
                    table_name219=table_name();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, table_name219.getTree());

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:438:4: subquery
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_subquery_in_table_spec2089);
                    subquery220=subquery();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, subquery220.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "table_spec"

    public static class join_claus_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "join_claus"
    // D:\\tools\\antlr\\test\\MySQLParser.g:441:2: join_claus : ( join_type )? 'JOIN' table_spec ( alias )? 'ON' column_spec EQ column_spec -> ^( JOIN table_spec ( alias )? column_spec column_spec ( join_type )? ) ;
    public final MySQLParserParser.join_claus_return join_claus() throws RecognitionException {
        MySQLParserParser.join_claus_return retval = new MySQLParserParser.join_claus_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal222=null;
        Token string_literal225=null;
        Token EQ227=null;
        MySQLParserParser.join_type_return join_type221 = null;

        MySQLParserParser.table_spec_return table_spec223 = null;

        MySQLParserParser.alias_return alias224 = null;

        MySQLParserParser.column_spec_return column_spec226 = null;

        MySQLParserParser.column_spec_return column_spec228 = null;


        CommonTree string_literal222_tree=null;
        CommonTree string_literal225_tree=null;
        CommonTree EQ227_tree=null;
        RewriteRuleTokenStream stream_125=new RewriteRuleTokenStream(adaptor,"token 125");
        RewriteRuleTokenStream stream_EQ=new RewriteRuleTokenStream(adaptor,"token EQ");
        RewriteRuleTokenStream stream_124=new RewriteRuleTokenStream(adaptor,"token 124");
        RewriteRuleSubtreeStream stream_alias=new RewriteRuleSubtreeStream(adaptor,"rule alias");
        RewriteRuleSubtreeStream stream_column_spec=new RewriteRuleSubtreeStream(adaptor,"rule column_spec");
        RewriteRuleSubtreeStream stream_table_spec=new RewriteRuleSubtreeStream(adaptor,"rule table_spec");
        RewriteRuleSubtreeStream stream_join_type=new RewriteRuleSubtreeStream(adaptor,"rule join_type");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:442:4: ( ( join_type )? 'JOIN' table_spec ( alias )? 'ON' column_spec EQ column_spec -> ^( JOIN table_spec ( alias )? column_spec column_spec ( join_type )? ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:442:5: ( join_type )? 'JOIN' table_spec ( alias )? 'ON' column_spec EQ column_spec
            {
            // D:\\tools\\antlr\\test\\MySQLParser.g:442:5: ( join_type )?
            int alt56=2;
            int LA56_0 = input.LA(1);

            if ( ((LA56_0>=126 && LA56_0<=131)) ) {
                alt56=1;
            }
            switch (alt56) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:442:5: join_type
                    {
                    pushFollow(FOLLOW_join_type_in_join_claus2104);
                    join_type221=join_type();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_join_type.add(join_type221.getTree());

                    }
                    break;

            }

            string_literal222=(Token)match(input,124,FOLLOW_124_in_join_claus2107); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_124.add(string_literal222);

            pushFollow(FOLLOW_table_spec_in_join_claus2109);
            table_spec223=table_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_table_spec.add(table_spec223.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:442:35: ( alias )?
            int alt57=2;
            int LA57_0 = input.LA(1);

            if ( (LA57_0==ID||LA57_0==135) ) {
                alt57=1;
            }
            switch (alt57) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:442:35: alias
                    {
                    pushFollow(FOLLOW_alias_in_join_claus2112);
                    alias224=alias();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_alias.add(alias224.getTree());

                    }
                    break;

            }

            string_literal225=(Token)match(input,125,FOLLOW_125_in_join_claus2115); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_125.add(string_literal225);

            pushFollow(FOLLOW_column_spec_in_join_claus2117);
            column_spec226=column_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_column_spec.add(column_spec226.getTree());
            EQ227=(Token)match(input,EQ,FOLLOW_EQ_in_join_claus2119); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_EQ.add(EQ227);

            pushFollow(FOLLOW_column_spec_in_join_claus2121);
            column_spec228=column_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_column_spec.add(column_spec228.getTree());


            // AST REWRITE
            // elements: join_type, column_spec, column_spec, table_spec, alias
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 442:74: -> ^( JOIN table_spec ( alias )? column_spec column_spec ( join_type )? )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:442:76: ^( JOIN table_spec ( alias )? column_spec column_spec ( join_type )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(JOIN, "JOIN"), root_1);

                adaptor.addChild(root_1, stream_table_spec.nextTree());
                // D:\\tools\\antlr\\test\\MySQLParser.g:442:94: ( alias )?
                if ( stream_alias.hasNext() ) {
                    adaptor.addChild(root_1, stream_alias.nextTree());

                }
                stream_alias.reset();
                adaptor.addChild(root_1, stream_column_spec.nextTree());
                adaptor.addChild(root_1, stream_column_spec.nextTree());
                // D:\\tools\\antlr\\test\\MySQLParser.g:442:125: ( join_type )?
                if ( stream_join_type.hasNext() ) {
                    adaptor.addChild(root_1, stream_join_type.nextTree());

                }
                stream_join_type.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "join_claus"

    public static class join_type_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "join_type"
    // D:\\tools\\antlr\\test\\MySQLParser.g:445:2: join_type : ( 'INNER' -> INNER | 'LEFT' ( outer )? -> LEFT ( outer )? | 'RIGHT' ( outer )? -> RIGHT ( outer )? | 'FULL' ( outer )? -> FULL ( outer )? | 'UNION' -> UNION | 'CROSS' -> CROSS );
    public final MySQLParserParser.join_type_return join_type() throws RecognitionException {
        MySQLParserParser.join_type_return retval = new MySQLParserParser.join_type_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal229=null;
        Token string_literal230=null;
        Token string_literal232=null;
        Token string_literal234=null;
        Token string_literal236=null;
        Token string_literal237=null;
        MySQLParserParser.outer_return outer231 = null;

        MySQLParserParser.outer_return outer233 = null;

        MySQLParserParser.outer_return outer235 = null;


        CommonTree string_literal229_tree=null;
        CommonTree string_literal230_tree=null;
        CommonTree string_literal232_tree=null;
        CommonTree string_literal234_tree=null;
        CommonTree string_literal236_tree=null;
        CommonTree string_literal237_tree=null;
        RewriteRuleTokenStream stream_126=new RewriteRuleTokenStream(adaptor,"token 126");
        RewriteRuleTokenStream stream_127=new RewriteRuleTokenStream(adaptor,"token 127");
        RewriteRuleTokenStream stream_128=new RewriteRuleTokenStream(adaptor,"token 128");
        RewriteRuleTokenStream stream_131=new RewriteRuleTokenStream(adaptor,"token 131");
        RewriteRuleTokenStream stream_129=new RewriteRuleTokenStream(adaptor,"token 129");
        RewriteRuleTokenStream stream_130=new RewriteRuleTokenStream(adaptor,"token 130");
        RewriteRuleSubtreeStream stream_outer=new RewriteRuleSubtreeStream(adaptor,"rule outer");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:446:4: ( 'INNER' -> INNER | 'LEFT' ( outer )? -> LEFT ( outer )? | 'RIGHT' ( outer )? -> RIGHT ( outer )? | 'FULL' ( outer )? -> FULL ( outer )? | 'UNION' -> UNION | 'CROSS' -> CROSS )
            int alt61=6;
            switch ( input.LA(1) ) {
            case 126:
                {
                alt61=1;
                }
                break;
            case 127:
                {
                alt61=2;
                }
                break;
            case 128:
                {
                alt61=3;
                }
                break;
            case 129:
                {
                alt61=4;
                }
                break;
            case 130:
                {
                alt61=5;
                }
                break;
            case 131:
                {
                alt61=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 61, 0, input);

                throw nvae;
            }

            switch (alt61) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:446:5: 'INNER'
                    {
                    string_literal229=(Token)match(input,126,FOLLOW_126_in_join_type2154); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_126.add(string_literal229);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 446:12: -> INNER
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(INNER, "INNER"));

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:447:5: 'LEFT' ( outer )?
                    {
                    string_literal230=(Token)match(input,127,FOLLOW_127_in_join_type2162); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_127.add(string_literal230);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:447:12: ( outer )?
                    int alt58=2;
                    int LA58_0 = input.LA(1);

                    if ( (LA58_0==132) ) {
                        alt58=1;
                    }
                    switch (alt58) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:447:12: outer
                            {
                            pushFollow(FOLLOW_outer_in_join_type2164);
                            outer231=outer();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_outer.add(outer231.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: outer
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 447:19: -> LEFT ( outer )?
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(LEFT, "LEFT"));
                        // D:\\tools\\antlr\\test\\MySQLParser.g:447:26: ( outer )?
                        if ( stream_outer.hasNext() ) {
                            adaptor.addChild(root_0, stream_outer.nextTree());

                        }
                        stream_outer.reset();

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:448:5: 'RIGHT' ( outer )?
                    {
                    string_literal232=(Token)match(input,128,FOLLOW_128_in_join_type2177); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_128.add(string_literal232);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:448:13: ( outer )?
                    int alt59=2;
                    int LA59_0 = input.LA(1);

                    if ( (LA59_0==132) ) {
                        alt59=1;
                    }
                    switch (alt59) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:448:13: outer
                            {
                            pushFollow(FOLLOW_outer_in_join_type2179);
                            outer233=outer();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_outer.add(outer233.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: outer
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 448:20: -> RIGHT ( outer )?
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(RIGHT, "RIGHT"));
                        // D:\\tools\\antlr\\test\\MySQLParser.g:448:29: ( outer )?
                        if ( stream_outer.hasNext() ) {
                            adaptor.addChild(root_0, stream_outer.nextTree());

                        }
                        stream_outer.reset();

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:449:5: 'FULL' ( outer )?
                    {
                    string_literal234=(Token)match(input,129,FOLLOW_129_in_join_type2193); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_129.add(string_literal234);

                    // D:\\tools\\antlr\\test\\MySQLParser.g:449:12: ( outer )?
                    int alt60=2;
                    int LA60_0 = input.LA(1);

                    if ( (LA60_0==132) ) {
                        alt60=1;
                    }
                    switch (alt60) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:449:12: outer
                            {
                            pushFollow(FOLLOW_outer_in_join_type2195);
                            outer235=outer();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_outer.add(outer235.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: outer
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 449:19: -> FULL ( outer )?
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(FULL, "FULL"));
                        // D:\\tools\\antlr\\test\\MySQLParser.g:449:27: ( outer )?
                        if ( stream_outer.hasNext() ) {
                            adaptor.addChild(root_0, stream_outer.nextTree());

                        }
                        stream_outer.reset();

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:450:5: 'UNION'
                    {
                    string_literal236=(Token)match(input,130,FOLLOW_130_in_join_type2209); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_130.add(string_literal236);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 450:13: -> UNION
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(UNION, "UNION"));

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:451:5: 'CROSS'
                    {
                    string_literal237=(Token)match(input,131,FOLLOW_131_in_join_type2219); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_131.add(string_literal237);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 451:13: -> CROSS
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(CROSS, "CROSS"));

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "join_type"

    public static class outer_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "outer"
    // D:\\tools\\antlr\\test\\MySQLParser.g:454:1: outer : 'OUTER' -> OUTER ;
    public final MySQLParserParser.outer_return outer() throws RecognitionException {
        MySQLParserParser.outer_return retval = new MySQLParserParser.outer_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal238=null;

        CommonTree string_literal238_tree=null;
        RewriteRuleTokenStream stream_132=new RewriteRuleTokenStream(adaptor,"token 132");

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:455:4: ( 'OUTER' -> OUTER )
            // D:\\tools\\antlr\\test\\MySQLParser.g:455:5: 'OUTER'
            {
            string_literal238=(Token)match(input,132,FOLLOW_132_in_outer2237); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_132.add(string_literal238);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 455:13: -> OUTER
            {
                adaptor.addChild(root_0, (CommonTree)adaptor.create(OUTER, "OUTER"));

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "outer"

    public static class table_name_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "table_name"
    // D:\\tools\\antlr\\test\\MySQLParser.g:458:1: table_name : identifier ;
    public final MySQLParserParser.table_name_return table_name() throws RecognitionException {
        MySQLParserParser.table_name_return retval = new MySQLParserParser.table_name_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.identifier_return identifier239 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:459:2: ( identifier )
            // D:\\tools\\antlr\\test\\MySQLParser.g:459:3: identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_identifier_in_table_name2256);
            identifier239=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier239.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "table_name"

    public static class column_spec_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "column_spec"
    // D:\\tools\\antlr\\test\\MySQLParser.g:462:1: column_spec : ( ( table_name DOT )? identifier -> ^( COLUMN identifier ( table_name )? ) | ASTERISK -> ^( COLUMNAST ASTERISK ) );
    public final MySQLParserParser.column_spec_return column_spec() throws RecognitionException {
        MySQLParserParser.column_spec_return retval = new MySQLParserParser.column_spec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DOT241=null;
        Token ASTERISK243=null;
        MySQLParserParser.table_name_return table_name240 = null;

        MySQLParserParser.identifier_return identifier242 = null;


        CommonTree DOT241_tree=null;
        CommonTree ASTERISK243_tree=null;
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleTokenStream stream_ASTERISK=new RewriteRuleTokenStream(adaptor,"token ASTERISK");
        RewriteRuleSubtreeStream stream_table_name=new RewriteRuleSubtreeStream(adaptor,"rule table_name");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:463:2: ( ( table_name DOT )? identifier -> ^( COLUMN identifier ( table_name )? ) | ASTERISK -> ^( COLUMNAST ASTERISK ) )
            int alt63=2;
            int LA63_0 = input.LA(1);

            if ( (LA63_0==ID) ) {
                alt63=1;
            }
            else if ( (LA63_0==ASTERISK) ) {
                alt63=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 63, 0, input);

                throw nvae;
            }
            switch (alt63) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:463:3: ( table_name DOT )? identifier
                    {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:463:3: ( table_name DOT )?
                    int alt62=2;
                    int LA62_0 = input.LA(1);

                    if ( (LA62_0==ID) ) {
                        int LA62_1 = input.LA(2);

                        if ( (LA62_1==DOT) ) {
                            alt62=1;
                        }
                    }
                    switch (alt62) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLParser.g:463:4: table_name DOT
                            {
                            pushFollow(FOLLOW_table_name_in_column_spec2268);
                            table_name240=table_name();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_table_name.add(table_name240.getTree());
                            DOT241=(Token)match(input,DOT,FOLLOW_DOT_in_column_spec2270); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_DOT.add(DOT241);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_identifier_in_column_spec2274);
                    identifier242=identifier();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_identifier.add(identifier242.getTree());


                    // AST REWRITE
                    // elements: table_name, identifier
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 463:32: -> ^( COLUMN identifier ( table_name )? )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:463:34: ^( COLUMN identifier ( table_name )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(COLUMN, "COLUMN"), root_1);

                        adaptor.addChild(root_1, stream_identifier.nextTree());
                        // D:\\tools\\antlr\\test\\MySQLParser.g:463:54: ( table_name )?
                        if ( stream_table_name.hasNext() ) {
                            adaptor.addChild(root_1, stream_table_name.nextTree());

                        }
                        stream_table_name.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:464:3: ASTERISK
                    {
                    ASTERISK243=(Token)match(input,ASTERISK,FOLLOW_ASTERISK_in_column_spec2288); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ASTERISK.add(ASTERISK243);



                    // AST REWRITE
                    // elements: ASTERISK
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 464:11: -> ^( COLUMNAST ASTERISK )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:464:13: ^( COLUMNAST ASTERISK )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(COLUMNAST, "COLUMNAST"), root_1);

                        adaptor.addChild(root_1, stream_ASTERISK.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "column_spec"

    public static class schema_name_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "schema_name"
    // D:\\tools\\antlr\\test\\MySQLParser.g:467:1: schema_name : identifier ;
    public final MySQLParserParser.schema_name_return schema_name() throws RecognitionException {
        MySQLParserParser.schema_name_return retval = new MySQLParserParser.schema_name_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.identifier_return identifier244 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:468:2: ( identifier )
            // D:\\tools\\antlr\\test\\MySQLParser.g:468:3: identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_identifier_in_schema_name2304);
            identifier244=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier244.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "schema_name"

    public static class boolean_literal_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "boolean_literal"
    // D:\\tools\\antlr\\test\\MySQLParser.g:471:1: boolean_literal : ( 'TRUE' | 'FALSE' );
    public final MySQLParserParser.boolean_literal_return boolean_literal() throws RecognitionException {
        MySQLParserParser.boolean_literal_return retval = new MySQLParserParser.boolean_literal_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set245=null;

        CommonTree set245_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:472:2: ( 'TRUE' | 'FALSE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set245=(Token)input.LT(1);
            if ( (input.LA(1)>=133 && input.LA(1)<=134) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set245));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "boolean_literal"

    public static class alias_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "alias"
    // D:\\tools\\antlr\\test\\MySQLParser.g:475:1: alias : ( 'AS' )? identifier -> ^( AS identifier ) ;
    public final MySQLParserParser.alias_return alias() throws RecognitionException {
        MySQLParserParser.alias_return retval = new MySQLParserParser.alias_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal246=null;
        MySQLParserParser.identifier_return identifier247 = null;


        CommonTree string_literal246_tree=null;
        RewriteRuleTokenStream stream_135=new RewriteRuleTokenStream(adaptor,"token 135");
        RewriteRuleSubtreeStream stream_identifier=new RewriteRuleSubtreeStream(adaptor,"rule identifier");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:476:2: ( ( 'AS' )? identifier -> ^( AS identifier ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:476:4: ( 'AS' )? identifier
            {
            // D:\\tools\\antlr\\test\\MySQLParser.g:476:4: ( 'AS' )?
            int alt64=2;
            int LA64_0 = input.LA(1);

            if ( (LA64_0==135) ) {
                alt64=1;
            }
            switch (alt64) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:476:6: 'AS'
                    {
                    string_literal246=(Token)match(input,135,FOLLOW_135_in_alias2334); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_135.add(string_literal246);


                    }
                    break;

            }

            pushFollow(FOLLOW_identifier_in_alias2338);
            identifier247=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_identifier.add(identifier247.getTree());


            // AST REWRITE
            // elements: identifier
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 476:23: -> ^( AS identifier )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:476:25: ^( AS identifier )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(AS, "AS"), root_1);

                adaptor.addChild(root_1, stream_identifier.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "alias"

    public static class identifier_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "identifier"
    // D:\\tools\\antlr\\test\\MySQLParser.g:479:1: identifier : ID ;
    public final MySQLParserParser.identifier_return identifier() throws RecognitionException {
        MySQLParserParser.identifier_return retval = new MySQLParserParser.identifier_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID248=null;

        CommonTree ID248_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:480:2: ( ID )
            // D:\\tools\\antlr\\test\\MySQLParser.g:480:4: ID
            {
            root_0 = (CommonTree)adaptor.nil();

            ID248=(Token)match(input,ID,FOLLOW_ID_in_identifier2356); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            ID248_tree = (CommonTree)adaptor.create(ID248);
            adaptor.addChild(root_0, ID248_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "identifier"

    public static class quoted_string_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "quoted_string"
    // D:\\tools\\antlr\\test\\MySQLParser.g:560:1: quoted_string : QUOTED_STRING ;
    public final MySQLParserParser.quoted_string_return quoted_string() throws RecognitionException {
        MySQLParserParser.quoted_string_return retval = new MySQLParserParser.quoted_string_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token QUOTED_STRING249=null;

        CommonTree QUOTED_STRING249_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:561:2: ( QUOTED_STRING )
            // D:\\tools\\antlr\\test\\MySQLParser.g:561:4: QUOTED_STRING
            {
            root_0 = (CommonTree)adaptor.nil();

            QUOTED_STRING249=(Token)match(input,QUOTED_STRING,FOLLOW_QUOTED_STRING_in_quoted_string2782); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            QUOTED_STRING249_tree = (CommonTree)adaptor.create(QUOTED_STRING249);
            adaptor.addChild(root_0, QUOTED_STRING249_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "quoted_string"

    public static class select_command_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "select_command"
    // D:\\tools\\antlr\\test\\MySQLParser.g:602:1: select_command : selectClause ( fromClause )? ( whereClause )? ( groupByClause )? ( havingClause )? ( orderByClause )? ( limitClause )? ( indexClause )? ( for_update )? ;
    public final MySQLParserParser.select_command_return select_command() throws RecognitionException {
        MySQLParserParser.select_command_return retval = new MySQLParserParser.select_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLParserParser.selectClause_return selectClause250 = null;

        MySQLParserParser.fromClause_return fromClause251 = null;

        MySQLParserParser.whereClause_return whereClause252 = null;

        MySQLParserParser.groupByClause_return groupByClause253 = null;

        MySQLParserParser.havingClause_return havingClause254 = null;

        MySQLParserParser.orderByClause_return orderByClause255 = null;

        MySQLParserParser.limitClause_return limitClause256 = null;

        MySQLParserParser.indexClause_return indexClause257 = null;

        MySQLParserParser.for_update_return for_update258 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:603:6: ( selectClause ( fromClause )? ( whereClause )? ( groupByClause )? ( havingClause )? ( orderByClause )? ( limitClause )? ( indexClause )? ( for_update )? )
            // D:\\tools\\antlr\\test\\MySQLParser.g:603:8: selectClause ( fromClause )? ( whereClause )? ( groupByClause )? ( havingClause )? ( orderByClause )? ( limitClause )? ( indexClause )? ( for_update )?
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_selectClause_in_select_command2967);
            selectClause250=selectClause();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, selectClause250.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:603:21: ( fromClause )?
            int alt65=2;
            int LA65_0 = input.LA(1);

            if ( (LA65_0==123) ) {
                alt65=1;
            }
            switch (alt65) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:603:22: fromClause
                    {
                    pushFollow(FOLLOW_fromClause_in_select_command2970);
                    fromClause251=fromClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, fromClause251.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:603:35: ( whereClause )?
            int alt66=2;
            int LA66_0 = input.LA(1);

            if ( (LA66_0==110) ) {
                alt66=1;
            }
            switch (alt66) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:603:36: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_select_command2975);
                    whereClause252=whereClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, whereClause252.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:603:50: ( groupByClause )?
            int alt67=2;
            int LA67_0 = input.LA(1);

            if ( (LA67_0==104) ) {
                alt67=1;
            }
            switch (alt67) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:603:50: groupByClause
                    {
                    pushFollow(FOLLOW_groupByClause_in_select_command2979);
                    groupByClause253=groupByClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, groupByClause253.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:603:65: ( havingClause )?
            int alt68=2;
            int LA68_0 = input.LA(1);

            if ( (LA68_0==106) ) {
                alt68=1;
            }
            switch (alt68) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:603:65: havingClause
                    {
                    pushFollow(FOLLOW_havingClause_in_select_command2982);
                    havingClause254=havingClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, havingClause254.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:603:78: ( orderByClause )?
            int alt69=2;
            int LA69_0 = input.LA(1);

            if ( (LA69_0==107) ) {
                alt69=1;
            }
            switch (alt69) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:603:79: orderByClause
                    {
                    pushFollow(FOLLOW_orderByClause_in_select_command2985);
                    orderByClause255=orderByClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, orderByClause255.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:603:95: ( limitClause )?
            int alt70=2;
            int LA70_0 = input.LA(1);

            if ( (LA70_0==141) ) {
                alt70=1;
            }
            switch (alt70) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:603:96: limitClause
                    {
                    pushFollow(FOLLOW_limitClause_in_select_command2990);
                    limitClause256=limitClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, limitClause256.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:603:110: ( indexClause )?
            int alt71=2;
            int LA71_0 = input.LA(1);

            if ( (LA71_0==136||LA71_0==138) ) {
                alt71=1;
            }
            switch (alt71) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:603:110: indexClause
                    {
                    pushFollow(FOLLOW_indexClause_in_select_command2994);
                    indexClause257=indexClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, indexClause257.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:603:123: ( for_update )?
            int alt72=2;
            int LA72_0 = input.LA(1);

            if ( ((LA72_0>=142 && LA72_0<=143)) ) {
                alt72=1;
            }
            switch (alt72) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:603:123: for_update
                    {
                    pushFollow(FOLLOW_for_update_in_select_command2997);
                    for_update258=for_update();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, for_update258.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "select_command"

    public static class indexClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "indexClause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:605:2: indexClause : ( 'FORCE' 'INDEX' LPAREN select_list RPAREN | 'IGNORE' 'INDEX' LPAREN select_list RPAREN );
    public final MySQLParserParser.indexClause_return indexClause() throws RecognitionException {
        MySQLParserParser.indexClause_return retval = new MySQLParserParser.indexClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal259=null;
        Token string_literal260=null;
        Token LPAREN261=null;
        Token RPAREN263=null;
        Token string_literal264=null;
        Token string_literal265=null;
        Token LPAREN266=null;
        Token RPAREN268=null;
        MySQLParserParser.select_list_return select_list262 = null;

        MySQLParserParser.select_list_return select_list267 = null;


        CommonTree string_literal259_tree=null;
        CommonTree string_literal260_tree=null;
        CommonTree LPAREN261_tree=null;
        CommonTree RPAREN263_tree=null;
        CommonTree string_literal264_tree=null;
        CommonTree string_literal265_tree=null;
        CommonTree LPAREN266_tree=null;
        CommonTree RPAREN268_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:606:3: ( 'FORCE' 'INDEX' LPAREN select_list RPAREN | 'IGNORE' 'INDEX' LPAREN select_list RPAREN )
            int alt73=2;
            int LA73_0 = input.LA(1);

            if ( (LA73_0==136) ) {
                alt73=1;
            }
            else if ( (LA73_0==138) ) {
                alt73=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 73, 0, input);

                throw nvae;
            }
            switch (alt73) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:606:4: 'FORCE' 'INDEX' LPAREN select_list RPAREN
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    string_literal259=(Token)match(input,136,FOLLOW_136_in_indexClause3013); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal259_tree = (CommonTree)adaptor.create(string_literal259);
                    adaptor.addChild(root_0, string_literal259_tree);
                    }
                    string_literal260=(Token)match(input,137,FOLLOW_137_in_indexClause3015); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal260_tree = (CommonTree)adaptor.create(string_literal260);
                    adaptor.addChild(root_0, string_literal260_tree);
                    }
                    LPAREN261=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_indexClause3017); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    LPAREN261_tree = (CommonTree)adaptor.create(LPAREN261);
                    adaptor.addChild(root_0, LPAREN261_tree);
                    }
                    pushFollow(FOLLOW_select_list_in_indexClause3019);
                    select_list262=select_list();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, select_list262.getTree());
                    RPAREN263=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_indexClause3022); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    RPAREN263_tree = (CommonTree)adaptor.create(RPAREN263);
                    adaptor.addChild(root_0, RPAREN263_tree);
                    }

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:607:4: 'IGNORE' 'INDEX' LPAREN select_list RPAREN
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    string_literal264=(Token)match(input,138,FOLLOW_138_in_indexClause3027); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal264_tree = (CommonTree)adaptor.create(string_literal264);
                    adaptor.addChild(root_0, string_literal264_tree);
                    }
                    string_literal265=(Token)match(input,137,FOLLOW_137_in_indexClause3029); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal265_tree = (CommonTree)adaptor.create(string_literal265);
                    adaptor.addChild(root_0, string_literal265_tree);
                    }
                    LPAREN266=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_indexClause3031); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    LPAREN266_tree = (CommonTree)adaptor.create(LPAREN266);
                    adaptor.addChild(root_0, LPAREN266_tree);
                    }
                    pushFollow(FOLLOW_select_list_in_indexClause3033);
                    select_list267=select_list();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, select_list267.getTree());
                    RPAREN268=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_indexClause3035); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    RPAREN268_tree = (CommonTree)adaptor.create(RPAREN268);
                    adaptor.addChild(root_0, RPAREN268_tree);
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "indexClause"

    public static class delete_command_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "delete_command"
    // D:\\tools\\antlr\\test\\MySQLParser.g:609:1: delete_command : 'DELETE' fromClause ( whereClause )? ( orderByClause )? ( limitClause )? -> ^( DELETE fromClause ( whereClause )? ( orderByClause )? ) ( limitClause )? ;
    public final MySQLParserParser.delete_command_return delete_command() throws RecognitionException {
        MySQLParserParser.delete_command_return retval = new MySQLParserParser.delete_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal269=null;
        MySQLParserParser.fromClause_return fromClause270 = null;

        MySQLParserParser.whereClause_return whereClause271 = null;

        MySQLParserParser.orderByClause_return orderByClause272 = null;

        MySQLParserParser.limitClause_return limitClause273 = null;


        CommonTree string_literal269_tree=null;
        RewriteRuleTokenStream stream_139=new RewriteRuleTokenStream(adaptor,"token 139");
        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        RewriteRuleSubtreeStream stream_limitClause=new RewriteRuleSubtreeStream(adaptor,"rule limitClause");
        RewriteRuleSubtreeStream stream_orderByClause=new RewriteRuleSubtreeStream(adaptor,"rule orderByClause");
        RewriteRuleSubtreeStream stream_fromClause=new RewriteRuleSubtreeStream(adaptor,"rule fromClause");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:610:2: ( 'DELETE' fromClause ( whereClause )? ( orderByClause )? ( limitClause )? -> ^( DELETE fromClause ( whereClause )? ( orderByClause )? ) ( limitClause )? )
            // D:\\tools\\antlr\\test\\MySQLParser.g:610:3: 'DELETE' fromClause ( whereClause )? ( orderByClause )? ( limitClause )?
            {
            string_literal269=(Token)match(input,139,FOLLOW_139_in_delete_command3045); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_139.add(string_literal269);

            pushFollow(FOLLOW_fromClause_in_delete_command3047);
            fromClause270=fromClause();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_fromClause.add(fromClause270.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:610:23: ( whereClause )?
            int alt74=2;
            int LA74_0 = input.LA(1);

            if ( (LA74_0==110) ) {
                alt74=1;
            }
            switch (alt74) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:610:23: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_delete_command3049);
                    whereClause271=whereClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_whereClause.add(whereClause271.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:610:36: ( orderByClause )?
            int alt75=2;
            int LA75_0 = input.LA(1);

            if ( (LA75_0==107) ) {
                alt75=1;
            }
            switch (alt75) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:610:36: orderByClause
                    {
                    pushFollow(FOLLOW_orderByClause_in_delete_command3052);
                    orderByClause272=orderByClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orderByClause.add(orderByClause272.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:610:52: ( limitClause )?
            int alt76=2;
            int LA76_0 = input.LA(1);

            if ( (LA76_0==141) ) {
                alt76=1;
            }
            switch (alt76) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:610:53: limitClause
                    {
                    pushFollow(FOLLOW_limitClause_in_delete_command3057);
                    limitClause273=limitClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_limitClause.add(limitClause273.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: whereClause, fromClause, orderByClause, limitClause
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 610:66: -> ^( DELETE fromClause ( whereClause )? ( orderByClause )? ) ( limitClause )?
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:610:68: ^( DELETE fromClause ( whereClause )? ( orderByClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(DELETE, "DELETE"), root_1);

                adaptor.addChild(root_1, stream_fromClause.nextTree());
                // D:\\tools\\antlr\\test\\MySQLParser.g:610:88: ( whereClause )?
                if ( stream_whereClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_whereClause.nextTree());

                }
                stream_whereClause.reset();
                // D:\\tools\\antlr\\test\\MySQLParser.g:610:101: ( orderByClause )?
                if ( stream_orderByClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_orderByClause.nextTree());

                }
                stream_orderByClause.reset();

                adaptor.addChild(root_0, root_1);
                }
                // D:\\tools\\antlr\\test\\MySQLParser.g:610:117: ( limitClause )?
                if ( stream_limitClause.hasNext() ) {
                    adaptor.addChild(root_0, stream_limitClause.nextTree());

                }
                stream_limitClause.reset();

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "delete_command"

    public static class update_command_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "update_command"
    // D:\\tools\\antlr\\test\\MySQLParser.g:612:1: update_command : 'UPDATE' selected_table setclause ( whereClause )? ( orderByClause )? ( limitClause )? -> ^( UPDATE selected_table setclause ( whereClause )? ( orderByClause )? ) ( limitClause )? ;
    public final MySQLParserParser.update_command_return update_command() throws RecognitionException {
        MySQLParserParser.update_command_return retval = new MySQLParserParser.update_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal274=null;
        MySQLParserParser.selected_table_return selected_table275 = null;

        MySQLParserParser.setclause_return setclause276 = null;

        MySQLParserParser.whereClause_return whereClause277 = null;

        MySQLParserParser.orderByClause_return orderByClause278 = null;

        MySQLParserParser.limitClause_return limitClause279 = null;


        CommonTree string_literal274_tree=null;
        RewriteRuleTokenStream stream_140=new RewriteRuleTokenStream(adaptor,"token 140");
        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        RewriteRuleSubtreeStream stream_selected_table=new RewriteRuleSubtreeStream(adaptor,"rule selected_table");
        RewriteRuleSubtreeStream stream_limitClause=new RewriteRuleSubtreeStream(adaptor,"rule limitClause");
        RewriteRuleSubtreeStream stream_setclause=new RewriteRuleSubtreeStream(adaptor,"rule setclause");
        RewriteRuleSubtreeStream stream_orderByClause=new RewriteRuleSubtreeStream(adaptor,"rule orderByClause");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:613:2: ( 'UPDATE' selected_table setclause ( whereClause )? ( orderByClause )? ( limitClause )? -> ^( UPDATE selected_table setclause ( whereClause )? ( orderByClause )? ) ( limitClause )? )
            // D:\\tools\\antlr\\test\\MySQLParser.g:613:3: 'UPDATE' selected_table setclause ( whereClause )? ( orderByClause )? ( limitClause )?
            {
            string_literal274=(Token)match(input,140,FOLLOW_140_in_update_command3085); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_140.add(string_literal274);

            pushFollow(FOLLOW_selected_table_in_update_command3087);
            selected_table275=selected_table();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_selected_table.add(selected_table275.getTree());
            pushFollow(FOLLOW_setclause_in_update_command3090);
            setclause276=setclause();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_setclause.add(setclause276.getTree());
            // D:\\tools\\antlr\\test\\MySQLParser.g:613:38: ( whereClause )?
            int alt77=2;
            int LA77_0 = input.LA(1);

            if ( (LA77_0==110) ) {
                alt77=1;
            }
            switch (alt77) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:613:38: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_update_command3092);
                    whereClause277=whereClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_whereClause.add(whereClause277.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:613:51: ( orderByClause )?
            int alt78=2;
            int LA78_0 = input.LA(1);

            if ( (LA78_0==107) ) {
                alt78=1;
            }
            switch (alt78) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:613:51: orderByClause
                    {
                    pushFollow(FOLLOW_orderByClause_in_update_command3095);
                    orderByClause278=orderByClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orderByClause.add(orderByClause278.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLParser.g:613:67: ( limitClause )?
            int alt79=2;
            int LA79_0 = input.LA(1);

            if ( (LA79_0==141) ) {
                alt79=1;
            }
            switch (alt79) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:613:68: limitClause
                    {
                    pushFollow(FOLLOW_limitClause_in_update_command3100);
                    limitClause279=limitClause();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_limitClause.add(limitClause279.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: selected_table, orderByClause, whereClause, setclause, limitClause
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 613:81: -> ^( UPDATE selected_table setclause ( whereClause )? ( orderByClause )? ) ( limitClause )?
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:613:83: ^( UPDATE selected_table setclause ( whereClause )? ( orderByClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(UPDATE, "UPDATE"), root_1);

                adaptor.addChild(root_1, stream_selected_table.nextTree());
                adaptor.addChild(root_1, stream_setclause.nextTree());
                // D:\\tools\\antlr\\test\\MySQLParser.g:613:117: ( whereClause )?
                if ( stream_whereClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_whereClause.nextTree());

                }
                stream_whereClause.reset();
                // D:\\tools\\antlr\\test\\MySQLParser.g:613:130: ( orderByClause )?
                if ( stream_orderByClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_orderByClause.nextTree());

                }
                stream_orderByClause.reset();

                adaptor.addChild(root_0, root_1);
                }
                // D:\\tools\\antlr\\test\\MySQLParser.g:613:145: ( limitClause )?
                if ( stream_limitClause.hasNext() ) {
                    adaptor.addChild(root_0, stream_limitClause.nextTree());

                }
                stream_limitClause.reset();

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "update_command"

    public static class limitClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "limitClause"
    // D:\\tools\\antlr\\test\\MySQLParser.g:615:1: limitClause : 'LIMIT' ( skip COMMA )? range -> ^( 'LIMIT' ( skip )? range ) ;
    public final MySQLParserParser.limitClause_return limitClause() throws RecognitionException {
        MySQLParserParser.limitClause_return retval = new MySQLParserParser.limitClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal280=null;
        Token COMMA282=null;
        MySQLParserParser.skip_return skip281 = null;

        MySQLParserParser.range_return range283 = null;


        CommonTree string_literal280_tree=null;
        CommonTree COMMA282_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_141=new RewriteRuleTokenStream(adaptor,"token 141");
        RewriteRuleSubtreeStream stream_range=new RewriteRuleSubtreeStream(adaptor,"rule range");
        RewriteRuleSubtreeStream stream_skip=new RewriteRuleSubtreeStream(adaptor,"rule skip");
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:616:2: ( 'LIMIT' ( skip COMMA )? range -> ^( 'LIMIT' ( skip )? range ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:616:3: 'LIMIT' ( skip COMMA )? range
            {
            string_literal280=(Token)match(input,141,FOLLOW_141_in_limitClause3129); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_141.add(string_literal280);

            // D:\\tools\\antlr\\test\\MySQLParser.g:616:11: ( skip COMMA )?
            int alt80=2;
            switch ( input.LA(1) ) {
                case PLUS:
                    {
                    int LA80_1 = input.LA(2);

                    if ( (LA80_1==N) ) {
                        int LA80_5 = input.LA(3);

                        if ( (LA80_5==COMMA) ) {
                            alt80=1;
                        }
                    }
                    }
                    break;
                case MINUS:
                    {
                    int LA80_2 = input.LA(2);

                    if ( (LA80_2==N) ) {
                        int LA80_6 = input.LA(3);

                        if ( (LA80_6==COMMA) ) {
                            alt80=1;
                        }
                    }
                    }
                    break;
                case N:
                    {
                    int LA80_3 = input.LA(2);

                    if ( (LA80_3==COMMA) ) {
                        alt80=1;
                    }
                    }
                    break;
                case 122:
                    {
                    int LA80_4 = input.LA(2);

                    if ( (LA80_4==COMMA) ) {
                        alt80=1;
                    }
                    }
                    break;
            }

            switch (alt80) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:616:12: skip COMMA
                    {
                    pushFollow(FOLLOW_skip_in_limitClause3132);
                    skip281=skip();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_skip.add(skip281.getTree());
                    COMMA282=(Token)match(input,COMMA,FOLLOW_COMMA_in_limitClause3134); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COMMA.add(COMMA282);


                    }
                    break;

            }

            pushFollow(FOLLOW_range_in_limitClause3139);
            range283=range();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_range.add(range283.getTree());


            // AST REWRITE
            // elements: 141, skip, range
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 616:31: -> ^( 'LIMIT' ( skip )? range )
            {
                // D:\\tools\\antlr\\test\\MySQLParser.g:616:33: ^( 'LIMIT' ( skip )? range )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(stream_141.nextNode(), root_1);

                // D:\\tools\\antlr\\test\\MySQLParser.g:616:43: ( skip )?
                if ( stream_skip.hasNext() ) {
                    adaptor.addChild(root_1, stream_skip.nextTree());

                }
                stream_skip.reset();
                adaptor.addChild(root_1, stream_range.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "limitClause"

    public static class for_update_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "for_update"
    // D:\\tools\\antlr\\test\\MySQLParser.g:618:1: for_update : ( 'FOR' 'UPDATE' -> ^( FORUPDATE ) | 'LOCK' 'IN' 'SHARE' 'MODE' -> ^( SHAREMODE ) );
    public final MySQLParserParser.for_update_return for_update() throws RecognitionException {
        MySQLParserParser.for_update_return retval = new MySQLParserParser.for_update_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal284=null;
        Token string_literal285=null;
        Token string_literal286=null;
        Token string_literal287=null;
        Token string_literal288=null;
        Token string_literal289=null;

        CommonTree string_literal284_tree=null;
        CommonTree string_literal285_tree=null;
        CommonTree string_literal286_tree=null;
        CommonTree string_literal287_tree=null;
        CommonTree string_literal288_tree=null;
        CommonTree string_literal289_tree=null;
        RewriteRuleTokenStream stream_143=new RewriteRuleTokenStream(adaptor,"token 143");
        RewriteRuleTokenStream stream_144=new RewriteRuleTokenStream(adaptor,"token 144");
        RewriteRuleTokenStream stream_145=new RewriteRuleTokenStream(adaptor,"token 145");
        RewriteRuleTokenStream stream_140=new RewriteRuleTokenStream(adaptor,"token 140");
        RewriteRuleTokenStream stream_142=new RewriteRuleTokenStream(adaptor,"token 142");
        RewriteRuleTokenStream stream_119=new RewriteRuleTokenStream(adaptor,"token 119");

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:619:2: ( 'FOR' 'UPDATE' -> ^( FORUPDATE ) | 'LOCK' 'IN' 'SHARE' 'MODE' -> ^( SHAREMODE ) )
            int alt81=2;
            int LA81_0 = input.LA(1);

            if ( (LA81_0==142) ) {
                alt81=1;
            }
            else if ( (LA81_0==143) ) {
                alt81=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 81, 0, input);

                throw nvae;
            }
            switch (alt81) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:619:3: 'FOR' 'UPDATE'
                    {
                    string_literal284=(Token)match(input,142,FOLLOW_142_in_for_update3157); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_142.add(string_literal284);

                    string_literal285=(Token)match(input,140,FOLLOW_140_in_for_update3159); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_140.add(string_literal285);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 619:18: -> ^( FORUPDATE )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:619:20: ^( FORUPDATE )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(FORUPDATE, "FORUPDATE"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:620:3: 'LOCK' 'IN' 'SHARE' 'MODE'
                    {
                    string_literal286=(Token)match(input,143,FOLLOW_143_in_for_update3168); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_143.add(string_literal286);

                    string_literal287=(Token)match(input,119,FOLLOW_119_in_for_update3170); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_119.add(string_literal287);

                    string_literal288=(Token)match(input,144,FOLLOW_144_in_for_update3172); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_144.add(string_literal288);

                    string_literal289=(Token)match(input,145,FOLLOW_145_in_for_update3174); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_145.add(string_literal289);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 620:29: -> ^( SHAREMODE )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:620:31: ^( SHAREMODE )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SHAREMODE, "SHAREMODE"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "for_update"

    public static class skip_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "skip"
    // D:\\tools\\antlr\\test\\MySQLParser.g:622:1: skip : ( PLUS N -> ^( SKIP N ) | MINUS N -> ^( SKIP MINUS N ) | N -> ^( SKIP N ) | '?' -> ^( SKIP '?' ) );
    public final MySQLParserParser.skip_return skip() throws RecognitionException {
        MySQLParserParser.skip_return retval = new MySQLParserParser.skip_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token PLUS290=null;
        Token N291=null;
        Token MINUS292=null;
        Token N293=null;
        Token N294=null;
        Token char_literal295=null;

        CommonTree PLUS290_tree=null;
        CommonTree N291_tree=null;
        CommonTree MINUS292_tree=null;
        CommonTree N293_tree=null;
        CommonTree N294_tree=null;
        CommonTree char_literal295_tree=null;
        RewriteRuleTokenStream stream_PLUS=new RewriteRuleTokenStream(adaptor,"token PLUS");
        RewriteRuleTokenStream stream_122=new RewriteRuleTokenStream(adaptor,"token 122");
        RewriteRuleTokenStream stream_MINUS=new RewriteRuleTokenStream(adaptor,"token MINUS");
        RewriteRuleTokenStream stream_N=new RewriteRuleTokenStream(adaptor,"token N");

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:623:2: ( PLUS N -> ^( SKIP N ) | MINUS N -> ^( SKIP MINUS N ) | N -> ^( SKIP N ) | '?' -> ^( SKIP '?' ) )
            int alt82=4;
            switch ( input.LA(1) ) {
            case PLUS:
                {
                alt82=1;
                }
                break;
            case MINUS:
                {
                alt82=2;
                }
                break;
            case N:
                {
                alt82=3;
                }
                break;
            case 122:
                {
                alt82=4;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 82, 0, input);

                throw nvae;
            }

            switch (alt82) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:623:3: PLUS N
                    {
                    PLUS290=(Token)match(input,PLUS,FOLLOW_PLUS_in_skip3188); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_PLUS.add(PLUS290);

                    N291=(Token)match(input,N,FOLLOW_N_in_skip3190); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_N.add(N291);



                    // AST REWRITE
                    // elements: N
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 623:10: -> ^( SKIP N )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:623:12: ^( SKIP N )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SKIP, "SKIP"), root_1);

                        adaptor.addChild(root_1, stream_N.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:624:3: MINUS N
                    {
                    MINUS292=(Token)match(input,MINUS,FOLLOW_MINUS_in_skip3201); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_MINUS.add(MINUS292);

                    N293=(Token)match(input,N,FOLLOW_N_in_skip3203); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_N.add(N293);



                    // AST REWRITE
                    // elements: MINUS, N
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 624:11: -> ^( SKIP MINUS N )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:624:13: ^( SKIP MINUS N )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SKIP, "SKIP"), root_1);

                        adaptor.addChild(root_1, stream_MINUS.nextNode());
                        adaptor.addChild(root_1, stream_N.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:625:3: N
                    {
                    N294=(Token)match(input,N,FOLLOW_N_in_skip3216); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_N.add(N294);



                    // AST REWRITE
                    // elements: N
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 625:5: -> ^( SKIP N )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:625:7: ^( SKIP N )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SKIP, "SKIP"), root_1);

                        adaptor.addChild(root_1, stream_N.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:626:3: '?'
                    {
                    char_literal295=(Token)match(input,122,FOLLOW_122_in_skip3227); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_122.add(char_literal295);



                    // AST REWRITE
                    // elements: 122
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 626:6: -> ^( SKIP '?' )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:626:8: ^( SKIP '?' )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SKIP, "SKIP"), root_1);

                        adaptor.addChild(root_1, stream_122.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "skip"

    public static class range_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "range"
    // D:\\tools\\antlr\\test\\MySQLParser.g:628:1: range : ( PLUS N -> ^( RANGE N ) | MINUS N -> ^( RANGE MINUS N ) | N -> ^( RANGE N ) | '?' -> ^( RANGE '?' ) );
    public final MySQLParserParser.range_return range() throws RecognitionException {
        MySQLParserParser.range_return retval = new MySQLParserParser.range_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token PLUS296=null;
        Token N297=null;
        Token MINUS298=null;
        Token N299=null;
        Token N300=null;
        Token char_literal301=null;

        CommonTree PLUS296_tree=null;
        CommonTree N297_tree=null;
        CommonTree MINUS298_tree=null;
        CommonTree N299_tree=null;
        CommonTree N300_tree=null;
        CommonTree char_literal301_tree=null;
        RewriteRuleTokenStream stream_PLUS=new RewriteRuleTokenStream(adaptor,"token PLUS");
        RewriteRuleTokenStream stream_122=new RewriteRuleTokenStream(adaptor,"token 122");
        RewriteRuleTokenStream stream_MINUS=new RewriteRuleTokenStream(adaptor,"token MINUS");
        RewriteRuleTokenStream stream_N=new RewriteRuleTokenStream(adaptor,"token N");

        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:628:7: ( PLUS N -> ^( RANGE N ) | MINUS N -> ^( RANGE MINUS N ) | N -> ^( RANGE N ) | '?' -> ^( RANGE '?' ) )
            int alt83=4;
            switch ( input.LA(1) ) {
            case PLUS:
                {
                alt83=1;
                }
                break;
            case MINUS:
                {
                alt83=2;
                }
                break;
            case N:
                {
                alt83=3;
                }
                break;
            case 122:
                {
                alt83=4;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 83, 0, input);

                throw nvae;
            }

            switch (alt83) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:628:8: PLUS N
                    {
                    PLUS296=(Token)match(input,PLUS,FOLLOW_PLUS_in_range3241); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_PLUS.add(PLUS296);

                    N297=(Token)match(input,N,FOLLOW_N_in_range3243); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_N.add(N297);



                    // AST REWRITE
                    // elements: N
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 628:15: -> ^( RANGE N )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:628:17: ^( RANGE N )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(RANGE, "RANGE"), root_1);

                        adaptor.addChild(root_1, stream_N.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:629:3: MINUS N
                    {
                    MINUS298=(Token)match(input,MINUS,FOLLOW_MINUS_in_range3254); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_MINUS.add(MINUS298);

                    N299=(Token)match(input,N,FOLLOW_N_in_range3256); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_N.add(N299);



                    // AST REWRITE
                    // elements: MINUS, N
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 629:11: -> ^( RANGE MINUS N )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:629:13: ^( RANGE MINUS N )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(RANGE, "RANGE"), root_1);

                        adaptor.addChild(root_1, stream_MINUS.nextNode());
                        adaptor.addChild(root_1, stream_N.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:630:3: N
                    {
                    N300=(Token)match(input,N,FOLLOW_N_in_range3269); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_N.add(N300);



                    // AST REWRITE
                    // elements: N
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 630:4: -> ^( RANGE N )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:630:6: ^( RANGE N )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(RANGE, "RANGE"), root_1);

                        adaptor.addChild(root_1, stream_N.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:631:3: '?'
                    {
                    char_literal301=(Token)match(input,122,FOLLOW_122_in_range3279); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_122.add(char_literal301);



                    // AST REWRITE
                    // elements: 122
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 631:6: -> ^( RANGE '?' )
                    {
                        // D:\\tools\\antlr\\test\\MySQLParser.g:631:8: ^( RANGE '?' )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(RANGE, "RANGE"), root_1);

                        adaptor.addChild(root_1, stream_122.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
        throw e;
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "range"

    // $ANTLR start synpred1_MySQLParser
    public final void synpred1_MySQLParser_fragment() throws RecognitionException {   
        // D:\\tools\\antlr\\test\\MySQLParser.g:214:3: ( LPAREN condition_or RPAREN )
        // D:\\tools\\antlr\\test\\MySQLParser.g:214:4: LPAREN condition_or RPAREN
        {
        match(input,LPAREN,FOLLOW_LPAREN_in_synpred1_MySQLParser789); if (state.failed) return ;
        pushFollow(FOLLOW_condition_or_in_synpred1_MySQLParser791);
        condition_or();

        state._fsp--;
        if (state.failed) return ;
        match(input,RPAREN,FOLLOW_RPAREN_in_synpred1_MySQLParser793); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred1_MySQLParser

    // Delegated rules

    public final boolean synpred1_MySQLParser() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred1_MySQLParser_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA9 dfa9 = new DFA9(this);
    static final String DFA9_eotS =
        "\17\uffff";
    static final String DFA9_eofS =
        "\17\uffff";
    static final String DFA9_minS =
        "\1\100\1\0\15\uffff";
    static final String DFA9_maxS =
        "\1\u0086\1\0\15\uffff";
    static final String DFA9_acceptS =
        "\2\uffff\1\2\13\uffff\1\1";
    static final String DFA9_specialS =
        "\1\uffff\1\0\15\uffff}>";
    static final String[] DFA9_transitionS = {
            "\1\2\5\uffff\1\1\6\uffff\3\2\2\uffff\2\2\10\uffff\1\2\3\uffff"+
            "\1\2\25\uffff\1\2\2\uffff\2\2\12\uffff\2\2",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA9_eot = DFA.unpackEncodedString(DFA9_eotS);
    static final short[] DFA9_eof = DFA.unpackEncodedString(DFA9_eofS);
    static final char[] DFA9_min = DFA.unpackEncodedStringToUnsignedChars(DFA9_minS);
    static final char[] DFA9_max = DFA.unpackEncodedStringToUnsignedChars(DFA9_maxS);
    static final short[] DFA9_accept = DFA.unpackEncodedString(DFA9_acceptS);
    static final short[] DFA9_special = DFA.unpackEncodedString(DFA9_specialS);
    static final short[][] DFA9_transition;

    static {
        int numStates = DFA9_transitionS.length;
        DFA9_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA9_transition[i] = DFA.unpackEncodedString(DFA9_transitionS[i]);
        }
    }

    class DFA9 extends DFA {

        public DFA9(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 9;
            this.eot = DFA9_eot;
            this.eof = DFA9_eof;
            this.min = DFA9_min;
            this.max = DFA9_max;
            this.accept = DFA9_accept;
            this.special = DFA9_special;
            this.transition = DFA9_transition;
        }
        public String getDescription() {
            return "213:1: condition_PAREN : ( ( LPAREN condition_or RPAREN )=> LPAREN condition_or RPAREN -> ^( PRIORITY condition_or ) | condition_expr );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA9_1 = input.LA(1);

                         
                        int index9_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred1_MySQLParser()) ) {s = 14;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index9_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 9, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_start_rule_in_beg332 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_select_command_in_start_rule344 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_update_command_in_start_rule348 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insert_command_in_start_rule352 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_delete_command_in_start_rule356 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_replace_command_in_start_rule360 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_99_in_setclause371 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_updateColumnSpecs_in_setclause373 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateColumnSpec_in_updateColumnSpecs390 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_updateColumnSpecs393 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_updateColumnSpec_in_updateColumnSpecs395 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_columnNameInUpdate_in_updateColumnSpec415 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_EQ_in_updateColumnSpec417 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_in_updateColumnSpec420 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_100_in_insert_command431 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
    public static final BitSet FOLLOW_101_in_insert_command433 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_selected_table_in_insert_command435 = new BitSet(new long[]{0x0000000000000000L,0x0000004000000040L});
    public static final BitSet FOLLOW_LPAREN_in_insert_command440 = new BitSet(new long[]{0x0000000000000000L,0x04000001100C8040L});
    public static final BitSet FOLLOW_column_specs_in_insert_command442 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_insert_command445 = new BitSet(new long[]{0x0000000000000000L,0x0000004000000000L});
    public static final BitSet FOLLOW_102_in_insert_command452 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_LPAREN_in_insert_command454 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_values_in_insert_command456 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_insert_command458 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_103_in_replace_command480 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
    public static final BitSet FOLLOW_101_in_replace_command482 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_selected_table_in_replace_command484 = new BitSet(new long[]{0x0000000000000000L,0x0000004000000040L});
    public static final BitSet FOLLOW_LPAREN_in_replace_command489 = new BitSet(new long[]{0x0000000000000000L,0x04000001100C8040L});
    public static final BitSet FOLLOW_column_specs_in_replace_command491 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_replace_command494 = new BitSet(new long[]{0x0000000000000000L,0x0000004000000000L});
    public static final BitSet FOLLOW_102_in_replace_command501 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_LPAREN_in_replace_command503 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_values_in_replace_command505 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_replace_command507 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_104_in_groupByClause528 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_groupByClause530 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_columnNamesAfterWhere_in_groupByClause532 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_106_in_havingClause556 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_condition_expr_in_havingClause558 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_107_in_orderByClause581 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_orderByClause583 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_columnNamesAfterWhere_in_orderByClause585 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnNameAfterWhere_in_columnNamesAfterWhere607 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_columnNamesAfterWhere610 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_columnNameAfterWhere_in_columnNamesAfterWhere613 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_108_in_selectClause640 = new BitSet(new long[]{0x0000000000000000L,0x00002000000C8000L});
    public static final BitSet FOLLOW_select_list_in_selectClause643 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_109_in_distinct678 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_110_in_whereClause699 = new BitSet(new long[]{0x0000000000000000L,0x06408001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_sqlCondition_in_whereClause701 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_111_in_sqlCondition717 = new BitSet(new long[]{0x0000000000000000L,0x06408001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_condition_or_in_sqlCondition719 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_or_in_sqlCondition730 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_and_in_condition_or747 = new BitSet(new long[]{0x0000000000000002L,0x0001000000000000L});
    public static final BitSet FOLLOW_112_in_condition_or751 = new BitSet(new long[]{0x0000000000000000L,0x06408001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_condition_and_in_condition_or754 = new BitSet(new long[]{0x0000000000000002L,0x0001000000000000L});
    public static final BitSet FOLLOW_condition_PAREN_in_condition_and767 = new BitSet(new long[]{0x0000000000000002L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_condition_and771 = new BitSet(new long[]{0x0000000000000000L,0x06408001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_condition_PAREN_in_condition_and774 = new BitSet(new long[]{0x0000000000000002L,0x0002000000000000L});
    public static final BitSet FOLLOW_LPAREN_in_condition_PAREN796 = new BitSet(new long[]{0x0000000000000000L,0x06408001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_condition_or_in_condition_PAREN798 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_condition_PAREN800 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_expr_in_condition_PAREN810 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_left_in_condition_expr822 = new BitSet(new long[]{0x0000000000000000L,0x018C800001F00020L});
    public static final BitSet FOLLOW_comparisonCondition_in_condition_expr827 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_inCondition_in_condition_expr833 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_isCondition_in_condition_expr839 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_likeCondition_in_condition_expr846 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_betweenCondition_in_condition_expr852 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_in_condition_left867 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_111_in_betweenCondition885 = new BitSet(new long[]{0x0000000000000000L,0x0004000000000000L});
    public static final BitSet FOLLOW_114_in_betweenCondition887 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_between_and_in_betweenCondition889 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_114_in_betweenCondition900 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_between_and_in_betweenCondition902 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_between_and_expression_in_between_and919 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_between_and921 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_between_and_expression_in_between_and924 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_bit_in_between_and_expression936 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_115_in_isCondition947 = new BitSet(new long[]{0x0000000000000000L,0x0000800000000000L});
    public static final BitSet FOLLOW_111_in_isCondition949 = new BitSet(new long[]{0x0000000000000000L,0x0070000000000000L});
    public static final BitSet FOLLOW_condition_is_valobject_in_isCondition951 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_115_in_isCondition961 = new BitSet(new long[]{0x0000000000000000L,0x0070000000000000L});
    public static final BitSet FOLLOW_condition_is_valobject_in_isCondition963 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_116_in_condition_is_valobject981 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_117_in_condition_is_valobject989 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_118_in_condition_is_valobject997 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_111_in_inCondition1013 = new BitSet(new long[]{0x0000000000000000L,0x0080000000000000L});
    public static final BitSet FOLLOW_119_in_inCondition1017 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_subquery_in_inCondition1020 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_inCondition1026 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_inCondition_expr_bits_in_inCondition1028 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_inCondition1030 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_111_in_likeCondition1057 = new BitSet(new long[]{0x0000000000000000L,0x0100000000000000L});
    public static final BitSet FOLLOW_120_in_likeCondition1058 = new BitSet(new long[]{0x0000000000000000L,0x04000001100C8040L});
    public static final BitSet FOLLOW_value_in_likeCondition1061 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_120_in_likeCondition1071 = new BitSet(new long[]{0x0000000000000000L,0x04000001100C8040L});
    public static final BitSet FOLLOW_value_in_likeCondition1073 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_bit_in_inCondition_expr_bits1089 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_inCondition_expr_bits1091 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_bit_in_inCondition_expr_bits1093 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_columnNameAfterWhere_in_identifiers1113 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_identifiers1116 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_identifier_in_identifiers1118 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_relational_op_in_comparisonCondition1130 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition1132 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_bit_in_expr1149 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_subquery_in_expr1153 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_subquery1168 = new BitSet(new long[]{0x0000000000000000L,0x0000100000000000L});
    public static final BitSet FOLLOW_select_command_in_subquery1170 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_subquery1172 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_add_in_expr_bit1190 = new BitSet(new long[]{0x0000000000000002L,0x0000000000001F00L});
    public static final BitSet FOLLOW_BITOR_in_expr_bit1196 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_BITAND_in_expr_bit1201 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_BITXOR_in_expr_bit1204 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_SHIFTLEFT_in_expr_bit1207 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_SHIFTRIGHT_in_expr_bit1210 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_bit1215 = new BitSet(new long[]{0x0000000000000002L,0x0000000000001F00L});
    public static final BitSet FOLLOW_expr_mul_in_expr_add1230 = new BitSet(new long[]{0x0000000000000002L,0x0000000000006000L});
    public static final BitSet FOLLOW_PLUS_in_expr_add1236 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_MINUS_in_expr_add1241 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_mul_in_expr_add1247 = new BitSet(new long[]{0x0000000000000002L,0x0000000000006000L});
    public static final BitSet FOLLOW_expr_sign_in_expr_mul1262 = new BitSet(new long[]{0x0000000000000002L,0x0000000000018000L});
    public static final BitSet FOLLOW_ASTERISK_in_expr_mul1268 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_DIVIDE_in_expr_mul1273 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_sign_in_expr_mul1278 = new BitSet(new long[]{0x0000000000000002L,0x0000000000018000L});
    public static final BitSet FOLLOW_PLUS_in_expr_sign1292 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_pow_in_expr_sign1294 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_MINUS_in_expr_sign1304 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_pow_in_expr_sign1306 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_pow_in_expr_sign1316 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_expr_in_expr_pow1327 = new BitSet(new long[]{0x0000000000000002L,0x0000000000020000L});
    public static final BitSet FOLLOW_EXPONENT_in_expr_pow1331 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_expr_in_expr_pow1334 = new BitSet(new long[]{0x0000000000000002L,0x0000000000020000L});
    public static final BitSet FOLLOW_interval_clause_in_expr_expr1354 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_expr_expr1364 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_LPAREN_in_expr_expr1367 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE0C1L,0x0000000000000060L});
    public static final BitSet FOLLOW_values_func_in_expr_expr1369 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_expr_expr1372 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_expr_expr1387 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_value_in_expr_expr1406 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_boolean_literal_in_expr_expr1410 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_118_in_expr_expr1414 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_121_in_expr_expr1418 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTERVAL_in_interval_clause1455 = new BitSet(new long[]{0x0000000000000000L,0x0400000000084000L});
    public static final BitSet FOLLOW_inner_value_in_interval_clause1457 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_unit_in_interval_clause1459 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_unit1500 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_122_in_inner_value1524 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_MINUS_in_inner_value1528 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_N_in_inner_value1530 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_N_in_inner_value1534 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_or_in_sql_condition1545 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_relational_op0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_123_in_fromClause1586 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_selected_table_in_fromClause1589 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_displayed_column_in_select_list1599 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_select_list1603 = new BitSet(new long[]{0x0000000000000000L,0x00000000000C8000L});
    public static final BitSet FOLLOW_displayed_column_in_select_list1605 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_distinct_col_in_select_list1619 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_displayed_column1640 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_LPAREN_in_displayed_column1643 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE0C1L,0x0000000000000060L});
    public static final BitSet FOLLOW_values_func_in_displayed_column1645 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_displayed_column1648 = new BitSet(new long[]{0x0000000000000002L,0x0000000000040000L,0x0000000000000080L});
    public static final BitSet FOLLOW_alias_in_displayed_column1651 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_displayed_column1668 = new BitSet(new long[]{0x0000000000000002L,0x0000000000040000L,0x0000000000000080L});
    public static final BitSet FOLLOW_alias_in_displayed_column1670 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_displayed_column1687 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_LPAREN_in_displayed_column1689 = new BitSet(new long[]{0x0000000000000000L,0x00002000000C8000L});
    public static final BitSet FOLLOW_distinct_col_in_displayed_column1691 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_displayed_column1693 = new BitSet(new long[]{0x0000000000000002L,0x0000000000040000L,0x0000000000000080L});
    public static final BitSet FOLLOW_alias_in_displayed_column1695 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_table_alias_in_displayed_column1713 = new BitSet(new long[]{0x0000000000000000L,0x00000000000C8000L});
    public static final BitSet FOLLOW_column_in_displayed_column1717 = new BitSet(new long[]{0x0000000000000002L,0x0000000000040000L,0x0000000000000080L});
    public static final BitSet FOLLOW_alias_in_displayed_column1720 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_distinct_in_distinct_col1747 = new BitSet(new long[]{0x0000000000000000L,0x00000000000C8000L});
    public static final BitSet FOLLOW_displayed_column_in_distinct_col1749 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_distinct_col1753 = new BitSet(new long[]{0x0000000000000000L,0x00000000000C8000L});
    public static final BitSet FOLLOW_displayed_column_in_distinct_col1755 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_table_alias_in_columnNameAfterWhere1777 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_identifier_in_columnNameAfterWhere1780 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_table_alias_in_columnNameAfterWhere1795 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_identifier_in_columnNameAfterWhere1798 = new BitSet(new long[]{0x0000000000000000L,0x0000000002000000L});
    public static final BitSet FOLLOW_ASC_in_columnNameAfterWhere1801 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_table_alias_in_columnNameAfterWhere1816 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_identifier_in_columnNameAfterWhere1819 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
    public static final BitSet FOLLOW_DESC_in_columnNameAfterWhere1822 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_table_alias_in_columnNameInUpdate1843 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_identifier_in_columnNameInUpdate1846 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_table_alias1858 = new BitSet(new long[]{0x0000000000000000L,0x0000000008000000L});
    public static final BitSet FOLLOW_DOT_in_table_alias1860 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ASTERISK_in_column1876 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_N_in_column1882 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_column1886 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_in_values1898 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_values1902 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_in_values1904 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_expr_in_values_func1925 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_values_func1929 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_in_values_func1932 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_N_in_value1948 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NUMBER_in_value1952 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_122_in_value1956 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_value1960 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_in_value1963 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_value1965 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_quoted_string_in_value1970 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_column_spec_in_value1981 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_column_spec_in_column_specs1993 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_column_specs1997 = new BitSet(new long[]{0x0000000000000000L,0x04000001100C8040L});
    public static final BitSet FOLLOW_column_spec_in_column_specs1999 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_a_table_in_selected_table2020 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_selected_table2023 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_a_table_in_selected_table2025 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_table_spec_in_a_table2045 = new BitSet(new long[]{0x0000000000000002L,0xD000000000040000L,0x000000000000008FL});
    public static final BitSet FOLLOW_alias_in_a_table2048 = new BitSet(new long[]{0x0000000000000002L,0xD000000000000000L,0x000000000000000FL});
    public static final BitSet FOLLOW_join_claus_in_a_table2051 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_schema_name_in_table_spec2076 = new BitSet(new long[]{0x0000000000000000L,0x0000000008000000L});
    public static final BitSet FOLLOW_DOT_in_table_spec2078 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_table_name_in_table_spec2083 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_subquery_in_table_spec2089 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_join_type_in_join_claus2104 = new BitSet(new long[]{0x0000000000000000L,0x1000000000000000L});
    public static final BitSet FOLLOW_124_in_join_claus2107 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_table_spec_in_join_claus2109 = new BitSet(new long[]{0x0000000000000000L,0x2000000000040000L,0x0000000000000080L});
    public static final BitSet FOLLOW_alias_in_join_claus2112 = new BitSet(new long[]{0x0000000000000000L,0x2000000000000000L});
    public static final BitSet FOLLOW_125_in_join_claus2115 = new BitSet(new long[]{0x0000000000000000L,0x04000001100C8040L});
    public static final BitSet FOLLOW_column_spec_in_join_claus2117 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_EQ_in_join_claus2119 = new BitSet(new long[]{0x0000000000000000L,0x04000001100C8040L});
    public static final BitSet FOLLOW_column_spec_in_join_claus2121 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_126_in_join_type2154 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_127_in_join_type2162 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_outer_in_join_type2164 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_128_in_join_type2177 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_outer_in_join_type2179 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_129_in_join_type2193 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_outer_in_join_type2195 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_130_in_join_type2209 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_131_in_join_type2219 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_132_in_outer2237 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_table_name2256 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_table_name_in_column_spec2268 = new BitSet(new long[]{0x0000000000000000L,0x0000000008000000L});
    public static final BitSet FOLLOW_DOT_in_column_spec2270 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_identifier_in_column_spec2274 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ASTERISK_in_column_spec2288 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_schema_name2304 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_boolean_literal0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_135_in_alias2334 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_identifier_in_alias2338 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_identifier2356 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_STRING_in_quoted_string2782 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectClause_in_select_command2967 = new BitSet(new long[]{0x0000000000000002L,0x08004D0000000000L,0x000000000000E500L});
    public static final BitSet FOLLOW_fromClause_in_select_command2970 = new BitSet(new long[]{0x0000000000000002L,0x00004D0000000000L,0x000000000000E500L});
    public static final BitSet FOLLOW_whereClause_in_select_command2975 = new BitSet(new long[]{0x0000000000000002L,0x00000D0000000000L,0x000000000000E500L});
    public static final BitSet FOLLOW_groupByClause_in_select_command2979 = new BitSet(new long[]{0x0000000000000002L,0x00000C0000000000L,0x000000000000E500L});
    public static final BitSet FOLLOW_havingClause_in_select_command2982 = new BitSet(new long[]{0x0000000000000002L,0x0000080000000000L,0x000000000000E500L});
    public static final BitSet FOLLOW_orderByClause_in_select_command2985 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x000000000000E500L});
    public static final BitSet FOLLOW_limitClause_in_select_command2990 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x000000000000C500L});
    public static final BitSet FOLLOW_indexClause_in_select_command2994 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x000000000000C000L});
    public static final BitSet FOLLOW_for_update_in_select_command2997 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_136_in_indexClause3013 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_indexClause3015 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_LPAREN_in_indexClause3017 = new BitSet(new long[]{0x0000000000000000L,0x00002000000C8000L});
    public static final BitSet FOLLOW_select_list_in_indexClause3019 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_indexClause3022 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_138_in_indexClause3027 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_137_in_indexClause3029 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_LPAREN_in_indexClause3031 = new BitSet(new long[]{0x0000000000000000L,0x00002000000C8000L});
    public static final BitSet FOLLOW_select_list_in_indexClause3033 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_indexClause3035 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_139_in_delete_command3045 = new BitSet(new long[]{0x0000000000000000L,0x0800000000000000L});
    public static final BitSet FOLLOW_fromClause_in_delete_command3047 = new BitSet(new long[]{0x0000000000000002L,0x0000480000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_whereClause_in_delete_command3049 = new BitSet(new long[]{0x0000000000000002L,0x0000080000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_orderByClause_in_delete_command3052 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_limitClause_in_delete_command3057 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_140_in_update_command3085 = new BitSet(new long[]{0x0000000000000000L,0x06400001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_selected_table_in_update_command3087 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
    public static final BitSet FOLLOW_setclause_in_update_command3090 = new BitSet(new long[]{0x0000000000000002L,0x0000480000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_whereClause_in_update_command3092 = new BitSet(new long[]{0x0000000000000002L,0x0000080000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_orderByClause_in_update_command3095 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_limitClause_in_update_command3100 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_141_in_limitClause3129 = new BitSet(new long[]{0x0000000000000000L,0x0400000000086000L});
    public static final BitSet FOLLOW_skip_in_limitClause3132 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_COMMA_in_limitClause3134 = new BitSet(new long[]{0x0000000000000000L,0x0400000000086000L});
    public static final BitSet FOLLOW_range_in_limitClause3139 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_142_in_for_update3157 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000001000L});
    public static final BitSet FOLLOW_140_in_for_update3159 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_143_in_for_update3168 = new BitSet(new long[]{0x0000000000000000L,0x0080000000000000L});
    public static final BitSet FOLLOW_119_in_for_update3170 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000010000L});
    public static final BitSet FOLLOW_144_in_for_update3172 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000020000L});
    public static final BitSet FOLLOW_145_in_for_update3174 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_PLUS_in_skip3188 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_N_in_skip3190 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_MINUS_in_skip3201 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_N_in_skip3203 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_N_in_skip3216 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_122_in_skip3227 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_PLUS_in_range3241 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_N_in_range3243 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_MINUS_in_range3254 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_N_in_range3256 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_N_in_range3269 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_122_in_range3279 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_synpred1_MySQLParser789 = new BitSet(new long[]{0x0000000000000000L,0x06408001100CE041L,0x0000000000000060L});
    public static final BitSet FOLLOW_condition_or_in_synpred1_MySQLParser791 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_RPAREN_in_synpred1_MySQLParser793 = new BitSet(new long[]{0x0000000000000002L});

}