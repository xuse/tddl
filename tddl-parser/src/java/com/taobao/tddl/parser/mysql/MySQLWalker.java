// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\tools\\antlr\\test\\MySQLWalker.g 2011-09-13 18:10:48

 package  com.taobao.tddl.parser.mysql; 

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.sqlobjecttree.*;

import com.taobao.tddl.sqlobjecttree.common.expression.ExpressionGroup;

import com.taobao.tddl.sqlobjecttree.common.value.function.*;
import com.taobao.tddl.sqlobjecttree.common.value.*;

import com.taobao.tddl.sqlobjecttree.mysql.*;

import com.taobao.tddl.sqlobjecttree.common.expression.OrExpressionGroup;
import com.taobao.tddl.sqlobjecttree.common.*;
import com.taobao.tddl.sqlobjecttree.mysql.function.*;
import static com.taobao.tddl.sqlobjecttree.Utils.*;
import com.taobao.tddl.sqlobjecttree.mysql.function.datefunction.*;
import com.taobao.tddl.sqlobjecttree.JOIN_TYPE;

import com.taobao.tddl.sqlobjecttree.common.expression.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.parser.AntlrStringStream;



import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;import java.util.Stack;
import java.util.List;
import java.util.ArrayList;


public class MySQLWalker extends TreeParser {
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
    public static final int T__139=139;
    public static final int FORUPDATE=61;
    public static final int T__138=138;
    public static final int POSITIVE=63;
    public static final int T__137=137;
    public static final int T__136=136;
    public static final int DISPLAYED_COLUMN=13;
    public static final int ASC=89;
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
    public static final int T__144=144;
    public static final int SKIP=58;
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
    public static final int T__108=108;
    public static final int COMMA=68;
    public static final int IS=35;
    public static final int T__109=109;
    public static final int LEFT=26;
    public static final int REPLACE=22;
    public static final int T__103=103;
    public static final int T__104=104;
    public static final int SHIFTRIGHT=76;
    public static final int T__105=105;
    public static final int BITXOR=74;
    public static final int COLUMN=8;
    public static final int T__106=106;
    public static final int T__111=111;
    public static final int T__110=110;
    public static final int T__113=113;
    public static final int T__112=112;
    public static final int PLUS=77;
    public static final int QUOTED_STRING=96;
    public static final int DOT=91;
    public static final int SHAREMODE=60;
    public static final int LIKE=39;
    public static final int OUTER=24;
    public static final int TIME_FUC_NAME=65;
    public static final int DOUBLEQUOTED_STRING=97;
    public static final int NEGATIVE=62;
    public static final int NOT_LIKE=40;
    public static final int TIME_FUC=66;
    public static final int NOT_BETWEEN=41;
    public static final int RANGE=59;
    public static final int INFINITE=38;
    public static final int RIGHT=27;
    public static final int SET=52;
    public static final int HAVING=44;
    public static final int T__102=102;
    public static final int T__101=101;
    public static final int INSERT_VAL=47;
    public static final int T__100=100;
    public static final int MINUS=78;
    public static final int JOIN=29;
    public static final int CONDITION_OR_NOT=23;
    public static final int UNION=30;
    public static final int NOT_EQ=86;
    public static final int LTH=84;
    public static final int COLUMNS=50;
    public static final int COL_TAB=54;
    public static final int SET_ELE=53;
    public static final int ARROW=94;
    public static final int ISNOT=34;
    public static final int DESC=90;
    public static final int BETWEEN=42;
    public static final int TABLENAME=5;
    public static final int LEQ=87;

    // delegates
    // delegators


        public MySQLWalker(TreeNodeStream input) {
            this(input, new RecognizerSharedState());
        }
        public MySQLWalker(TreeNodeStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return MySQLWalker.tokenNames; }
    public String getGrammarFileName() { return "D:\\tools\\antlr\\test\\MySQLWalker.g"; }





    		protected MySQLConsistStringRegister consistStr;
    		public MySQLConsistStringRegister getConsist(){
    				return consistStr;
    		}
    		public void setConsist(MySQLConsistStringRegister consist){
    				this.consistStr=consist;
    		}
    		protected  MySQLFunctionRegister funcreg;


        		public MySQLFunctionRegister getFunc() {
    				return funcreg;
    			}
    			public void setFunc(MySQLFunctionRegister funcreg) {
    				this.funcreg = funcreg;
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


    public static class beg_return extends TreeRuleReturnScope {
        public DMLCommon obj;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "beg"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:70:1: beg returns [DMLCommon obj] : start_rule ;
    public final MySQLWalker.beg_return beg() throws RecognitionException {
        MySQLWalker.beg_return retval = new MySQLWalker.beg_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.start_rule_return start_rule1 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:71:9: ( start_rule )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:71:10: start_rule
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_start_rule_in_beg57);
            start_rule1=start_rule();

            state._fsp--;

            adaptor.addChild(root_0, start_rule1.getTree());
            retval.obj =(start_rule1!=null?start_rule1.obj:null);

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "beg"

    public static class start_rule_return extends TreeRuleReturnScope {
        public DMLCommon obj;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "start_rule"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:74:1: start_rule returns [DMLCommon obj] : ( select_command[null] | insert_command | update_command | delete_command | replace_command );
    public final MySQLWalker.start_rule_return start_rule() throws RecognitionException {
        MySQLWalker.start_rule_return retval = new MySQLWalker.start_rule_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.select_command_return select_command2 = null;

        MySQLWalker.insert_command_return insert_command3 = null;

        MySQLWalker.update_command_return update_command4 = null;

        MySQLWalker.delete_command_return delete_command5 = null;

        MySQLWalker.replace_command_return replace_command6 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:75:2: ( select_command[null] | insert_command | update_command | delete_command | replace_command )
            int alt1=5;
            switch ( input.LA(1) ) {
            case SELECT:
                {
                alt1=1;
                }
                break;
            case INSERT:
                {
                alt1=2;
                }
                break;
            case UPDATE:
                {
                alt1=3;
                }
                break;
            case DELETE:
                {
                alt1=4;
                }
                break;
            case REPLACE:
                {
                alt1=5;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }

            switch (alt1) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:75:3: select_command[null]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_select_command_in_start_rule74);
                    select_command2=select_command(null);

                    state._fsp--;

                    adaptor.addChild(root_0, select_command2.getTree());
                    retval.obj =(select_command2!=null?select_command2.select:null);

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:76:3: insert_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_insert_command_in_start_rule80);
                    insert_command3=insert_command();

                    state._fsp--;

                    adaptor.addChild(root_0, insert_command3.getTree());
                    retval.obj =(insert_command3!=null?insert_command3.ins:null);

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:77:3: update_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_update_command_in_start_rule85);
                    update_command4=update_command();

                    state._fsp--;

                    adaptor.addChild(root_0, update_command4.getTree());
                    retval.obj =(update_command4!=null?update_command4.update:null);

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:78:3: delete_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_delete_command_in_start_rule90);
                    delete_command5=delete_command();

                    state._fsp--;

                    adaptor.addChild(root_0, delete_command5.getTree());
                    retval.obj =(delete_command5!=null?delete_command5.del:null);

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:79:3: replace_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_replace_command_in_start_rule95);
                    replace_command6=replace_command();

                    state._fsp--;

                    adaptor.addChild(root_0, replace_command6.getTree());
                    retval.obj =(replace_command6!=null?replace_command6.replace:null);

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "start_rule"

    public static class setclause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "setclause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:82:1: setclause[Update update] : ^( SET ( updateColumnSpecs[$update] )+ ) ;
    public final MySQLWalker.setclause_return setclause(Update update) throws RecognitionException {
        MySQLWalker.setclause_return retval = new MySQLWalker.setclause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SET7=null;
        MySQLWalker.updateColumnSpecs_return updateColumnSpecs8 = null;


        CommonTree SET7_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:83:2: ( ^( SET ( updateColumnSpecs[$update] )+ ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:83:3: ^( SET ( updateColumnSpecs[$update] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SET7=(CommonTree)match(input,SET,FOLLOW_SET_in_setclause108); 
            SET7_tree = (CommonTree)adaptor.dupNode(SET7);

            root_1 = (CommonTree)adaptor.becomeRoot(SET7_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\tools\\antlr\\test\\MySQLWalker.g:83:9: ( updateColumnSpecs[$update] )+
            int cnt2=0;
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( (LA2_0==SET_ELE) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:83:9: updateColumnSpecs[$update]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_updateColumnSpecs_in_setclause110);
            	    updateColumnSpecs8=updateColumnSpecs(update);

            	    state._fsp--;

            	    adaptor.addChild(root_1, updateColumnSpecs8.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt2 >= 1 ) break loop2;
                        EarlyExitException eee =
                            new EarlyExitException(2, input);
                        throw eee;
                }
                cnt2++;
            } while (true);


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "setclause"

    public static class updateColumnSpecs_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "updateColumnSpecs"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:86:1: updateColumnSpecs[Update update] : ^( SET_ELE updateColumnSpec[$update] ) ;
    public final MySQLWalker.updateColumnSpecs_return updateColumnSpecs(Update update) throws RecognitionException {
        MySQLWalker.updateColumnSpecs_return retval = new MySQLWalker.updateColumnSpecs_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SET_ELE9=null;
        MySQLWalker.updateColumnSpec_return updateColumnSpec10 = null;


        CommonTree SET_ELE9_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:87:2: ( ^( SET_ELE updateColumnSpec[$update] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:87:3: ^( SET_ELE updateColumnSpec[$update] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SET_ELE9=(CommonTree)match(input,SET_ELE,FOLLOW_SET_ELE_in_updateColumnSpecs127); 
            SET_ELE9_tree = (CommonTree)adaptor.dupNode(SET_ELE9);

            root_1 = (CommonTree)adaptor.becomeRoot(SET_ELE9_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_updateColumnSpec_in_updateColumnSpecs129);
            updateColumnSpec10=updateColumnSpec(update);

            state._fsp--;

            adaptor.addChild(root_1, updateColumnSpec10.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "updateColumnSpecs"

    public static class updateColumnSpec_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "updateColumnSpec"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:90:1: updateColumnSpec[Update update] : ^( EQ ( table_alias )? identifier expr[$update.getIndexHolder()] ) ;
    public final MySQLWalker.updateColumnSpec_return updateColumnSpec(Update update) throws RecognitionException {
        MySQLWalker.updateColumnSpec_return retval = new MySQLWalker.updateColumnSpec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree EQ11=null;
        MySQLWalker.table_alias_return table_alias12 = null;

        MySQLWalker.identifier_return identifier13 = null;

        MySQLWalker.expr_return expr14 = null;


        CommonTree EQ11_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:91:2: ( ^( EQ ( table_alias )? identifier expr[$update.getIndexHolder()] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:91:3: ^( EQ ( table_alias )? identifier expr[$update.getIndexHolder()] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            EQ11=(CommonTree)match(input,EQ,FOLLOW_EQ_in_updateColumnSpec145); 
            EQ11_tree = (CommonTree)adaptor.dupNode(EQ11);

            root_1 = (CommonTree)adaptor.becomeRoot(EQ11_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\tools\\antlr\\test\\MySQLWalker.g:91:8: ( table_alias )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==COL_TAB) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:91:8: table_alias
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_table_alias_in_updateColumnSpec147);
                    table_alias12=table_alias();

                    state._fsp--;

                    adaptor.addChild(root_1, table_alias12.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_updateColumnSpec150);
            identifier13=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier13.getTree());
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_expr_in_updateColumnSpec152);
            expr14=expr(update.getIndexHolder());

            state._fsp--;

            adaptor.addChild(root_1, expr14.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            	update.addSetElement((identifier13!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier13.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier13.start))):null),(table_alias12!=null?table_alias12.aText:null),(expr14!=null?expr14.valueObj:null));
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "updateColumnSpec"

    public static class insert_command_return extends TreeRuleReturnScope {
        public Insert ins;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "insert_command"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:97:1: insert_command returns [Insert ins] : ^( INSERT tables[$ins] ( column_specs[$ins] )* values[$ins] ) ;
    public final MySQLWalker.insert_command_return insert_command() throws RecognitionException {
        MySQLWalker.insert_command_return retval = new MySQLWalker.insert_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree INSERT15=null;
        MySQLWalker.tables_return tables16 = null;

        MySQLWalker.column_specs_return column_specs17 = null;

        MySQLWalker.values_return values18 = null;


        CommonTree INSERT15_tree=null;

        retval.ins =new Insert();
        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:99:2: ( ^( INSERT tables[$ins] ( column_specs[$ins] )* values[$ins] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:99:3: ^( INSERT tables[$ins] ( column_specs[$ins] )* values[$ins] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            INSERT15=(CommonTree)match(input,INSERT,FOLLOW_INSERT_in_insert_command175); 
            INSERT15_tree = (CommonTree)adaptor.dupNode(INSERT15);

            root_1 = (CommonTree)adaptor.becomeRoot(INSERT15_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_tables_in_insert_command177);
            tables16=tables(retval.ins);

            state._fsp--;

            adaptor.addChild(root_1, tables16.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:99:25: ( column_specs[$ins] )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==COLUMNS) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:99:25: column_specs[$ins]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_column_specs_in_insert_command180);
            	    column_specs17=column_specs(retval.ins);

            	    state._fsp--;

            	    adaptor.addChild(root_1, column_specs17.getTree());

            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_values_in_insert_command184);
            values18=values(retval.ins);

            state._fsp--;

            adaptor.addChild(root_1, values18.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "insert_command"

    public static class replace_command_return extends TreeRuleReturnScope {
        public Replace replace;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "replace_command"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:102:1: replace_command returns [Replace replace] : ^( REPLACE tables[$replace] ( column_specs[$replace] )* values[$replace] ) ;
    public final MySQLWalker.replace_command_return replace_command() throws RecognitionException {
        MySQLWalker.replace_command_return retval = new MySQLWalker.replace_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree REPLACE19=null;
        MySQLWalker.tables_return tables20 = null;

        MySQLWalker.column_specs_return column_specs21 = null;

        MySQLWalker.values_return values22 = null;


        CommonTree REPLACE19_tree=null;

        retval.replace =new Replace();
        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:104:2: ( ^( REPLACE tables[$replace] ( column_specs[$replace] )* values[$replace] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:104:3: ^( REPLACE tables[$replace] ( column_specs[$replace] )* values[$replace] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            REPLACE19=(CommonTree)match(input,REPLACE,FOLLOW_REPLACE_in_replace_command205); 
            REPLACE19_tree = (CommonTree)adaptor.dupNode(REPLACE19);

            root_1 = (CommonTree)adaptor.becomeRoot(REPLACE19_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_tables_in_replace_command207);
            tables20=tables(retval.replace);

            state._fsp--;

            adaptor.addChild(root_1, tables20.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:104:30: ( column_specs[$replace] )*
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( (LA5_0==COLUMNS) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:104:30: column_specs[$replace]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_column_specs_in_replace_command210);
            	    column_specs21=column_specs(retval.replace);

            	    state._fsp--;

            	    adaptor.addChild(root_1, column_specs21.getTree());

            	    }
            	    break;

            	default :
            	    break loop5;
                }
            } while (true);

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_values_in_replace_command214);
            values22=values(retval.replace);

            state._fsp--;

            adaptor.addChild(root_1, values22.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "replace_command"

    public static class values_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "values"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:107:1: values[Insert ins] : ^( INSERT_VAL ( expr[$ins.getIndexHolder()] )* ) ;
    public final MySQLWalker.values_return values(Insert ins) throws RecognitionException {
        MySQLWalker.values_return retval = new MySQLWalker.values_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree INSERT_VAL23=null;
        MySQLWalker.expr_return expr24 = null;


        CommonTree INSERT_VAL23_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:108:9: ( ^( INSERT_VAL ( expr[$ins.getIndexHolder()] )* ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:108:10: ^( INSERT_VAL ( expr[$ins.getIndexHolder()] )* )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            INSERT_VAL23=(CommonTree)match(input,INSERT_VAL,FOLLOW_INSERT_VAL_in_values237); 
            INSERT_VAL23_tree = (CommonTree)adaptor.dupNode(INSERT_VAL23);

            root_1 = (CommonTree)adaptor.becomeRoot(INSERT_VAL23_tree, root_1);



            if ( input.LA(1)==Token.DOWN ) {
                match(input, Token.DOWN, null); 
                // D:\\tools\\antlr\\test\\MySQLWalker.g:108:23: ( expr[$ins.getIndexHolder()] )*
                loop6:
                do {
                    int alt6=2;
                    int LA6_0 = input.LA(1);

                    if ( ((LA6_0>=SUBQUERY && LA6_0<=COLUMN)||LA6_0==QUTED_STR||LA6_0==COLUMNAST||LA6_0==CONSIST||(LA6_0>=NEGATIVE && LA6_0<=INTERVAL)||(LA6_0>=BITOR && LA6_0<=DIVIDE)||(LA6_0>=ID && LA6_0<=N)||LA6_0==NUMBER||LA6_0==118||(LA6_0>=121 && LA6_0<=122)||(LA6_0>=133 && LA6_0<=134)) ) {
                        alt6=1;
                    }


                    switch (alt6) {
                	case 1 :
                	    // D:\\tools\\antlr\\test\\MySQLWalker.g:108:24: expr[$ins.getIndexHolder()]
                	    {
                	    _last = (CommonTree)input.LT(1);
                	    pushFollow(FOLLOW_expr_in_values240);
                	    expr24=expr(ins.getIndexHolder());

                	    state._fsp--;

                	    adaptor.addChild(root_1, expr24.getTree());
                	    ins.addValue((expr24!=null?expr24.valueObj:null));

                	    }
                	    break;

                	default :
                	    break loop6;
                    }
                } while (true);


                match(input, Token.UP, null); 
            }adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "values"

    public static class column_specs_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "column_specs"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:111:1: column_specs[Insert ins] : ^( COLUMNS ( column_spec[$ins] )+ ) ;
    public final MySQLWalker.column_specs_return column_specs(Insert ins) throws RecognitionException {
        MySQLWalker.column_specs_return retval = new MySQLWalker.column_specs_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree COLUMNS25=null;
        MySQLWalker.column_spec_return column_spec26 = null;


        CommonTree COLUMNS25_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:112:2: ( ^( COLUMNS ( column_spec[$ins] )+ ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:112:3: ^( COLUMNS ( column_spec[$ins] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            COLUMNS25=(CommonTree)match(input,COLUMNS,FOLLOW_COLUMNS_in_column_specs258); 
            COLUMNS25_tree = (CommonTree)adaptor.dupNode(COLUMNS25);

            root_1 = (CommonTree)adaptor.becomeRoot(COLUMNS25_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\tools\\antlr\\test\\MySQLWalker.g:112:13: ( column_spec[$ins] )+
            int cnt7=0;
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( (LA7_0==COLUMN) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:112:13: column_spec[$ins]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_column_spec_in_column_specs260);
            	    column_spec26=column_spec(ins);

            	    state._fsp--;

            	    adaptor.addChild(root_1, column_spec26.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt7 >= 1 ) break loop7;
                        EarlyExitException eee =
                            new EarlyExitException(7, input);
                        throw eee;
                }
                cnt7++;
            } while (true);


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "column_specs"

    public static class column_spec_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "column_spec"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:115:1: column_spec[Insert ins] : ^( COLUMN identifier ( table_name )? ) ;
    public final MySQLWalker.column_spec_return column_spec(Insert ins) throws RecognitionException {
        MySQLWalker.column_spec_return retval = new MySQLWalker.column_spec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree COLUMN27=null;
        MySQLWalker.identifier_return identifier28 = null;

        MySQLWalker.table_name_return table_name29 = null;


        CommonTree COLUMN27_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:116:2: ( ^( COLUMN identifier ( table_name )? ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:116:3: ^( COLUMN identifier ( table_name )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            COLUMN27=(CommonTree)match(input,COLUMN,FOLLOW_COLUMN_in_column_spec276); 
            COLUMN27_tree = (CommonTree)adaptor.dupNode(COLUMN27);

            root_1 = (CommonTree)adaptor.becomeRoot(COLUMN27_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_column_spec278);
            identifier28=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier28.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:116:23: ( table_name )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==ID) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:116:23: table_name
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_table_name_in_column_spec280);
                    table_name29=table_name();

                    state._fsp--;

                    adaptor.addChild(root_1, table_name29.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		ins.addColumnTandC((table_name29!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(table_name29.start),
              input.getTreeAdaptor().getTokenStopIndex(table_name29.start))):null),(identifier28!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier28.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier28.start))):null));
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "column_spec"

    public static class whereClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "whereClause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:122:1: whereClause[WhereCondition where] : ^( WHERE sqlCondition[$where] ) ;
    public final MySQLWalker.whereClause_return whereClause(WhereCondition where) throws RecognitionException {
        MySQLWalker.whereClause_return retval = new MySQLWalker.whereClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree WHERE30=null;
        MySQLWalker.sqlCondition_return sqlCondition31 = null;


        CommonTree WHERE30_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:123:2: ( ^( WHERE sqlCondition[$where] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:123:3: ^( WHERE sqlCondition[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            WHERE30=(CommonTree)match(input,WHERE,FOLLOW_WHERE_in_whereClause297); 
            WHERE30_tree = (CommonTree)adaptor.dupNode(WHERE30);

            root_1 = (CommonTree)adaptor.becomeRoot(WHERE30_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_sqlCondition_in_whereClause299);
            sqlCondition31=sqlCondition(where);

            state._fsp--;

            adaptor.addChild(root_1, sqlCondition31.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "whereClause"

    public static class groupByClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "groupByClause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:126:1: groupByClause[WhereCondition where] : ^( GROUPBY columnNamesAfterWhere ) ;
    public final MySQLWalker.groupByClause_return groupByClause(WhereCondition where) throws RecognitionException {
        MySQLWalker.groupByClause_return retval = new MySQLWalker.groupByClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree GROUPBY32=null;
        MySQLWalker.columnNamesAfterWhere_return columnNamesAfterWhere33 = null;


        CommonTree GROUPBY32_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:127:2: ( ^( GROUPBY columnNamesAfterWhere ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:127:3: ^( GROUPBY columnNamesAfterWhere )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            GROUPBY32=(CommonTree)match(input,GROUPBY,FOLLOW_GROUPBY_in_groupByClause314); 
            GROUPBY32_tree = (CommonTree)adaptor.dupNode(GROUPBY32);

            root_1 = (CommonTree)adaptor.becomeRoot(GROUPBY32_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_columnNamesAfterWhere_in_groupByClause316);
            columnNamesAfterWhere33=columnNamesAfterWhere();

            state._fsp--;

            adaptor.addChild(root_1, columnNamesAfterWhere33.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		where.setGroupByColumns((columnNamesAfterWhere33!=null?columnNamesAfterWhere33.ret:null));
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "groupByClause"

    public static class havingClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "havingClause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:133:1: havingClause[HavingCondition having] : ^( HAVING condition_PAREN[having.getHolder(),having.getExpGroup()] ) ;
    public final MySQLWalker.havingClause_return havingClause(HavingCondition having) throws RecognitionException {
        MySQLWalker.havingClause_return retval = new MySQLWalker.havingClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree HAVING34=null;
        MySQLWalker.condition_PAREN_return condition_PAREN35 = null;


        CommonTree HAVING34_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:134:9: ( ^( HAVING condition_PAREN[having.getHolder(),having.getExpGroup()] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:134:10: ^( HAVING condition_PAREN[having.getHolder(),having.getExpGroup()] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            HAVING34=(CommonTree)match(input,HAVING,FOLLOW_HAVING_in_havingClause340); 
            HAVING34_tree = (CommonTree)adaptor.dupNode(HAVING34);

            root_1 = (CommonTree)adaptor.becomeRoot(HAVING34_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_condition_PAREN_in_havingClause342);
            condition_PAREN35=condition_PAREN(having.getHolder(), having.getExpGroup());

            state._fsp--;

            adaptor.addChild(root_1, condition_PAREN35.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "havingClause"

    public static class orderByClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orderByClause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:137:1: orderByClause[WhereCondition where] : ^( ORDERBY columnNamesAfterWhere ) ;
    public final MySQLWalker.orderByClause_return orderByClause(WhereCondition where) throws RecognitionException {
        MySQLWalker.orderByClause_return retval = new MySQLWalker.orderByClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ORDERBY36=null;
        MySQLWalker.columnNamesAfterWhere_return columnNamesAfterWhere37 = null;


        CommonTree ORDERBY36_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:138:2: ( ^( ORDERBY columnNamesAfterWhere ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:138:3: ^( ORDERBY columnNamesAfterWhere )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            ORDERBY36=(CommonTree)match(input,ORDERBY,FOLLOW_ORDERBY_in_orderByClause371); 
            ORDERBY36_tree = (CommonTree)adaptor.dupNode(ORDERBY36);

            root_1 = (CommonTree)adaptor.becomeRoot(ORDERBY36_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_columnNamesAfterWhere_in_orderByClause373);
            columnNamesAfterWhere37=columnNamesAfterWhere();

            state._fsp--;

            adaptor.addChild(root_1, columnNamesAfterWhere37.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		where.setOrderByColumns((columnNamesAfterWhere37!=null?columnNamesAfterWhere37.ret:null));
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "orderByClause"

    public static class columnNamesAfterWhere_return extends TreeRuleReturnScope {
        public List<OrderByEle> ret;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnNamesAfterWhere"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:144:1: columnNamesAfterWhere returns [List<OrderByEle> ret] : ( columnNameAfterWhere[$ret] )+ ;
    public final MySQLWalker.columnNamesAfterWhere_return columnNamesAfterWhere() throws RecognitionException {
        MySQLWalker.columnNamesAfterWhere_return retval = new MySQLWalker.columnNamesAfterWhere_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.columnNameAfterWhere_return columnNameAfterWhere38 = null;




        	retval.ret = new ArrayList<OrderByEle>();

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:148:2: ( ( columnNameAfterWhere[$ret] )+ )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:148:3: ( columnNameAfterWhere[$ret] )+
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\tools\\antlr\\test\\MySQLWalker.g:148:3: ( columnNameAfterWhere[$ret] )+
            int cnt9=0;
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( ((LA9_0>=ASC && LA9_0<=DESC)) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:148:4: columnNameAfterWhere[$ret]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_columnNameAfterWhere_in_columnNamesAfterWhere396);
            	    columnNameAfterWhere38=columnNameAfterWhere(retval.ret);

            	    state._fsp--;

            	    adaptor.addChild(root_0, columnNameAfterWhere38.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt9 >= 1 ) break loop9;
                        EarlyExitException eee =
                            new EarlyExitException(9, input);
                        throw eee;
                }
                cnt9++;
            } while (true);


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnNamesAfterWhere"

    public static class columnNameAfterWhere_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnNameAfterWhere"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:150:1: columnNameAfterWhere[List<OrderByEle> orderByEles] : ( ^( ASC identifier ( table_alias )? ) | ^( DESC identifier ( table_alias )? ) );
    public final MySQLWalker.columnNameAfterWhere_return columnNameAfterWhere(List<OrderByEle> orderByEles) throws RecognitionException {
        MySQLWalker.columnNameAfterWhere_return retval = new MySQLWalker.columnNameAfterWhere_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ASC39=null;
        CommonTree DESC42=null;
        MySQLWalker.identifier_return identifier40 = null;

        MySQLWalker.table_alias_return table_alias41 = null;

        MySQLWalker.identifier_return identifier43 = null;

        MySQLWalker.table_alias_return table_alias44 = null;


        CommonTree ASC39_tree=null;
        CommonTree DESC42_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:151:2: ( ^( ASC identifier ( table_alias )? ) | ^( DESC identifier ( table_alias )? ) )
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0==ASC) ) {
                alt12=1;
            }
            else if ( (LA12_0==DESC) ) {
                alt12=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }
            switch (alt12) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:151:3: ^( ASC identifier ( table_alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ASC39=(CommonTree)match(input,ASC,FOLLOW_ASC_in_columnNameAfterWhere410); 
                    ASC39_tree = (CommonTree)adaptor.dupNode(ASC39);

                    root_1 = (CommonTree)adaptor.becomeRoot(ASC39_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_identifier_in_columnNameAfterWhere413);
                    identifier40=identifier();

                    state._fsp--;

                    adaptor.addChild(root_1, identifier40.getTree());
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:151:21: ( table_alias )?
                    int alt10=2;
                    int LA10_0 = input.LA(1);

                    if ( (LA10_0==COL_TAB) ) {
                        alt10=1;
                    }
                    switch (alt10) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:151:21: table_alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_table_alias_in_columnNameAfterWhere415);
                            table_alias41=table_alias();

                            state._fsp--;

                            adaptor.addChild(root_1, table_alias41.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		orderByEles.add(new OrderByEle((table_alias41!=null?table_alias41.aText:null),(identifier40!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(identifier40.start),
                      input.getTreeAdaptor().getTokenStopIndex(identifier40.start))):null),true));
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:155:3: ^( DESC identifier ( table_alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    DESC42=(CommonTree)match(input,DESC,FOLLOW_DESC_in_columnNameAfterWhere425); 
                    DESC42_tree = (CommonTree)adaptor.dupNode(DESC42);

                    root_1 = (CommonTree)adaptor.becomeRoot(DESC42_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_identifier_in_columnNameAfterWhere428);
                    identifier43=identifier();

                    state._fsp--;

                    adaptor.addChild(root_1, identifier43.getTree());
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:155:22: ( table_alias )?
                    int alt11=2;
                    int LA11_0 = input.LA(1);

                    if ( (LA11_0==COL_TAB) ) {
                        alt11=1;
                    }
                    switch (alt11) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:155:22: table_alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_table_alias_in_columnNameAfterWhere430);
                            table_alias44=table_alias();

                            state._fsp--;

                            adaptor.addChild(root_1, table_alias44.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		orderByEles.add(new OrderByEle((table_alias44!=null?table_alias44.aText:null),(identifier43!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(identifier43.start),
                      input.getTreeAdaptor().getTokenStopIndex(identifier43.start))):null),false));
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnNameAfterWhere"

    public static class selectClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "selectClause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:161:1: selectClause[Select select] : ^( SELECT select_list[$select] ) ;
    public final MySQLWalker.selectClause_return selectClause(Select select) throws RecognitionException {
        MySQLWalker.selectClause_return retval = new MySQLWalker.selectClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SELECT45=null;
        MySQLWalker.select_list_return select_list46 = null;


        CommonTree SELECT45_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:162:9: ( ^( SELECT select_list[$select] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:162:10: ^( SELECT select_list[$select] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SELECT45=(CommonTree)match(input,SELECT,FOLLOW_SELECT_in_selectClause454); 
            SELECT45_tree = (CommonTree)adaptor.dupNode(SELECT45);

            root_1 = (CommonTree)adaptor.becomeRoot(SELECT45_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_select_list_in_selectClause457);
            select_list46=select_list(select);

            state._fsp--;

            adaptor.addChild(root_1, select_list46.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "selectClause"

    public static class sqlCondition_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "sqlCondition"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:165:1: sqlCondition[WhereCondition where] : ( ^( CONDITION_OR_NOT condition[where.getHolder(),where.getExpGroup(),false] ) | ^( CONDITION_OR condition[where.getHolder(),where.getExpGroup(),false] ) );
    public final MySQLWalker.sqlCondition_return sqlCondition(WhereCondition where) throws RecognitionException {
        MySQLWalker.sqlCondition_return retval = new MySQLWalker.sqlCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree CONDITION_OR_NOT47=null;
        CommonTree CONDITION_OR49=null;
        MySQLWalker.condition_return condition48 = null;

        MySQLWalker.condition_return condition50 = null;


        CommonTree CONDITION_OR_NOT47_tree=null;
        CommonTree CONDITION_OR49_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:166:2: ( ^( CONDITION_OR_NOT condition[where.getHolder(),where.getExpGroup(),false] ) | ^( CONDITION_OR condition[where.getHolder(),where.getExpGroup(),false] ) )
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( (LA13_0==CONDITION_OR_NOT) ) {
                alt13=1;
            }
            else if ( (LA13_0==CONDITION_OR) ) {
                alt13=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 13, 0, input);

                throw nvae;
            }
            switch (alt13) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:166:3: ^( CONDITION_OR_NOT condition[where.getHolder(),where.getExpGroup(),false] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CONDITION_OR_NOT47=(CommonTree)match(input,CONDITION_OR_NOT,FOLLOW_CONDITION_OR_NOT_in_sqlCondition488); 
                    CONDITION_OR_NOT47_tree = (CommonTree)adaptor.dupNode(CONDITION_OR_NOT47);

                    root_1 = (CommonTree)adaptor.becomeRoot(CONDITION_OR_NOT47_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_condition_in_sqlCondition490);
                    condition48=condition(where.getHolder(), where.getExpGroup(), false);

                    state._fsp--;

                    adaptor.addChild(root_1, condition48.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:167:3: ^( CONDITION_OR condition[where.getHolder(),where.getExpGroup(),false] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CONDITION_OR49=(CommonTree)match(input,CONDITION_OR,FOLLOW_CONDITION_OR_in_sqlCondition497); 
                    CONDITION_OR49_tree = (CommonTree)adaptor.dupNode(CONDITION_OR49);

                    root_1 = (CommonTree)adaptor.becomeRoot(CONDITION_OR49_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_condition_in_sqlCondition499);
                    condition50=condition(where.getHolder(), where.getExpGroup(), false);

                    state._fsp--;

                    adaptor.addChild(root_1, condition50.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "sqlCondition"

    public static class condition_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:170:1: condition[BindIndexHolder where,ExpressionGroup eg,boolean isPriority] : ( ^( 'OR' (s1= condition[where,orExp,$isPriority] )+ ) | ^( 'AND' ( condition[where,andExp,$isPriority] )+ ) | condition_PAREN[where,$eg] | ^( PRIORITY condition[where,$eg,true] ) );
    public final MySQLWalker.condition_return condition(BindIndexHolder where, ExpressionGroup eg, boolean isPriority) throws RecognitionException {
        MySQLWalker.condition_return retval = new MySQLWalker.condition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree string_literal51=null;
        CommonTree string_literal52=null;
        CommonTree PRIORITY55=null;
        MySQLWalker.condition_return s1 = null;

        MySQLWalker.condition_return condition53 = null;

        MySQLWalker.condition_PAREN_return condition_PAREN54 = null;

        MySQLWalker.condition_return condition56 = null;


        CommonTree string_literal51_tree=null;
        CommonTree string_literal52_tree=null;
        CommonTree PRIORITY55_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:171:2: ( ^( 'OR' (s1= condition[where,orExp,$isPriority] )+ ) | ^( 'AND' ( condition[where,andExp,$isPriority] )+ ) | condition_PAREN[where,$eg] | ^( PRIORITY condition[where,$eg,true] ) )
            int alt16=4;
            switch ( input.LA(1) ) {
            case 112:
                {
                alt16=1;
                }
                break;
            case 113:
                {
                alt16=2;
                }
                break;
            case IN:
            case ISNOT:
            case IS:
            case LIKE:
            case NOT_LIKE:
            case EQ:
            case LTH:
            case GTH:
            case NOT_EQ:
            case LEQ:
            case GEQ:
                {
                alt16=3;
                }
                break;
            case PRIORITY:
                {
                alt16=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }

            switch (alt16) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:172:2: ^( 'OR' (s1= condition[where,orExp,$isPriority] )+ )
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    		OrExpressionGroup orExp=new OrExpressionGroup();
                    		eg.addExpressionGroup(orExp);
                    	
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    string_literal51=(CommonTree)match(input,112,FOLLOW_112_in_condition517); 
                    string_literal51_tree = (CommonTree)adaptor.dupNode(string_literal51);

                    root_1 = (CommonTree)adaptor.becomeRoot(string_literal51_tree, root_1);



                    match(input, Token.DOWN, null); 
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:175:12: (s1= condition[where,orExp,$isPriority] )+
                    int cnt14=0;
                    loop14:
                    do {
                        int alt14=2;
                        int LA14_0 = input.LA(1);

                        if ( (LA14_0==IN||(LA14_0>=ISNOT && LA14_0<=IS)||(LA14_0>=LIKE && LA14_0<=NOT_LIKE)||LA14_0==PRIORITY||LA14_0==EQ||(LA14_0>=LTH && LA14_0<=GEQ)||(LA14_0>=112 && LA14_0<=113)) ) {
                            alt14=1;
                        }


                        switch (alt14) {
                    	case 1 :
                    	    // D:\\tools\\antlr\\test\\MySQLWalker.g:175:12: s1= condition[where,orExp,$isPriority]
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    pushFollow(FOLLOW_condition_in_condition521);
                    	    s1=condition(where, orExp, isPriority);

                    	    state._fsp--;

                    	    adaptor.addChild(root_1, s1.getTree());

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt14 >= 1 ) break loop14;
                                EarlyExitException eee =
                                    new EarlyExitException(14, input);
                                throw eee;
                        }
                        cnt14++;
                    } while (true);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:177:2: ^( 'AND' ( condition[where,andExp,$isPriority] )+ )
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    		ExpressionGroup andExp=new ExpressionGroup();
                    		eg.addExpressionGroup(andExp);
                    	
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    string_literal52=(CommonTree)match(input,113,FOLLOW_113_in_condition533); 
                    string_literal52_tree = (CommonTree)adaptor.dupNode(string_literal52);

                    root_1 = (CommonTree)adaptor.becomeRoot(string_literal52_tree, root_1);



                    match(input, Token.DOWN, null); 
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:180:11: ( condition[where,andExp,$isPriority] )+
                    int cnt15=0;
                    loop15:
                    do {
                        int alt15=2;
                        int LA15_0 = input.LA(1);

                        if ( (LA15_0==IN||(LA15_0>=ISNOT && LA15_0<=IS)||(LA15_0>=LIKE && LA15_0<=NOT_LIKE)||LA15_0==PRIORITY||LA15_0==EQ||(LA15_0>=LTH && LA15_0<=GEQ)||(LA15_0>=112 && LA15_0<=113)) ) {
                            alt15=1;
                        }


                        switch (alt15) {
                    	case 1 :
                    	    // D:\\tools\\antlr\\test\\MySQLWalker.g:180:11: condition[where,andExp,$isPriority]
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    pushFollow(FOLLOW_condition_in_condition535);
                    	    condition53=condition(where, andExp, isPriority);

                    	    state._fsp--;

                    	    adaptor.addChild(root_1, condition53.getTree());

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt15 >= 1 ) break loop15;
                                EarlyExitException eee =
                                    new EarlyExitException(15, input);
                                throw eee;
                        }
                        cnt15++;
                    } while (true);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:181:3: condition_PAREN[where,$eg]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_condition_PAREN_in_condition542);
                    condition_PAREN54=condition_PAREN(where, eg);

                    state._fsp--;

                    adaptor.addChild(root_0, condition_PAREN54.getTree());

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:182:3: ^( PRIORITY condition[where,$eg,true] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    PRIORITY55=(CommonTree)match(input,PRIORITY,FOLLOW_PRIORITY_in_condition548); 
                    PRIORITY55_tree = (CommonTree)adaptor.dupNode(PRIORITY55);

                    root_1 = (CommonTree)adaptor.becomeRoot(PRIORITY55_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_condition_in_condition550);
                    condition56=condition(where, eg, true);

                    state._fsp--;

                    adaptor.addChild(root_1, condition56.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition"

    public static class condition_PAREN_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition_PAREN"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:185:1: condition_PAREN[BindIndexHolder where,ExpressionGroup exp] : ( condition_expr[$where,$exp] )+ ;
    public final MySQLWalker.condition_PAREN_return condition_PAREN(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        MySQLWalker.condition_PAREN_return retval = new MySQLWalker.condition_PAREN_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.condition_expr_return condition_expr57 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:186:2: ( ( condition_expr[$where,$exp] )+ )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:186:3: ( condition_expr[$where,$exp] )+
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\tools\\antlr\\test\\MySQLWalker.g:186:3: ( condition_expr[$where,$exp] )+
            int cnt17=0;
            loop17:
            do {
                int alt17=2;
                alt17 = dfa17.predict(input);
                switch (alt17) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:186:3: condition_expr[$where,$exp]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_condition_expr_in_condition_PAREN563);
            	    condition_expr57=condition_expr(where, exp);

            	    state._fsp--;

            	    adaptor.addChild(root_0, condition_expr57.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt17 >= 1 ) break loop17;
                        EarlyExitException eee =
                            new EarlyExitException(17, input);
                        throw eee;
                }
                cnt17++;
            } while (true);


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition_PAREN"

    public static class condition_expr_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "condition_expr"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:189:1: condition_expr[BindIndexHolder where,ExpressionGroup exp] : ( comparisonCondition[$where,$exp] | inCondition[$where,$exp] | isCondition[$where,$exp] | likeCondition[$where,$exp] );
    public final MySQLWalker.condition_expr_return condition_expr(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        MySQLWalker.condition_expr_return retval = new MySQLWalker.condition_expr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.comparisonCondition_return comparisonCondition58 = null;

        MySQLWalker.inCondition_return inCondition59 = null;

        MySQLWalker.isCondition_return isCondition60 = null;

        MySQLWalker.likeCondition_return likeCondition61 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:190:2: ( comparisonCondition[$where,$exp] | inCondition[$where,$exp] | isCondition[$where,$exp] | likeCondition[$where,$exp] )
            int alt18=4;
            switch ( input.LA(1) ) {
            case EQ:
            case LTH:
            case GTH:
            case NOT_EQ:
            case LEQ:
            case GEQ:
                {
                alt18=1;
                }
                break;
            case IN:
                {
                alt18=2;
                }
                break;
            case ISNOT:
            case IS:
                {
                alt18=3;
                }
                break;
            case LIKE:
            case NOT_LIKE:
                {
                alt18=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 18, 0, input);

                throw nvae;
            }

            switch (alt18) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:190:4: comparisonCondition[$where,$exp]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_comparisonCondition_in_condition_expr578);
                    comparisonCondition58=comparisonCondition(where, exp);

                    state._fsp--;

                    adaptor.addChild(root_0, comparisonCondition58.getTree());

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:191:4: inCondition[$where,$exp]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_inCondition_in_condition_expr584);
                    inCondition59=inCondition(where, exp);

                    state._fsp--;

                    adaptor.addChild(root_0, inCondition59.getTree());

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:192:4: isCondition[$where,$exp]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_isCondition_in_condition_expr591);
                    isCondition60=isCondition(where, exp);

                    state._fsp--;

                    adaptor.addChild(root_0, isCondition60.getTree());

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:193:4: likeCondition[$where,$exp]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_likeCondition_in_condition_expr598);
                    likeCondition61=likeCondition(where, exp);

                    state._fsp--;

                    adaptor.addChild(root_0, likeCondition61.getTree());

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "condition_expr"

    public static class betweenCondition_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "betweenCondition"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:197:1: betweenCondition : ( ^( NOT_BETWEEN between_and ) | ^( BETWEEN between_and ) );
    public final MySQLWalker.betweenCondition_return betweenCondition() throws RecognitionException {
        MySQLWalker.betweenCondition_return retval = new MySQLWalker.betweenCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree NOT_BETWEEN62=null;
        CommonTree BETWEEN64=null;
        MySQLWalker.between_and_return between_and63 = null;

        MySQLWalker.between_and_return between_and65 = null;


        CommonTree NOT_BETWEEN62_tree=null;
        CommonTree BETWEEN64_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:198:2: ( ^( NOT_BETWEEN between_and ) | ^( BETWEEN between_and ) )
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==NOT_BETWEEN) ) {
                alt19=1;
            }
            else if ( (LA19_0==BETWEEN) ) {
                alt19=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 19, 0, input);

                throw nvae;
            }
            switch (alt19) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:198:3: ^( NOT_BETWEEN between_and )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NOT_BETWEEN62=(CommonTree)match(input,NOT_BETWEEN,FOLLOW_NOT_BETWEEN_in_betweenCondition612); 
                    NOT_BETWEEN62_tree = (CommonTree)adaptor.dupNode(NOT_BETWEEN62);

                    root_1 = (CommonTree)adaptor.becomeRoot(NOT_BETWEEN62_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        _last = (CommonTree)input.LT(1);
                        pushFollow(FOLLOW_between_and_in_betweenCondition614);
                        between_and63=between_and();

                        state._fsp--;

                        adaptor.addChild(root_1, between_and63.getTree());

                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:199:3: ^( BETWEEN between_and )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BETWEEN64=(CommonTree)match(input,BETWEEN,FOLLOW_BETWEEN_in_betweenCondition620); 
                    BETWEEN64_tree = (CommonTree)adaptor.dupNode(BETWEEN64);

                    root_1 = (CommonTree)adaptor.becomeRoot(BETWEEN64_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        _last = (CommonTree)input.LT(1);
                        pushFollow(FOLLOW_between_and_in_betweenCondition622);
                        between_and65=between_and();

                        state._fsp--;

                        adaptor.addChild(root_1, between_and65.getTree());

                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "betweenCondition"

    public static class between_and_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "between_and"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:202:1: between_and : ;
    public final MySQLWalker.between_and_return between_and() throws RecognitionException {
        MySQLWalker.between_and_return retval = new MySQLWalker.between_and_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:203:2: ()
            // D:\\tools\\antlr\\test\\MySQLWalker.g:204:2: 
            {
            root_0 = (CommonTree)adaptor.nil();

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "between_and"

    public static class likeCondition_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "likeCondition"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:206:1: likeCondition[BindIndexHolder where,ExpressionGroup exp] : ( ^( NOT_LIKE expr[$where] left_cond[$where] ) | ^( LIKE expr[$where] left_cond[$where] ) );
    public final MySQLWalker.likeCondition_return likeCondition(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        MySQLWalker.likeCondition_return retval = new MySQLWalker.likeCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree NOT_LIKE66=null;
        CommonTree LIKE69=null;
        MySQLWalker.expr_return expr67 = null;

        MySQLWalker.left_cond_return left_cond68 = null;

        MySQLWalker.expr_return expr70 = null;

        MySQLWalker.left_cond_return left_cond71 = null;


        CommonTree NOT_LIKE66_tree=null;
        CommonTree LIKE69_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:207:2: ( ^( NOT_LIKE expr[$where] left_cond[$where] ) | ^( LIKE expr[$where] left_cond[$where] ) )
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==NOT_LIKE) ) {
                alt20=1;
            }
            else if ( (LA20_0==LIKE) ) {
                alt20=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 20, 0, input);

                throw nvae;
            }
            switch (alt20) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:207:3: ^( NOT_LIKE expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NOT_LIKE66=(CommonTree)match(input,NOT_LIKE,FOLLOW_NOT_LIKE_in_likeCondition647); 
                    NOT_LIKE66_tree = (CommonTree)adaptor.dupNode(NOT_LIKE66);

                    root_1 = (CommonTree)adaptor.becomeRoot(NOT_LIKE66_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_likeCondition649);
                    expr67=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr67.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_likeCondition652);
                    left_cond68=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond68.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    			NotLike notlike=new NotLike();
                    		notlike.setLeft((left_cond68!=null?left_cond68.ret:null));
                    		notlike.setRight((expr67!=null?expr67.valueObj:null));
                    		exp.addExpression(notlike);
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:213:3: ^( LIKE expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    LIKE69=(CommonTree)match(input,LIKE,FOLLOW_LIKE_in_likeCondition660); 
                    LIKE69_tree = (CommonTree)adaptor.dupNode(LIKE69);

                    root_1 = (CommonTree)adaptor.becomeRoot(LIKE69_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_likeCondition662);
                    expr70=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr70.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_likeCondition665);
                    left_cond71=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond71.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		Like like=new Like();
                    		like.setLeft((left_cond71!=null?left_cond71.ret:null));
                    		like.setRight((expr70!=null?expr70.valueObj:null));
                    		exp.addExpression(like);
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "likeCondition"

    public static class isCondition_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "isCondition"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:221:1: isCondition[BindIndexHolder where,ExpressionGroup exp] : ( ^( ISNOT NULL left_cond[$where] ) | ^( IS NULL left_cond[$where] ) );
    public final MySQLWalker.isCondition_return isCondition(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        MySQLWalker.isCondition_return retval = new MySQLWalker.isCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ISNOT72=null;
        CommonTree NULL73=null;
        CommonTree IS75=null;
        CommonTree NULL76=null;
        MySQLWalker.left_cond_return left_cond74 = null;

        MySQLWalker.left_cond_return left_cond77 = null;


        CommonTree ISNOT72_tree=null;
        CommonTree NULL73_tree=null;
        CommonTree IS75_tree=null;
        CommonTree NULL76_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:222:2: ( ^( ISNOT NULL left_cond[$where] ) | ^( IS NULL left_cond[$where] ) )
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( (LA21_0==ISNOT) ) {
                alt21=1;
            }
            else if ( (LA21_0==IS) ) {
                alt21=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 21, 0, input);

                throw nvae;
            }
            switch (alt21) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:222:3: ^( ISNOT NULL left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ISNOT72=(CommonTree)match(input,ISNOT,FOLLOW_ISNOT_in_isCondition681); 
                    ISNOT72_tree = (CommonTree)adaptor.dupNode(ISNOT72);

                    root_1 = (CommonTree)adaptor.becomeRoot(ISNOT72_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    NULL73=(CommonTree)match(input,NULL,FOLLOW_NULL_in_isCondition683); 
                    NULL73_tree = (CommonTree)adaptor.dupNode(NULL73);

                    adaptor.addChild(root_1, NULL73_tree);

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_isCondition685);
                    left_cond74=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond74.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		IsNot arg=new IsNot();
                    		arg.setLeft((left_cond74!=null?left_cond74.ret:null));
                    		arg.setRight(null);
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:228:3: ^( IS NULL left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    IS75=(CommonTree)match(input,IS,FOLLOW_IS_in_isCondition693); 
                    IS75_tree = (CommonTree)adaptor.dupNode(IS75);

                    root_1 = (CommonTree)adaptor.becomeRoot(IS75_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    NULL76=(CommonTree)match(input,NULL,FOLLOW_NULL_in_isCondition695); 
                    NULL76_tree = (CommonTree)adaptor.dupNode(NULL76);

                    adaptor.addChild(root_1, NULL76_tree);

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_isCondition697);
                    left_cond77=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond77.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		Is arg=new Is();
                    		arg.setLeft((left_cond77!=null?left_cond77.ret:null));
                    		arg.setRight(null);
                    		exp.addExpression(arg);
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "isCondition"

    public static class comparisonCondition_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparisonCondition"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:237:1: comparisonCondition[BindIndexHolder where,ExpressionGroup exp] : ( ^( EQ expr[$where] left_cond[$where] ) | ^( NOT_EQ expr[$where] left_cond[$where] ) | ^( LTH expr[$where] left_cond[$where] ) | ^( GTH expr[$where] left_cond[$where] ) | ^( LEQ expr[$where] left_cond[$where] ) | ^( GEQ expr[$where] left_cond[$where] ) );
    public final MySQLWalker.comparisonCondition_return comparisonCondition(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        MySQLWalker.comparisonCondition_return retval = new MySQLWalker.comparisonCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree EQ78=null;
        CommonTree NOT_EQ81=null;
        CommonTree LTH84=null;
        CommonTree GTH87=null;
        CommonTree LEQ90=null;
        CommonTree GEQ93=null;
        MySQLWalker.expr_return expr79 = null;

        MySQLWalker.left_cond_return left_cond80 = null;

        MySQLWalker.expr_return expr82 = null;

        MySQLWalker.left_cond_return left_cond83 = null;

        MySQLWalker.expr_return expr85 = null;

        MySQLWalker.left_cond_return left_cond86 = null;

        MySQLWalker.expr_return expr88 = null;

        MySQLWalker.left_cond_return left_cond89 = null;

        MySQLWalker.expr_return expr91 = null;

        MySQLWalker.left_cond_return left_cond92 = null;

        MySQLWalker.expr_return expr94 = null;

        MySQLWalker.left_cond_return left_cond95 = null;


        CommonTree EQ78_tree=null;
        CommonTree NOT_EQ81_tree=null;
        CommonTree LTH84_tree=null;
        CommonTree GTH87_tree=null;
        CommonTree LEQ90_tree=null;
        CommonTree GEQ93_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:238:2: ( ^( EQ expr[$where] left_cond[$where] ) | ^( NOT_EQ expr[$where] left_cond[$where] ) | ^( LTH expr[$where] left_cond[$where] ) | ^( GTH expr[$where] left_cond[$where] ) | ^( LEQ expr[$where] left_cond[$where] ) | ^( GEQ expr[$where] left_cond[$where] ) )
            int alt22=6;
            switch ( input.LA(1) ) {
            case EQ:
                {
                alt22=1;
                }
                break;
            case NOT_EQ:
                {
                alt22=2;
                }
                break;
            case LTH:
                {
                alt22=3;
                }
                break;
            case GTH:
                {
                alt22=4;
                }
                break;
            case LEQ:
                {
                alt22=5;
                }
                break;
            case GEQ:
                {
                alt22=6;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 22, 0, input);

                throw nvae;
            }

            switch (alt22) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:238:3: ^( EQ expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    EQ78=(CommonTree)match(input,EQ,FOLLOW_EQ_in_comparisonCondition714); 
                    EQ78_tree = (CommonTree)adaptor.dupNode(EQ78);

                    root_1 = (CommonTree)adaptor.becomeRoot(EQ78_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition716);
                    expr79=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr79.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition719);
                    left_cond80=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond80.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		Equivalent arg=new Equivalent();
                    		arg.setLeft((left_cond80!=null?left_cond80.ret:null));
                    		arg.setRight((expr79!=null?expr79.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:245:3: ^( NOT_EQ expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NOT_EQ81=(CommonTree)match(input,NOT_EQ,FOLLOW_NOT_EQ_in_comparisonCondition729); 
                    NOT_EQ81_tree = (CommonTree)adaptor.dupNode(NOT_EQ81);

                    root_1 = (CommonTree)adaptor.becomeRoot(NOT_EQ81_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition731);
                    expr82=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr82.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition734);
                    left_cond83=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond83.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		NotEquivalent arg=new NotEquivalent();
                    		arg.setLeft((left_cond83!=null?left_cond83.ret:null));
                    		arg.setRight((expr82!=null?expr82.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:252:3: ^( LTH expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    LTH84=(CommonTree)match(input,LTH,FOLLOW_LTH_in_comparisonCondition744); 
                    LTH84_tree = (CommonTree)adaptor.dupNode(LTH84);

                    root_1 = (CommonTree)adaptor.becomeRoot(LTH84_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition746);
                    expr85=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr85.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition749);
                    left_cond86=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond86.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		LessThan arg=new LessThan();
                    		arg.setLeft((left_cond86!=null?left_cond86.ret:null));
                    		arg.setRight((expr85!=null?expr85.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:259:3: ^( GTH expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    GTH87=(CommonTree)match(input,GTH,FOLLOW_GTH_in_comparisonCondition759); 
                    GTH87_tree = (CommonTree)adaptor.dupNode(GTH87);

                    root_1 = (CommonTree)adaptor.becomeRoot(GTH87_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition761);
                    expr88=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr88.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition764);
                    left_cond89=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond89.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		GreaterThan arg=new GreaterThan();
                    		arg.setLeft((left_cond89!=null?left_cond89.ret:null));
                    		arg.setRight((expr88!=null?expr88.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:266:3: ^( LEQ expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    LEQ90=(CommonTree)match(input,LEQ,FOLLOW_LEQ_in_comparisonCondition774); 
                    LEQ90_tree = (CommonTree)adaptor.dupNode(LEQ90);

                    root_1 = (CommonTree)adaptor.becomeRoot(LEQ90_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition776);
                    expr91=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr91.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition779);
                    left_cond92=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond92.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		LessThanOrEquivalent arg=new LessThanOrEquivalent();
                    		arg.setLeft((left_cond92!=null?left_cond92.ret:null));
                    		arg.setRight((expr91!=null?expr91.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 6 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:273:3: ^( GEQ expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    GEQ93=(CommonTree)match(input,GEQ,FOLLOW_GEQ_in_comparisonCondition789); 
                    GEQ93_tree = (CommonTree)adaptor.dupNode(GEQ93);

                    root_1 = (CommonTree)adaptor.becomeRoot(GEQ93_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition791);
                    expr94=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr94.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition794);
                    left_cond95=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond95.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		GreaterThanOrEquivalent arg=new GreaterThanOrEquivalent();
                    		arg.setLeft((left_cond95!=null?left_cond95.ret:null));
                    		arg.setRight((expr94!=null?expr94.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "comparisonCondition"

    public static class left_cond_return extends TreeRuleReturnScope {
        public Object ret;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "left_cond"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:282:1: left_cond[BindIndexHolder where] returns [Object ret] : ^( CONDITION_LEFT expr[$where] ) ;
    public final MySQLWalker.left_cond_return left_cond(BindIndexHolder where) throws RecognitionException {
        MySQLWalker.left_cond_return retval = new MySQLWalker.left_cond_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree CONDITION_LEFT96=null;
        MySQLWalker.expr_return expr97 = null;


        CommonTree CONDITION_LEFT96_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:284:2: ( ^( CONDITION_LEFT expr[$where] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:284:3: ^( CONDITION_LEFT expr[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            CONDITION_LEFT96=(CommonTree)match(input,CONDITION_LEFT,FOLLOW_CONDITION_LEFT_in_left_cond816); 
            CONDITION_LEFT96_tree = (CommonTree)adaptor.dupNode(CONDITION_LEFT96);

            root_1 = (CommonTree)adaptor.becomeRoot(CONDITION_LEFT96_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_expr_in_left_cond818);
            expr97=expr(where);

            state._fsp--;

            adaptor.addChild(root_1, expr97.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		retval.ret =(expr97!=null?expr97.valueObj:null);
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "left_cond"

    public static class in_list_return extends TreeRuleReturnScope {
        public List list;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "in_list"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:290:1: in_list[BindIndexHolder where] returns [List list] : ^( IN_LISTS inCondition_expr_adds[$where] ) ;
    public final MySQLWalker.in_list_return in_list(BindIndexHolder where) throws RecognitionException {
        MySQLWalker.in_list_return retval = new MySQLWalker.in_list_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree IN_LISTS98=null;
        MySQLWalker.inCondition_expr_adds_return inCondition_expr_adds99 = null;


        CommonTree IN_LISTS98_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:291:2: ( ^( IN_LISTS inCondition_expr_adds[$where] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:291:3: ^( IN_LISTS inCondition_expr_adds[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            IN_LISTS98=(CommonTree)match(input,IN_LISTS,FOLLOW_IN_LISTS_in_in_list839); 
            IN_LISTS98_tree = (CommonTree)adaptor.dupNode(IN_LISTS98);

            root_1 = (CommonTree)adaptor.becomeRoot(IN_LISTS98_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_inCondition_expr_adds_in_in_list841);
            inCondition_expr_adds99=inCondition_expr_adds(where);

            state._fsp--;

            adaptor.addChild(root_1, inCondition_expr_adds99.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		retval.list =(inCondition_expr_adds99!=null?inCondition_expr_adds99.list:null);
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "in_list"

    public static class inCondition_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "inCondition"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:297:1: inCondition[BindIndexHolder where,ExpressionGroup exp] : ^( IN (not= 'NOT' )? ( subquery[$where] )? ( in_list[$where] )? left_cond[$where] ) ;
    public final MySQLWalker.inCondition_return inCondition(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        MySQLWalker.inCondition_return retval = new MySQLWalker.inCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree not=null;
        CommonTree IN100=null;
        MySQLWalker.subquery_return subquery101 = null;

        MySQLWalker.in_list_return in_list102 = null;

        MySQLWalker.left_cond_return left_cond103 = null;


        CommonTree not_tree=null;
        CommonTree IN100_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:298:2: ( ^( IN (not= 'NOT' )? ( subquery[$where] )? ( in_list[$where] )? left_cond[$where] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:298:3: ^( IN (not= 'NOT' )? ( subquery[$where] )? ( in_list[$where] )? left_cond[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            IN100=(CommonTree)match(input,IN,FOLLOW_IN_in_inCondition861); 
            IN100_tree = (CommonTree)adaptor.dupNode(IN100);

            root_1 = (CommonTree)adaptor.becomeRoot(IN100_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\tools\\antlr\\test\\MySQLWalker.g:298:11: (not= 'NOT' )?
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( (LA23_0==111) ) {
                alt23=1;
            }
            switch (alt23) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:298:11: not= 'NOT'
                    {
                    _last = (CommonTree)input.LT(1);
                    not=(CommonTree)match(input,111,FOLLOW_111_in_inCondition865); 
                    not_tree = (CommonTree)adaptor.dupNode(not);

                    adaptor.addChild(root_1, not_tree);


                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:298:19: ( subquery[$where] )?
            int alt24=2;
            int LA24_0 = input.LA(1);

            if ( (LA24_0==SUBQUERY) ) {
                alt24=1;
            }
            switch (alt24) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:298:19: subquery[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_subquery_in_inCondition868);
                    subquery101=subquery(where);

                    state._fsp--;

                    adaptor.addChild(root_1, subquery101.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:298:37: ( in_list[$where] )?
            int alt25=2;
            int LA25_0 = input.LA(1);

            if ( (LA25_0==IN_LISTS) ) {
                alt25=1;
            }
            switch (alt25) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:298:37: in_list[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_in_list_in_inCondition872);
                    in_list102=in_list(where);

                    state._fsp--;

                    adaptor.addChild(root_1, in_list102.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_left_cond_in_inCondition877);
            left_cond103=left_cond(where);

            state._fsp--;

            adaptor.addChild(root_1, left_cond103.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		if((not!=null?not.getText():null)!=null){
            			if((subquery101!=null?subquery101.subselect:null)!=null){
            					NotInExpression arg=new NotInExpression();
            					arg.setLeft((left_cond103!=null?left_cond103.ret:null));
            					arg.setRight((subquery101!=null?subquery101.subselect:null));
            					exp.addExpression(arg);
            			}
            			else if((in_list102!=null?in_list102.list:null)!=null){
            					NotInExpression arg=new NotInExpression();
            					arg.setLeft((left_cond103!=null?left_cond103.ret:null));
            					arg.setRight((in_list102!=null?in_list102.list:null));
            					exp.addExpression(arg);
            			}
            		}else{
            			if((subquery101!=null?subquery101.subselect:null)!=null){
            					InExpression arg=new InExpression();
            					arg.setLeft((left_cond103!=null?left_cond103.ret:null));
            					arg.setRight((subquery101!=null?subquery101.subselect:null));
            					exp.addExpression(arg);
            			}
            			else if((in_list102!=null?in_list102.list:null)!=null){
            					InExpression arg=new InExpression();
            					arg.setLeft((left_cond103!=null?left_cond103.ret:null));
            					arg.setRight((in_list102!=null?in_list102.list:null));
            					exp.addExpression(arg);
            			}
            		}
            		;
            		

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "inCondition"

    public static class inCondition_expr_adds_return extends TreeRuleReturnScope {
        public List list;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "inCondition_expr_adds"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:332:1: inCondition_expr_adds[BindIndexHolder where] returns [List list] : ( expr_add[$where] )+ ;
    public final MySQLWalker.inCondition_expr_adds_return inCondition_expr_adds(BindIndexHolder where) throws RecognitionException {
        MySQLWalker.inCondition_expr_adds_return retval = new MySQLWalker.inCondition_expr_adds_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.expr_add_return expr_add104 = null;



        retval.list =new ArrayList();
        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:333:31: ( ( expr_add[$where] )+ )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:334:2: ( expr_add[$where] )+
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\tools\\antlr\\test\\MySQLWalker.g:334:2: ( expr_add[$where] )+
            int cnt26=0;
            loop26:
            do {
                int alt26=2;
                int LA26_0 = input.LA(1);

                if ( (LA26_0==COLUMN||LA26_0==QUTED_STR||LA26_0==COLUMNAST||LA26_0==CONSIST||(LA26_0>=NEGATIVE && LA26_0<=INTERVAL)||(LA26_0>=BITOR && LA26_0<=DIVIDE)||(LA26_0>=ID && LA26_0<=N)||LA26_0==NUMBER||LA26_0==118||(LA26_0>=121 && LA26_0<=122)||(LA26_0>=133 && LA26_0<=134)) ) {
                    alt26=1;
                }


                switch (alt26) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:334:3: expr_add[$where]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_expr_add_in_inCondition_expr_adds908);
            	    expr_add104=expr_add(where);

            	    state._fsp--;

            	    adaptor.addChild(root_0, expr_add104.getTree());

            	    		retval.list.add((expr_add104!=null?expr_add104.valueObjExpr:null));
            	    	

            	    }
            	    break;

            	default :
            	    if ( cnt26 >= 1 ) break loop26;
                        EarlyExitException eee =
                            new EarlyExitException(26, input);
                        throw eee;
                }
                cnt26++;
            } while (true);


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "inCondition_expr_adds"

    public static class expr_return extends TreeRuleReturnScope {
        public Object valueObj;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:341:1: expr[BindIndexHolder where] returns [Object valueObj] : ( expr_add[$where] | subquery[$where] ) ;
    public final MySQLWalker.expr_return expr(BindIndexHolder where) throws RecognitionException {
        MySQLWalker.expr_return retval = new MySQLWalker.expr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.expr_add_return expr_add105 = null;

        MySQLWalker.subquery_return subquery106 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:342:2: ( ( expr_add[$where] | subquery[$where] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:342:3: ( expr_add[$where] | subquery[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\tools\\antlr\\test\\MySQLWalker.g:342:3: ( expr_add[$where] | subquery[$where] )
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( (LA27_0==COLUMN||LA27_0==QUTED_STR||LA27_0==COLUMNAST||LA27_0==CONSIST||(LA27_0>=NEGATIVE && LA27_0<=INTERVAL)||(LA27_0>=BITOR && LA27_0<=DIVIDE)||(LA27_0>=ID && LA27_0<=N)||LA27_0==NUMBER||LA27_0==118||(LA27_0>=121 && LA27_0<=122)||(LA27_0>=133 && LA27_0<=134)) ) {
                alt27=1;
            }
            else if ( (LA27_0==SUBQUERY) ) {
                alt27=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 27, 0, input);

                throw nvae;
            }
            switch (alt27) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:342:4: expr_add[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr933);
                    expr_add105=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_0, expr_add105.getTree());
                    retval.valueObj =(expr_add105!=null?expr_add105.valueObjExpr:null);

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:343:3: subquery[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_subquery_in_expr940);
                    subquery106=subquery(where);

                    state._fsp--;

                    adaptor.addChild(root_0, subquery106.getTree());
                    retval.valueObj =(subquery106!=null?subquery106.subselect:null);

                    }
                    break;

            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr"

    public static class expr_add_return extends TreeRuleReturnScope {
        public Object valueObjExpr;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr_add"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:347:1: expr_add[BindIndexHolder where] returns [Object valueObjExpr] : ( ^( NEGATIVE s1= expr_add[$where] ) | ^( POSITIVE s1= expr_add[$where] ) | ^( PLUS s1= expr_add[$where] s2= expr_add[$where] ) | ^( MINUS s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITOR s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITAND s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITXOR s1= expr_add[$where] s2= expr_add[$where] ) | ^( SHIFTLEFT s1= expr_add[$where] s2= expr_add[$where] ) | ^( SHIFTRIGHT s1= expr_add[$where] s2= expr_add[$where] ) | ^( ASTERISK s1= expr_add[$where] s2= expr_add[$where] ) | ^( DIVIDE s1= expr_add[$where] s2= expr_add[$where] ) | N | NUMBER | boolean_literal | 'NULL' | 'ROWNUM' | '?' | ^( QUTED_STR quoted_string ) | ^( COLUMN identifier ( table_name )? ) | ^( COLUMNAST ASTERISK ) | ^( INTERVAL i_v= inner_value[$where] u= unit ) | ^( ID ( expr[$where] )* ) | ^( CONSIST ID ) );
    public final MySQLWalker.expr_add_return expr_add(BindIndexHolder where) throws RecognitionException {
        MySQLWalker.expr_add_return retval = new MySQLWalker.expr_add_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree NEGATIVE107=null;
        CommonTree POSITIVE108=null;
        CommonTree PLUS109=null;
        CommonTree MINUS110=null;
        CommonTree BITOR111=null;
        CommonTree BITAND112=null;
        CommonTree BITXOR113=null;
        CommonTree SHIFTLEFT114=null;
        CommonTree SHIFTRIGHT115=null;
        CommonTree ASTERISK116=null;
        CommonTree DIVIDE117=null;
        CommonTree N118=null;
        CommonTree NUMBER119=null;
        CommonTree string_literal121=null;
        CommonTree string_literal122=null;
        CommonTree char_literal123=null;
        CommonTree QUTED_STR124=null;
        CommonTree COLUMN126=null;
        CommonTree COLUMNAST129=null;
        CommonTree ASTERISK130=null;
        CommonTree INTERVAL131=null;
        CommonTree ID132=null;
        CommonTree CONSIST134=null;
        CommonTree ID135=null;
        MySQLWalker.expr_add_return s1 = null;

        MySQLWalker.expr_add_return s2 = null;

        MySQLWalker.inner_value_return i_v = null;

        MySQLWalker.unit_return u = null;

        MySQLWalker.boolean_literal_return boolean_literal120 = null;

        MySQLWalker.quoted_string_return quoted_string125 = null;

        MySQLWalker.identifier_return identifier127 = null;

        MySQLWalker.table_name_return table_name128 = null;

        MySQLWalker.expr_return expr133 = null;


        CommonTree NEGATIVE107_tree=null;
        CommonTree POSITIVE108_tree=null;
        CommonTree PLUS109_tree=null;
        CommonTree MINUS110_tree=null;
        CommonTree BITOR111_tree=null;
        CommonTree BITAND112_tree=null;
        CommonTree BITXOR113_tree=null;
        CommonTree SHIFTLEFT114_tree=null;
        CommonTree SHIFTRIGHT115_tree=null;
        CommonTree ASTERISK116_tree=null;
        CommonTree DIVIDE117_tree=null;
        CommonTree N118_tree=null;
        CommonTree NUMBER119_tree=null;
        CommonTree string_literal121_tree=null;
        CommonTree string_literal122_tree=null;
        CommonTree char_literal123_tree=null;
        CommonTree QUTED_STR124_tree=null;
        CommonTree COLUMN126_tree=null;
        CommonTree COLUMNAST129_tree=null;
        CommonTree ASTERISK130_tree=null;
        CommonTree INTERVAL131_tree=null;
        CommonTree ID132_tree=null;
        CommonTree CONSIST134_tree=null;
        CommonTree ID135_tree=null;


        List<Object> list=new ArrayList<Object>();

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:351:2: ( ^( NEGATIVE s1= expr_add[$where] ) | ^( POSITIVE s1= expr_add[$where] ) | ^( PLUS s1= expr_add[$where] s2= expr_add[$where] ) | ^( MINUS s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITOR s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITAND s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITXOR s1= expr_add[$where] s2= expr_add[$where] ) | ^( SHIFTLEFT s1= expr_add[$where] s2= expr_add[$where] ) | ^( SHIFTRIGHT s1= expr_add[$where] s2= expr_add[$where] ) | ^( ASTERISK s1= expr_add[$where] s2= expr_add[$where] ) | ^( DIVIDE s1= expr_add[$where] s2= expr_add[$where] ) | N | NUMBER | boolean_literal | 'NULL' | 'ROWNUM' | '?' | ^( QUTED_STR quoted_string ) | ^( COLUMN identifier ( table_name )? ) | ^( COLUMNAST ASTERISK ) | ^( INTERVAL i_v= inner_value[$where] u= unit ) | ^( ID ( expr[$where] )* ) | ^( CONSIST ID ) )
            int alt30=23;
            switch ( input.LA(1) ) {
            case NEGATIVE:
                {
                alt30=1;
                }
                break;
            case POSITIVE:
                {
                alt30=2;
                }
                break;
            case PLUS:
                {
                alt30=3;
                }
                break;
            case MINUS:
                {
                alt30=4;
                }
                break;
            case BITOR:
                {
                alt30=5;
                }
                break;
            case BITAND:
                {
                alt30=6;
                }
                break;
            case BITXOR:
                {
                alt30=7;
                }
                break;
            case SHIFTLEFT:
                {
                alt30=8;
                }
                break;
            case SHIFTRIGHT:
                {
                alt30=9;
                }
                break;
            case ASTERISK:
                {
                alt30=10;
                }
                break;
            case DIVIDE:
                {
                alt30=11;
                }
                break;
            case N:
                {
                alt30=12;
                }
                break;
            case NUMBER:
                {
                alt30=13;
                }
                break;
            case 133:
            case 134:
                {
                alt30=14;
                }
                break;
            case 118:
                {
                alt30=15;
                }
                break;
            case 121:
                {
                alt30=16;
                }
                break;
            case 122:
                {
                alt30=17;
                }
                break;
            case QUTED_STR:
                {
                alt30=18;
                }
                break;
            case COLUMN:
                {
                alt30=19;
                }
                break;
            case COLUMNAST:
                {
                alt30=20;
                }
                break;
            case INTERVAL:
                {
                alt30=21;
                }
                break;
            case ID:
                {
                alt30=22;
                }
                break;
            case CONSIST:
                {
                alt30=23;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 30, 0, input);

                throw nvae;
            }

            switch (alt30) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:351:3: ^( NEGATIVE s1= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NEGATIVE107=(CommonTree)match(input,NEGATIVE,FOLLOW_NEGATIVE_in_expr_add968); 
                    NEGATIVE107_tree = (CommonTree)adaptor.dupNode(NEGATIVE107);

                    root_1 = (CommonTree)adaptor.becomeRoot(NEGATIVE107_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add972);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  retval.valueObjExpr =((BigDecimal)(s1!=null?s1.valueObjExpr:null)).negate();
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:354:3: ^( POSITIVE s1= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    POSITIVE108=(CommonTree)match(input,POSITIVE,FOLLOW_POSITIVE_in_expr_add981); 
                    POSITIVE108_tree = (CommonTree)adaptor.dupNode(POSITIVE108);

                    root_1 = (CommonTree)adaptor.becomeRoot(POSITIVE108_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add985);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  retval.valueObjExpr =(s1!=null?s1.valueObjExpr:null);
                    	

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:357:3: ^( PLUS s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    PLUS109=(CommonTree)match(input,PLUS,FOLLOW_PLUS_in_expr_add994); 
                    PLUS109_tree = (CommonTree)adaptor.dupNode(PLUS109);

                    root_1 = (CommonTree)adaptor.becomeRoot(PLUS109_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add998);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1003);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new Add();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:364:3: ^( MINUS s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    MINUS110=(CommonTree)match(input,MINUS,FOLLOW_MINUS_in_expr_add1012); 
                    MINUS110_tree = (CommonTree)adaptor.dupNode(MINUS110);

                    root_1 = (CommonTree)adaptor.becomeRoot(MINUS110_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1016);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1021);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new Subtract();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:371:3: ^( BITOR s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BITOR111=(CommonTree)match(input,BITOR,FOLLOW_BITOR_in_expr_add1029); 
                    BITOR111_tree = (CommonTree)adaptor.dupNode(BITOR111);

                    root_1 = (CommonTree)adaptor.becomeRoot(BITOR111_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1033);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1038);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new BitOr();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 6 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:378:3: ^( BITAND s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BITAND112=(CommonTree)match(input,BITAND,FOLLOW_BITAND_in_expr_add1046); 
                    BITAND112_tree = (CommonTree)adaptor.dupNode(BITAND112);

                    root_1 = (CommonTree)adaptor.becomeRoot(BITAND112_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1050);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1055);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new BitAnd();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 7 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:385:3: ^( BITXOR s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BITXOR113=(CommonTree)match(input,BITXOR,FOLLOW_BITXOR_in_expr_add1063); 
                    BITXOR113_tree = (CommonTree)adaptor.dupNode(BITXOR113);

                    root_1 = (CommonTree)adaptor.becomeRoot(BITXOR113_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1067);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1072);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new BitXOr();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 8 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:392:3: ^( SHIFTLEFT s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    SHIFTLEFT114=(CommonTree)match(input,SHIFTLEFT,FOLLOW_SHIFTLEFT_in_expr_add1080); 
                    SHIFTLEFT114_tree = (CommonTree)adaptor.dupNode(SHIFTLEFT114);

                    root_1 = (CommonTree)adaptor.becomeRoot(SHIFTLEFT114_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1084);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1089);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new ShiftLeft();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 9 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:399:3: ^( SHIFTRIGHT s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    SHIFTRIGHT115=(CommonTree)match(input,SHIFTRIGHT,FOLLOW_SHIFTRIGHT_in_expr_add1097); 
                    SHIFTRIGHT115_tree = (CommonTree)adaptor.dupNode(SHIFTRIGHT115);

                    root_1 = (CommonTree)adaptor.becomeRoot(SHIFTRIGHT115_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1101);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1106);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new ShiftRight();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 10 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:406:3: ^( ASTERISK s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ASTERISK116=(CommonTree)match(input,ASTERISK,FOLLOW_ASTERISK_in_expr_add1114); 
                    ASTERISK116_tree = (CommonTree)adaptor.dupNode(ASTERISK116);

                    root_1 = (CommonTree)adaptor.becomeRoot(ASTERISK116_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1118);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1123);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new Multiply();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 11 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:413:3: ^( DIVIDE s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    DIVIDE117=(CommonTree)match(input,DIVIDE,FOLLOW_DIVIDE_in_expr_add1131); 
                    DIVIDE117_tree = (CommonTree)adaptor.dupNode(DIVIDE117);

                    root_1 = (CommonTree)adaptor.becomeRoot(DIVIDE117_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1135);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add1140);
                    s2=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s2.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=new Divide();
                    	  list.add((s1!=null?s1.valueObjExpr:null));
                    	  list.add((s2!=null?s2.valueObjExpr:null));
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 12 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:420:3: N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    N118=(CommonTree)match(input,N,FOLLOW_N_in_expr_add1147); 
                    N118_tree = (CommonTree)adaptor.dupNode(N118);

                    adaptor.addChild(root_0, N118_tree);


                    	  retval.valueObjExpr =new BigDecimal((N118!=null?N118.getText():null));
                    	

                    }
                    break;
                case 13 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:424:3: NUMBER
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    NUMBER119=(CommonTree)match(input,NUMBER,FOLLOW_NUMBER_in_expr_add1155); 
                    NUMBER119_tree = (CommonTree)adaptor.dupNode(NUMBER119);

                    adaptor.addChild(root_0, NUMBER119_tree);


                    	  retval.valueObjExpr =new BigDecimal((NUMBER119!=null?NUMBER119.getText():null));
                    	

                    }
                    break;
                case 14 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:428:3: boolean_literal
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_boolean_literal_in_expr_add1162);
                    boolean_literal120=boolean_literal();

                    state._fsp--;

                    adaptor.addChild(root_0, boolean_literal120.getTree());

                    }
                    break;
                case 15 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:429:3: 'NULL'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    string_literal121=(CommonTree)match(input,118,FOLLOW_118_in_expr_add1166); 
                    string_literal121_tree = (CommonTree)adaptor.dupNode(string_literal121);

                    adaptor.addChild(root_0, string_literal121_tree);


                    	  retval.valueObjExpr =null;
                    	

                    }
                    break;
                case 16 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:433:3: 'ROWNUM'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    string_literal122=(CommonTree)match(input,121,FOLLOW_121_in_expr_add1174); 
                    string_literal122_tree = (CommonTree)adaptor.dupNode(string_literal122);

                    adaptor.addChild(root_0, string_literal122_tree);


                    }
                    break;
                case 17 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:434:3: '?'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    char_literal123=(CommonTree)match(input,122,FOLLOW_122_in_expr_add1178); 
                    char_literal123_tree = (CommonTree)adaptor.dupNode(char_literal123);

                    adaptor.addChild(root_0, char_literal123_tree);


                    	  BindVar val=new BindVar(where.selfAddAndGet());
                    	  retval.valueObjExpr =val;
                    	

                    }
                    break;
                case 18 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:438:3: ^( QUTED_STR quoted_string )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    QUTED_STR124=(CommonTree)match(input,QUTED_STR,FOLLOW_QUTED_STR_in_expr_add1184); 
                    QUTED_STR124_tree = (CommonTree)adaptor.dupNode(QUTED_STR124);

                    root_1 = (CommonTree)adaptor.becomeRoot(QUTED_STR124_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_quoted_string_in_expr_add1186);
                    quoted_string125=quoted_string();

                    state._fsp--;

                    adaptor.addChild(root_1, quoted_string125.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  retval.valueObjExpr =(quoted_string125!=null?quoted_string125.aText:null);
                    	

                    }
                    break;
                case 19 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:441:3: ^( COLUMN identifier ( table_name )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    COLUMN126=(CommonTree)match(input,COLUMN,FOLLOW_COLUMN_in_expr_add1193); 
                    COLUMN126_tree = (CommonTree)adaptor.dupNode(COLUMN126);

                    root_1 = (CommonTree)adaptor.becomeRoot(COLUMN126_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_identifier_in_expr_add1195);
                    identifier127=identifier();

                    state._fsp--;

                    adaptor.addChild(root_1, identifier127.getTree());
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:441:23: ( table_name )?
                    int alt28=2;
                    int LA28_0 = input.LA(1);

                    if ( (LA28_0==ID) ) {
                        alt28=1;
                    }
                    switch (alt28) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:441:23: table_name
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_table_name_in_expr_add1197);
                            table_name128=table_name();

                            state._fsp--;

                            adaptor.addChild(root_1, table_name128.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Column col=new ColumnImp((table_name128!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(table_name128.start),
                      input.getTreeAdaptor().getTokenStopIndex(table_name128.start))):null),(identifier127!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(identifier127.start),
                      input.getTreeAdaptor().getTokenStopIndex(identifier127.start))):null),null);
                    	  retval.valueObjExpr =col;
                    	

                    }
                    break;
                case 20 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:446:3: ^( COLUMNAST ASTERISK )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    COLUMNAST129=(CommonTree)match(input,COLUMNAST,FOLLOW_COLUMNAST_in_expr_add1207); 
                    COLUMNAST129_tree = (CommonTree)adaptor.dupNode(COLUMNAST129);

                    root_1 = (CommonTree)adaptor.becomeRoot(COLUMNAST129_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    ASTERISK130=(CommonTree)match(input,ASTERISK,FOLLOW_ASTERISK_in_expr_add1209); 
                    ASTERISK130_tree = (CommonTree)adaptor.dupNode(ASTERISK130);

                    adaptor.addChild(root_1, ASTERISK130_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Column col=new ColumnImp(null,(ASTERISK130!=null?ASTERISK130.getText():null),null);
                    	  retval.valueObjExpr =col;
                    	

                    }
                    break;
                case 21 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:455:3: ^( INTERVAL i_v= inner_value[$where] u= unit )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    INTERVAL131=(CommonTree)match(input,INTERVAL,FOLLOW_INTERVAL_in_expr_add1226); 
                    INTERVAL131_tree = (CommonTree)adaptor.dupNode(INTERVAL131);

                    root_1 = (CommonTree)adaptor.becomeRoot(INTERVAL131_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_inner_value_in_expr_add1230);
                    i_v=inner_value(where);

                    state._fsp--;

                    adaptor.addChild(root_1, i_v.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_unit_in_expr_add1235);
                    u=unit();

                    state._fsp--;

                    adaptor.addChild(root_1, u.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function intervalObj=new Interval();	
                    	  list.add((i_v!=null?i_v.valueObj:null));
                    	  list.add((u!=null?u.unitObj:null));
                    	  intervalObj.setValue(list);
                    	  retval.valueObjExpr =intervalObj;
                    	

                    }
                    break;
                case 22 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:463:3: ^( ID ( expr[$where] )* )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ID132=(CommonTree)match(input,ID,FOLLOW_ID_in_expr_add1244); 
                    ID132_tree = (CommonTree)adaptor.dupNode(ID132);

                    root_1 = (CommonTree)adaptor.becomeRoot(ID132_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        // D:\\tools\\antlr\\test\\MySQLWalker.g:463:8: ( expr[$where] )*
                        loop29:
                        do {
                            int alt29=2;
                            int LA29_0 = input.LA(1);

                            if ( ((LA29_0>=SUBQUERY && LA29_0<=COLUMN)||LA29_0==QUTED_STR||LA29_0==COLUMNAST||LA29_0==CONSIST||(LA29_0>=NEGATIVE && LA29_0<=INTERVAL)||(LA29_0>=BITOR && LA29_0<=DIVIDE)||(LA29_0>=ID && LA29_0<=N)||LA29_0==NUMBER||LA29_0==118||(LA29_0>=121 && LA29_0<=122)||(LA29_0>=133 && LA29_0<=134)) ) {
                                alt29=1;
                            }


                            switch (alt29) {
                        	case 1 :
                        	    // D:\\tools\\antlr\\test\\MySQLWalker.g:463:9: expr[$where]
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    pushFollow(FOLLOW_expr_in_expr_add1247);
                        	    expr133=expr(where);

                        	    state._fsp--;

                        	    adaptor.addChild(root_1, expr133.getTree());
                        	    list.add((expr133!=null?expr133.valueObj:null));

                        	    }
                        	    break;

                        	default :
                        	    break loop29;
                            }
                        } while (true);


                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=funcreg.get(((ID132!=null?ID132.getText():null)).toUpperCase());
                    	  func.setValue(list);
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 23 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:469:3: ^( CONSIST ID )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CONSIST134=(CommonTree)match(input,CONSIST,FOLLOW_CONSIST_in_expr_add1260); 
                    CONSIST134_tree = (CommonTree)adaptor.dupNode(CONSIST134);

                    root_1 = (CommonTree)adaptor.becomeRoot(CONSIST134_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    ID135=(CommonTree)match(input,ID,FOLLOW_ID_in_expr_add1262); 
                    ID135_tree = (CommonTree)adaptor.dupNode(ID135);

                    adaptor.addChild(root_1, ID135_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=consistStr.get(((ID135!=null?ID135.getText():null)).toUpperCase());
                    	  retval.valueObjExpr =func;
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr_add"

    public static class inner_value_return extends TreeRuleReturnScope {
        public Object valueObj;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "inner_value"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:515:1: inner_value[BindIndexHolder where] returns [Object valueObj] : ( '?' | N | MINUS N );
    public final MySQLWalker.inner_value_return inner_value(BindIndexHolder where) throws RecognitionException {
        MySQLWalker.inner_value_return retval = new MySQLWalker.inner_value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree char_literal136=null;
        CommonTree N137=null;
        CommonTree MINUS138=null;
        CommonTree N139=null;

        CommonTree char_literal136_tree=null;
        CommonTree N137_tree=null;
        CommonTree MINUS138_tree=null;
        CommonTree N139_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:516:2: ( '?' | N | MINUS N )
            int alt31=3;
            switch ( input.LA(1) ) {
            case 122:
                {
                alt31=1;
                }
                break;
            case N:
                {
                alt31=2;
                }
                break;
            case MINUS:
                {
                alt31=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 31, 0, input);

                throw nvae;
            }

            switch (alt31) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:516:3: '?'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    char_literal136=(CommonTree)match(input,122,FOLLOW_122_in_inner_value1320); 
                    char_literal136_tree = (CommonTree)adaptor.dupNode(char_literal136);

                    adaptor.addChild(root_0, char_literal136_tree);


                    	  BindVar val=new BindVar(where.selfAddAndGet());
                    	  retval.valueObj =val;
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:520:3: N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    N137=(CommonTree)match(input,N,FOLLOW_N_in_inner_value1325); 
                    N137_tree = (CommonTree)adaptor.dupNode(N137);

                    adaptor.addChild(root_0, N137_tree);


                    	  retval.valueObj =(N137!=null?N137.getText():null);
                    	

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:523:3: MINUS N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    MINUS138=(CommonTree)match(input,MINUS,FOLLOW_MINUS_in_inner_value1330); 
                    MINUS138_tree = (CommonTree)adaptor.dupNode(MINUS138);

                    adaptor.addChild(root_0, MINUS138_tree);

                    _last = (CommonTree)input.LT(1);
                    N139=(CommonTree)match(input,N,FOLLOW_N_in_inner_value1332); 
                    N139_tree = (CommonTree)adaptor.dupNode(N139);

                    adaptor.addChild(root_0, N139_tree);


                    	  StringBuilder sb=new StringBuilder();
                    	  sb.append((MINUS138!=null?MINUS138.getText():null));
                    	  sb.append((N139!=null?N139.getText():null));
                    	  retval.valueObj =sb;
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "inner_value"

    public static class unit_return extends TreeRuleReturnScope {
        public Object unitObj;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unit"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:531:1: unit returns [Object unitObj] : ^( UNIT ID ) ;
    public final MySQLWalker.unit_return unit() throws RecognitionException {
        MySQLWalker.unit_return retval = new MySQLWalker.unit_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree UNIT140=null;
        CommonTree ID141=null;

        CommonTree UNIT140_tree=null;
        CommonTree ID141_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:532:9: ( ^( UNIT ID ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:532:10: ^( UNIT ID )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            UNIT140=(CommonTree)match(input,UNIT,FOLLOW_UNIT_in_unit1357); 
            UNIT140_tree = (CommonTree)adaptor.dupNode(UNIT140);

            root_1 = (CommonTree)adaptor.becomeRoot(UNIT140_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            ID141=(CommonTree)match(input,ID,FOLLOW_ID_in_unit1359); 
            ID141_tree = (CommonTree)adaptor.dupNode(ID141);

            adaptor.addChild(root_1, ID141_tree);


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


              	retval.unitObj =consistStr.get(((ID141!=null?ID141.getText():null)).toUpperCase());
                    

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "unit"

    public static class value_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "value"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:538:1: value : ( N | NUMBER | '?' );
    public final MySQLWalker.value_return value() throws RecognitionException {
        MySQLWalker.value_return retval = new MySQLWalker.value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree set142=null;

        CommonTree set142_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:538:7: ( N | NUMBER | '?' )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            set142=(CommonTree)input.LT(1);
            if ( input.LA(1)==N||input.LA(1)==NUMBER||input.LA(1)==122 ) {
                input.consume();

                set142_tree = (CommonTree)adaptor.dupNode(set142);

                adaptor.addChild(root_0, set142_tree);

                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "value"

    public static class boolean_literal_return extends TreeRuleReturnScope {
        public Object valueObj;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "boolean_literal"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:544:1: boolean_literal returns [Object valueObj] : (s1= 'TRUE' | s1= 'FALSE' );
    public final MySQLWalker.boolean_literal_return boolean_literal() throws RecognitionException {
        MySQLWalker.boolean_literal_return retval = new MySQLWalker.boolean_literal_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree s1=null;

        CommonTree s1_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:545:2: (s1= 'TRUE' | s1= 'FALSE' )
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==133) ) {
                alt32=1;
            }
            else if ( (LA32_0==134) ) {
                alt32=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 32, 0, input);

                throw nvae;
            }
            switch (alt32) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:545:3: s1= 'TRUE'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    s1=(CommonTree)match(input,133,FOLLOW_133_in_boolean_literal1406); 
                    s1_tree = (CommonTree)adaptor.dupNode(s1);

                    adaptor.addChild(root_0, s1_tree);

                    retval.valueObj =Boolean.parseBoolean((s1!=null?s1.getText():null));

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:546:4: s1= 'FALSE'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    s1=(CommonTree)match(input,134,FOLLOW_134_in_boolean_literal1415); 
                    s1_tree = (CommonTree)adaptor.dupNode(s1);

                    adaptor.addChild(root_0, s1_tree);

                    retval.valueObj =Boolean.parseBoolean((s1!=null?s1.getText():null));

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "boolean_literal"

    public static class select_list_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "select_list"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:549:1: select_list[Select select] : ^( SELECT_LIST dis_col[$select] ) ;
    public final MySQLWalker.select_list_return select_list(Select select) throws RecognitionException {
        MySQLWalker.select_list_return retval = new MySQLWalker.select_list_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SELECT_LIST143=null;
        MySQLWalker.dis_col_return dis_col144 = null;


        CommonTree SELECT_LIST143_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:550:2: ( ^( SELECT_LIST dis_col[$select] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:550:3: ^( SELECT_LIST dis_col[$select] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SELECT_LIST143=(CommonTree)match(input,SELECT_LIST,FOLLOW_SELECT_LIST_in_select_list1429); 
            SELECT_LIST143_tree = (CommonTree)adaptor.dupNode(SELECT_LIST143);

            root_1 = (CommonTree)adaptor.becomeRoot(SELECT_LIST143_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_dis_col_in_select_list1431);
            dis_col144=dis_col(select);

            state._fsp--;

            adaptor.addChild(root_1, dis_col144.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "select_list"

    public static class dis_col_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "dis_col"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:553:1: dis_col[Select select] : ( ( displayed_column[$select] )+ | distinct_col[$select] );
    public final MySQLWalker.dis_col_return dis_col(Select select) throws RecognitionException {
        MySQLWalker.dis_col_return retval = new MySQLWalker.dis_col_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.displayed_column_return displayed_column145 = null;

        MySQLWalker.distinct_col_return distinct_col146 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:554:2: ( ( displayed_column[$select] )+ | distinct_col[$select] )
            int alt34=2;
            int LA34_0 = input.LA(1);

            if ( (LA34_0==COLUMN||LA34_0==CONSIST||LA34_0==ID) ) {
                alt34=1;
            }
            else if ( (LA34_0==DISTINCT) ) {
                alt34=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 34, 0, input);

                throw nvae;
            }
            switch (alt34) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:554:3: ( displayed_column[$select] )+
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    // D:\\tools\\antlr\\test\\MySQLWalker.g:554:3: ( displayed_column[$select] )+
                    int cnt33=0;
                    loop33:
                    do {
                        int alt33=2;
                        int LA33_0 = input.LA(1);

                        if ( (LA33_0==COLUMN||LA33_0==CONSIST||LA33_0==ID) ) {
                            alt33=1;
                        }


                        switch (alt33) {
                    	case 1 :
                    	    // D:\\tools\\antlr\\test\\MySQLWalker.g:554:4: displayed_column[$select]
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    pushFollow(FOLLOW_displayed_column_in_dis_col1446);
                    	    displayed_column145=displayed_column(select);

                    	    state._fsp--;

                    	    adaptor.addChild(root_0, displayed_column145.getTree());
                    	    select.addColumn((displayed_column145!=null?displayed_column145.column:null));

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt33 >= 1 ) break loop33;
                                EarlyExitException eee =
                                    new EarlyExitException(33, input);
                                throw eee;
                        }
                        cnt33++;
                    } while (true);


                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:555:4: distinct_col[$select]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_distinct_col_in_dis_col1455);
                    distinct_col146=distinct_col(select);

                    state._fsp--;

                    adaptor.addChild(root_0, distinct_col146.getTree());

                              //duplicated for resultset compact
                    	  select.setDistinct((distinct_col146!=null?distinct_col146.dis:null));
                              //the real version to use
                    	  FunctionColumn funcolumn=new FunctionColumn();
                    	  funcolumn.setFunction((distinct_col146!=null?distinct_col146.dis:null));
                    	  select.addColumn(funcolumn);
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "dis_col"

    public static class fromClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fromClause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:566:1: fromClause[Select select] : ^( TABLENAMES ( table[$select] )+ ) ;
    public final MySQLWalker.fromClause_return fromClause(Select select) throws RecognitionException {
        MySQLWalker.fromClause_return retval = new MySQLWalker.fromClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree TABLENAMES147=null;
        MySQLWalker.table_return table148 = null;


        CommonTree TABLENAMES147_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:567:2: ( ^( TABLENAMES ( table[$select] )+ ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:567:3: ^( TABLENAMES ( table[$select] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            TABLENAMES147=(CommonTree)match(input,TABLENAMES,FOLLOW_TABLENAMES_in_fromClause1473); 
            TABLENAMES147_tree = (CommonTree)adaptor.dupNode(TABLENAMES147);

            root_1 = (CommonTree)adaptor.becomeRoot(TABLENAMES147_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\tools\\antlr\\test\\MySQLWalker.g:567:16: ( table[$select] )+
            int cnt35=0;
            loop35:
            do {
                int alt35=2;
                int LA35_0 = input.LA(1);

                if ( (LA35_0==TABLENAME) ) {
                    alt35=1;
                }


                switch (alt35) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:567:16: table[$select]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_table_in_fromClause1475);
            	    table148=table(select);

            	    state._fsp--;

            	    adaptor.addChild(root_1, table148.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt35 >= 1 ) break loop35;
                        EarlyExitException eee =
                            new EarlyExitException(35, input);
                        throw eee;
                }
                cnt35++;
            } while (true);


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "fromClause"

    public static class join_clause_return extends TreeRuleReturnScope {
        public JoinClause joinClause;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "join_clause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:570:1: join_clause[DMLCommon common] returns [JoinClause joinClause] : ^( JOIN table_name ( alias )? temp1= join_column temp2= join_column ( join_type )? ) ;
    public final MySQLWalker.join_clause_return join_clause(DMLCommon common) throws RecognitionException {
        MySQLWalker.join_clause_return retval = new MySQLWalker.join_clause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree JOIN149=null;
        MySQLWalker.join_column_return temp1 = null;

        MySQLWalker.join_column_return temp2 = null;

        MySQLWalker.table_name_return table_name150 = null;

        MySQLWalker.alias_return alias151 = null;

        MySQLWalker.join_type_return join_type152 = null;


        CommonTree JOIN149_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:571:9: ( ^( JOIN table_name ( alias )? temp1= join_column temp2= join_column ( join_type )? ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:571:10: ^( JOIN table_name ( alias )? temp1= join_column temp2= join_column ( join_type )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            JOIN149=(CommonTree)match(input,JOIN,FOLLOW_JOIN_in_join_clause1502); 
            JOIN149_tree = (CommonTree)adaptor.dupNode(JOIN149);

            root_1 = (CommonTree)adaptor.becomeRoot(JOIN149_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_table_name_in_join_clause1504);
            table_name150=table_name();

            state._fsp--;

            adaptor.addChild(root_1, table_name150.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:571:28: ( alias )?
            int alt36=2;
            int LA36_0 = input.LA(1);

            if ( (LA36_0==AS) ) {
                alt36=1;
            }
            switch (alt36) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:571:28: alias
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_alias_in_join_clause1506);
                    alias151=alias();

                    state._fsp--;

                    adaptor.addChild(root_1, alias151.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_join_column_in_join_clause1511);
            temp1=join_column();

            state._fsp--;

            adaptor.addChild(root_1, temp1.getTree());
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_join_column_in_join_clause1515);
            temp2=join_column();

            state._fsp--;

            adaptor.addChild(root_1, temp2.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:571:71: ( join_type )?
            int alt37=2;
            int LA37_0 = input.LA(1);

            if ( ((LA37_0>=INNER && LA37_0<=FULL)||(LA37_0>=UNION && LA37_0<=CROSS)) ) {
                alt37=1;
            }
            switch (alt37) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:571:71: join_type
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_join_type_in_join_clause1517);
                    join_type152=join_type();

                    state._fsp--;

                    adaptor.addChild(root_1, join_type152.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


                      retval.joinClause = new JoinClause();
                      TableNameImp tableName = new TableNameImp();
                      tableName.setTablename((table_name150!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(table_name150.start),
              input.getTreeAdaptor().getTokenStopIndex(table_name150.start))):null));
                      tableName.setAlias((alias151!=null?alias151.aliText:null));
                      retval.joinClause.setTableName(tableName);
                      retval.joinClause.setLeftCondition((temp1!=null?temp1.col:null));
                      retval.joinClause.setRightCondition((temp2!=null?temp2.col:null));
                      retval.joinClause.setJoinType((join_type152!=null?join_type152.joinType:null));
                    

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "join_clause"

    public static class join_column_return extends TreeRuleReturnScope {
        public Column col;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "join_column"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:584:1: join_column returns [Column col] : ^( COLUMN identifier ( table_name )? ) ;
    public final MySQLWalker.join_column_return join_column() throws RecognitionException {
        MySQLWalker.join_column_return retval = new MySQLWalker.join_column_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree COLUMN153=null;
        MySQLWalker.identifier_return identifier154 = null;

        MySQLWalker.table_name_return table_name155 = null;


        CommonTree COLUMN153_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:585:9: ( ^( COLUMN identifier ( table_name )? ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:585:10: ^( COLUMN identifier ( table_name )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            COLUMN153=(CommonTree)match(input,COLUMN,FOLLOW_COLUMN_in_join_column1560); 
            COLUMN153_tree = (CommonTree)adaptor.dupNode(COLUMN153);

            root_1 = (CommonTree)adaptor.becomeRoot(COLUMN153_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_join_column1562);
            identifier154=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier154.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:585:30: ( table_name )?
            int alt38=2;
            int LA38_0 = input.LA(1);

            if ( (LA38_0==ID) ) {
                alt38=1;
            }
            switch (alt38) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:585:30: table_name
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_table_name_in_join_column1564);
                    table_name155=table_name();

                    state._fsp--;

                    adaptor.addChild(root_1, table_name155.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


                       retval.col =new ColumnImp((table_name155!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(table_name155.start),
              input.getTreeAdaptor().getTokenStopIndex(table_name155.start))):null),(identifier154!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier154.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier154.start))):null),null);
                       
                    

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "join_column"

    public static class join_type_return extends TreeRuleReturnScope {
        public JOIN_TYPE joinType;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "join_type"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:592:1: join_type returns [JOIN_TYPE joinType] : ( INNER | LEFT ( outer )? | RIGHT ( outer )? | FULL ( outer )? | UNION | CROSS );
    public final MySQLWalker.join_type_return join_type() throws RecognitionException {
        MySQLWalker.join_type_return retval = new MySQLWalker.join_type_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree INNER156=null;
        CommonTree LEFT157=null;
        CommonTree RIGHT159=null;
        CommonTree FULL161=null;
        CommonTree UNION163=null;
        CommonTree CROSS164=null;
        MySQLWalker.outer_return outer158 = null;

        MySQLWalker.outer_return outer160 = null;

        MySQLWalker.outer_return outer162 = null;


        CommonTree INNER156_tree=null;
        CommonTree LEFT157_tree=null;
        CommonTree RIGHT159_tree=null;
        CommonTree FULL161_tree=null;
        CommonTree UNION163_tree=null;
        CommonTree CROSS164_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:593:2: ( INNER | LEFT ( outer )? | RIGHT ( outer )? | FULL ( outer )? | UNION | CROSS )
            int alt42=6;
            switch ( input.LA(1) ) {
            case INNER:
                {
                alt42=1;
                }
                break;
            case LEFT:
                {
                alt42=2;
                }
                break;
            case RIGHT:
                {
                alt42=3;
                }
                break;
            case FULL:
                {
                alt42=4;
                }
                break;
            case UNION:
                {
                alt42=5;
                }
                break;
            case CROSS:
                {
                alt42=6;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 42, 0, input);

                throw nvae;
            }

            switch (alt42) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:593:3: INNER
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    INNER156=(CommonTree)match(input,INNER,FOLLOW_INNER_in_join_type1605); 
                    INNER156_tree = (CommonTree)adaptor.dupNode(INNER156);

                    adaptor.addChild(root_0, INNER156_tree);


                    		retval.joinType = JOIN_TYPE.INNER;
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:597:3: LEFT ( outer )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    LEFT157=(CommonTree)match(input,LEFT,FOLLOW_LEFT_in_join_type1613); 
                    LEFT157_tree = (CommonTree)adaptor.dupNode(LEFT157);

                    adaptor.addChild(root_0, LEFT157_tree);

                    // D:\\tools\\antlr\\test\\MySQLWalker.g:597:8: ( outer )?
                    int alt39=2;
                    int LA39_0 = input.LA(1);

                    if ( (LA39_0==OUTER) ) {
                        alt39=1;
                    }
                    switch (alt39) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:597:8: outer
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_outer_in_join_type1615);
                            outer158=outer();

                            state._fsp--;

                            adaptor.addChild(root_0, outer158.getTree());

                            }
                            break;

                    }


                    		boolean outter = (outer158!=null?outer158.outter:false);
                    		if(outter){
                    			retval.joinType = JOIN_TYPE.LEFT_OUTER;
                    		}else{
                    			retval.joinType = JOIN_TYPE.LEFT;
                    		}
                    	

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:606:3: RIGHT ( outer )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    RIGHT159=(CommonTree)match(input,RIGHT,FOLLOW_RIGHT_in_join_type1623); 
                    RIGHT159_tree = (CommonTree)adaptor.dupNode(RIGHT159);

                    adaptor.addChild(root_0, RIGHT159_tree);

                    // D:\\tools\\antlr\\test\\MySQLWalker.g:606:9: ( outer )?
                    int alt40=2;
                    int LA40_0 = input.LA(1);

                    if ( (LA40_0==OUTER) ) {
                        alt40=1;
                    }
                    switch (alt40) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:606:9: outer
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_outer_in_join_type1625);
                            outer160=outer();

                            state._fsp--;

                            adaptor.addChild(root_0, outer160.getTree());

                            }
                            break;

                    }


                    		boolean outter = (outer160!=null?outer160.outter:false);
                    		if(outter){
                    			retval.joinType = JOIN_TYPE.RIGHT_OUTER;
                    		}else{
                    			retval.joinType = JOIN_TYPE.RIGHT;
                    		}
                    	

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:615:3: FULL ( outer )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    FULL161=(CommonTree)match(input,FULL,FOLLOW_FULL_in_join_type1633); 
                    FULL161_tree = (CommonTree)adaptor.dupNode(FULL161);

                    adaptor.addChild(root_0, FULL161_tree);

                    // D:\\tools\\antlr\\test\\MySQLWalker.g:615:8: ( outer )?
                    int alt41=2;
                    int LA41_0 = input.LA(1);

                    if ( (LA41_0==OUTER) ) {
                        alt41=1;
                    }
                    switch (alt41) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:615:8: outer
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_outer_in_join_type1635);
                            outer162=outer();

                            state._fsp--;

                            adaptor.addChild(root_0, outer162.getTree());

                            }
                            break;

                    }


                    		boolean outter = (outer162!=null?outer162.outter:false);
                    		if(outter){
                    			retval.joinType = JOIN_TYPE.FULL_OUTER;
                    		}else{
                    			retval.joinType = JOIN_TYPE.FULL;
                    		}
                    	

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:624:3: UNION
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    UNION163=(CommonTree)match(input,UNION,FOLLOW_UNION_in_join_type1644); 
                    UNION163_tree = (CommonTree)adaptor.dupNode(UNION163);

                    adaptor.addChild(root_0, UNION163_tree);


                    		retval.joinType = JOIN_TYPE.UNION;
                    	

                    }
                    break;
                case 6 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:628:3: CROSS
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    CROSS164=(CommonTree)match(input,CROSS,FOLLOW_CROSS_in_join_type1652); 
                    CROSS164_tree = (CommonTree)adaptor.dupNode(CROSS164);

                    adaptor.addChild(root_0, CROSS164_tree);


                    		retval.joinType = JOIN_TYPE.CROSS;
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "join_type"

    public static class outer_return extends TreeRuleReturnScope {
        public boolean outter;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "outer"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:634:1: outer returns [boolean outter] : OUTER ;
    public final MySQLWalker.outer_return outer() throws RecognitionException {
        MySQLWalker.outer_return retval = new MySQLWalker.outer_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree OUTER165=null;

        CommonTree OUTER165_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:635:2: ( OUTER )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:635:3: OUTER
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            OUTER165=(CommonTree)match(input,OUTER,FOLLOW_OUTER_in_outer1671); 
            OUTER165_tree = (CommonTree)adaptor.dupNode(OUTER165);

            adaptor.addChild(root_0, OUTER165_tree);


            		retval.outter = true;
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "outer"

    public static class table_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "table"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:641:1: table[DMLCommon common] : ^( TABLENAME table_spec[$common] ( join_clause[$common] )? ) ;
    public final MySQLWalker.table_return table(DMLCommon common) throws RecognitionException {
        MySQLWalker.table_return retval = new MySQLWalker.table_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree TABLENAME166=null;
        MySQLWalker.table_spec_return table_spec167 = null;

        MySQLWalker.join_clause_return join_clause168 = null;


        CommonTree TABLENAME166_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:642:2: ( ^( TABLENAME table_spec[$common] ( join_clause[$common] )? ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:642:3: ^( TABLENAME table_spec[$common] ( join_clause[$common] )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            TABLENAME166=(CommonTree)match(input,TABLENAME,FOLLOW_TABLENAME_in_table1686); 
            TABLENAME166_tree = (CommonTree)adaptor.dupNode(TABLENAME166);

            root_1 = (CommonTree)adaptor.becomeRoot(TABLENAME166_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_table_spec_in_table1688);
            table_spec167=table_spec(common);

            state._fsp--;

            adaptor.addChild(root_1, table_spec167.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:642:35: ( join_clause[$common] )?
            int alt43=2;
            int LA43_0 = input.LA(1);

            if ( (LA43_0==JOIN) ) {
                alt43=1;
            }
            switch (alt43) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:642:35: join_clause[$common]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_join_clause_in_table1691);
                    join_clause168=join_clause(common);

                    state._fsp--;

                    adaptor.addChild(root_1, join_clause168.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		TableName tableName = (table_spec167!=null?table_spec167.tableName:null);
            		tableName.setJoinClause((join_clause168!=null?join_clause168.joinClause:null));
            		common.addTable(tableName);
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "table"

    public static class tables_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "tables"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:650:1: tables[DMLCommon common] : ^( TABLENAMES ( table[$common] )+ ) ;
    public final MySQLWalker.tables_return tables(DMLCommon common) throws RecognitionException {
        MySQLWalker.tables_return retval = new MySQLWalker.tables_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree TABLENAMES169=null;
        MySQLWalker.table_return table170 = null;


        CommonTree TABLENAMES169_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:651:2: ( ^( TABLENAMES ( table[$common] )+ ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:651:3: ^( TABLENAMES ( table[$common] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            TABLENAMES169=(CommonTree)match(input,TABLENAMES,FOLLOW_TABLENAMES_in_tables1710); 
            TABLENAMES169_tree = (CommonTree)adaptor.dupNode(TABLENAMES169);

            root_1 = (CommonTree)adaptor.becomeRoot(TABLENAMES169_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\tools\\antlr\\test\\MySQLWalker.g:651:16: ( table[$common] )+
            int cnt44=0;
            loop44:
            do {
                int alt44=2;
                int LA44_0 = input.LA(1);

                if ( (LA44_0==TABLENAME) ) {
                    alt44=1;
                }


                switch (alt44) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:651:16: table[$common]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_table_in_tables1712);
            	    table170=table(common);

            	    state._fsp--;

            	    adaptor.addChild(root_1, table170.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt44 >= 1 ) break loop44;
                        EarlyExitException eee =
                            new EarlyExitException(44, input);
                        throw eee;
                }
                cnt44++;
            } while (true);


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "tables"

    public static class table_spec_return extends TreeRuleReturnScope {
        public TableName tableName;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "table_spec"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:654:1: table_spec[DMLCommon common] returns [TableName tableName] : ( ( schema_name )? table_name ( alias )? | subquery[$common.getIndexHolder()] ( alias )? );
    public final MySQLWalker.table_spec_return table_spec(DMLCommon common) throws RecognitionException {
        MySQLWalker.table_spec_return retval = new MySQLWalker.table_spec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.schema_name_return schema_name171 = null;

        MySQLWalker.table_name_return table_name172 = null;

        MySQLWalker.alias_return alias173 = null;

        MySQLWalker.subquery_return subquery174 = null;

        MySQLWalker.alias_return alias175 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:655:2: ( ( schema_name )? table_name ( alias )? | subquery[$common.getIndexHolder()] ( alias )? )
            int alt48=2;
            int LA48_0 = input.LA(1);

            if ( (LA48_0==ID) ) {
                alt48=1;
            }
            else if ( (LA48_0==SUBQUERY) ) {
                alt48=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 48, 0, input);

                throw nvae;
            }
            switch (alt48) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:655:3: ( schema_name )? table_name ( alias )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    // D:\\tools\\antlr\\test\\MySQLWalker.g:655:3: ( schema_name )?
                    int alt45=2;
                    int LA45_0 = input.LA(1);

                    if ( (LA45_0==ID) ) {
                        int LA45_1 = input.LA(2);

                        if ( (LA45_1==ID) ) {
                            alt45=1;
                        }
                    }
                    switch (alt45) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:655:5: schema_name
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_schema_name_in_table_spec1733);
                            schema_name171=schema_name();

                            state._fsp--;

                            adaptor.addChild(root_0, schema_name171.getTree());

                            }
                            break;

                    }

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_table_name_in_table_spec1737);
                    table_name172=table_name();

                    state._fsp--;

                    adaptor.addChild(root_0, table_name172.getTree());
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:655:31: ( alias )?
                    int alt46=2;
                    int LA46_0 = input.LA(1);

                    if ( (LA46_0==AS) ) {
                        alt46=1;
                    }
                    switch (alt46) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:655:31: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_table_spec1740);
                            alias173=alias();

                            state._fsp--;

                            adaptor.addChild(root_0, alias173.getTree());

                            }
                            break;

                    }


                    		retval.tableName = getTableNameAndSchemaName((table_name172!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(table_name172.start),
                      input.getTreeAdaptor().getTokenStopIndex(table_name172.start))):null),(schema_name171!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(schema_name171.start),
                      input.getTreeAdaptor().getTokenStopIndex(schema_name171.start))):null),(alias173!=null?alias173.aliText:null), false);
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:659:3: subquery[$common.getIndexHolder()] ( alias )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_subquery_in_table_spec1748);
                    subquery174=subquery(common.getIndexHolder());

                    state._fsp--;

                    adaptor.addChild(root_0, subquery174.getTree());
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:659:38: ( alias )?
                    int alt47=2;
                    int LA47_0 = input.LA(1);

                    if ( (LA47_0==AS) ) {
                        alt47=1;
                    }
                    switch (alt47) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:659:38: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_table_spec1751);
                            alias175=alias();

                            state._fsp--;

                            adaptor.addChild(root_0, alias175.getTree());

                            }
                            break;

                    }


                    		retval.tableName = getTableSubQuery((subquery174!=null?subquery174.subselect:null),(alias175!=null?alias175.aliText:null), false);
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "table_spec"

    public static class schema_name_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "schema_name"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:665:1: schema_name : identifier ;
    public final MySQLWalker.schema_name_return schema_name() throws RecognitionException {
        MySQLWalker.schema_name_return retval = new MySQLWalker.schema_name_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.identifier_return identifier176 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:666:2: ( identifier )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:666:3: identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_schema_name1766);
            identifier176=identifier();

            state._fsp--;

            adaptor.addChild(root_0, identifier176.getTree());

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "schema_name"

    public static class subquery_return extends TreeRuleReturnScope {
        public Select subselect;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "subquery"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:669:1: subquery[BindIndexHolder where] returns [Select subselect] : ^( SUBQUERY select_command[$where] ) ;
    public final MySQLWalker.subquery_return subquery(BindIndexHolder where) throws RecognitionException {
        MySQLWalker.subquery_return retval = new MySQLWalker.subquery_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SUBQUERY177=null;
        MySQLWalker.select_command_return select_command178 = null;


        CommonTree SUBQUERY177_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:669:59: ( ^( SUBQUERY select_command[$where] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:670:2: ^( SUBQUERY select_command[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SUBQUERY177=(CommonTree)match(input,SUBQUERY,FOLLOW_SUBQUERY_in_subquery1783); 
            SUBQUERY177_tree = (CommonTree)adaptor.dupNode(SUBQUERY177);

            root_1 = (CommonTree)adaptor.becomeRoot(SUBQUERY177_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_select_command_in_subquery1785);
            select_command178=select_command(where);

            state._fsp--;

            adaptor.addChild(root_1, select_command178.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            	retval.subselect =(select_command178!=null?select_command178.select:null);
            	retval.subselect.setSubSelect(true);
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "subquery"

    public static class table_name_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "table_name"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:677:1: table_name : identifier ;
    public final MySQLWalker.table_name_return table_name() throws RecognitionException {
        MySQLWalker.table_name_return retval = new MySQLWalker.table_name_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.identifier_return identifier179 = null;



        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:678:2: ( identifier )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:678:3: identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_table_name1802);
            identifier179=identifier();

            state._fsp--;

            adaptor.addChild(root_0, identifier179.getTree());

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "table_name"

    public static class distinct_col_return extends TreeRuleReturnScope {
        public Distinct dis;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "distinct_col"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:681:1: distinct_col[Select select] returns [Distinct dis] : ^( DISTINCT ( displayed_column[$select] )+ ) ;
    public final MySQLWalker.distinct_col_return distinct_col(Select select) throws RecognitionException {
        MySQLWalker.distinct_col_return retval = new MySQLWalker.distinct_col_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree DISTINCT180=null;
        MySQLWalker.displayed_column_return displayed_column181 = null;


        CommonTree DISTINCT180_tree=null;


        Distinct dis=new Distinct();

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:685:2: ( ^( DISTINCT ( displayed_column[$select] )+ ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:686:2: ^( DISTINCT ( displayed_column[$select] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            DISTINCT180=(CommonTree)match(input,DISTINCT,FOLLOW_DISTINCT_in_distinct_col1825); 
            DISTINCT180_tree = (CommonTree)adaptor.dupNode(DISTINCT180);

            root_1 = (CommonTree)adaptor.becomeRoot(DISTINCT180_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\tools\\antlr\\test\\MySQLWalker.g:687:2: ( displayed_column[$select] )+
            int cnt49=0;
            loop49:
            do {
                int alt49=2;
                int LA49_0 = input.LA(1);

                if ( (LA49_0==COLUMN||LA49_0==CONSIST||LA49_0==ID) ) {
                    alt49=1;
                }


                switch (alt49) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLWalker.g:687:3: displayed_column[$select]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_displayed_column_in_distinct_col1830);
            	    displayed_column181=displayed_column(select);

            	    state._fsp--;

            	    adaptor.addChild(root_1, displayed_column181.getTree());
            	    dis.addColumn((displayed_column181!=null?displayed_column181.column:null));

            	    }
            	    break;

            	default :
            	    if ( cnt49 >= 1 ) break loop49;
                        EarlyExitException eee =
                            new EarlyExitException(49, input);
                        throw eee;
                }
                cnt49++;
            } while (true);


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            	  retval.dis =dis;
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "distinct_col"

    public static class displayed_column_return extends TreeRuleReturnScope {
        public Column column;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "displayed_column"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:692:1: displayed_column[Select select] returns [Column column] : ( ^( ID ( expr[$select.getIndexHolder()] )* ( alias )? ) | ^( CONSIST ID ( alias )? ) | ^( COLUMN ( table_alias )? columnAnt ( alias )? ) );
    public final MySQLWalker.displayed_column_return displayed_column(Select select) throws RecognitionException {
        MySQLWalker.displayed_column_return retval = new MySQLWalker.displayed_column_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ID182=null;
        CommonTree CONSIST185=null;
        CommonTree ID186=null;
        CommonTree COLUMN188=null;
        MySQLWalker.expr_return expr183 = null;

        MySQLWalker.alias_return alias184 = null;

        MySQLWalker.alias_return alias187 = null;

        MySQLWalker.table_alias_return table_alias189 = null;

        MySQLWalker.columnAnt_return columnAnt190 = null;

        MySQLWalker.alias_return alias191 = null;


        CommonTree ID182_tree=null;
        CommonTree CONSIST185_tree=null;
        CommonTree ID186_tree=null;
        CommonTree COLUMN188_tree=null;


        List<Object> list=new ArrayList<Object>();

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:696:2: ( ^( ID ( expr[$select.getIndexHolder()] )* ( alias )? ) | ^( CONSIST ID ( alias )? ) | ^( COLUMN ( table_alias )? columnAnt ( alias )? ) )
            int alt55=3;
            switch ( input.LA(1) ) {
            case ID:
                {
                alt55=1;
                }
                break;
            case CONSIST:
                {
                alt55=2;
                }
                break;
            case COLUMN:
                {
                alt55=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 55, 0, input);

                throw nvae;
            }

            switch (alt55) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:697:2: ^( ID ( expr[$select.getIndexHolder()] )* ( alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ID182=(CommonTree)match(input,ID,FOLLOW_ID_in_displayed_column1860); 
                    ID182_tree = (CommonTree)adaptor.dupNode(ID182);

                    root_1 = (CommonTree)adaptor.becomeRoot(ID182_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        // D:\\tools\\antlr\\test\\MySQLWalker.g:697:6: ( expr[$select.getIndexHolder()] )*
                        loop50:
                        do {
                            int alt50=2;
                            int LA50_0 = input.LA(1);

                            if ( ((LA50_0>=SUBQUERY && LA50_0<=COLUMN)||LA50_0==QUTED_STR||LA50_0==COLUMNAST||LA50_0==CONSIST||(LA50_0>=NEGATIVE && LA50_0<=INTERVAL)||(LA50_0>=BITOR && LA50_0<=DIVIDE)||(LA50_0>=ID && LA50_0<=N)||LA50_0==NUMBER||LA50_0==118||(LA50_0>=121 && LA50_0<=122)||(LA50_0>=133 && LA50_0<=134)) ) {
                                alt50=1;
                            }


                            switch (alt50) {
                        	case 1 :
                        	    // D:\\tools\\antlr\\test\\MySQLWalker.g:697:7: expr[$select.getIndexHolder()]
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    pushFollow(FOLLOW_expr_in_displayed_column1862);
                        	    expr183=expr(select.getIndexHolder());

                        	    state._fsp--;

                        	    adaptor.addChild(root_1, expr183.getTree());
                        	    list.add((expr183!=null?expr183.valueObj:null));

                        	    }
                        	    break;

                        	default :
                        	    break loop50;
                            }
                        } while (true);

                        // D:\\tools\\antlr\\test\\MySQLWalker.g:697:67: ( alias )?
                        int alt51=2;
                        int LA51_0 = input.LA(1);

                        if ( (LA51_0==AS) ) {
                            alt51=1;
                        }
                        switch (alt51) {
                            case 1 :
                                // D:\\tools\\antlr\\test\\MySQLWalker.g:697:67: alias
                                {
                                _last = (CommonTree)input.LT(1);
                                pushFollow(FOLLOW_alias_in_displayed_column1868);
                                alias184=alias();

                                state._fsp--;

                                adaptor.addChild(root_1, alias184.getTree());

                                }
                                break;

                        }


                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=funcreg.get(((ID182!=null?ID182.getText():null)).toUpperCase());
                    	  func.setValue(list);
                    	  FunctionColumn funcolumn=new FunctionColumn();
                    	  funcolumn.setFunction(func);
                    	  funcolumn.setAlias((alias184!=null?alias184.aliText:null));
                    	  //select.addColumn(funcolumn);
                    	  retval.column =funcolumn;
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:707:3: ^( CONSIST ID ( alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CONSIST185=(CommonTree)match(input,CONSIST,FOLLOW_CONSIST_in_displayed_column1879); 
                    CONSIST185_tree = (CommonTree)adaptor.dupNode(CONSIST185);

                    root_1 = (CommonTree)adaptor.becomeRoot(CONSIST185_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    ID186=(CommonTree)match(input,ID,FOLLOW_ID_in_displayed_column1881); 
                    ID186_tree = (CommonTree)adaptor.dupNode(ID186);

                    adaptor.addChild(root_1, ID186_tree);

                    // D:\\tools\\antlr\\test\\MySQLWalker.g:707:16: ( alias )?
                    int alt52=2;
                    int LA52_0 = input.LA(1);

                    if ( (LA52_0==AS) ) {
                        alt52=1;
                    }
                    switch (alt52) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:707:16: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_displayed_column1883);
                            alias187=alias();

                            state._fsp--;

                            adaptor.addChild(root_1, alias187.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Function func=consistStr.get(((ID186!=null?ID186.getText():null)).toUpperCase());
                    	  FunctionColumn funcolumn=new FunctionColumn();
                    	  funcolumn.setFunction(func);
                    	  funcolumn.setAlias((alias187!=null?alias187.aliText:null));
                    	  //select.addColumn(funcolumn);
                    	  retval.column =funcolumn;
                    	

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:715:3: ^( COLUMN ( table_alias )? columnAnt ( alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    COLUMN188=(CommonTree)match(input,COLUMN,FOLLOW_COLUMN_in_displayed_column1891); 
                    COLUMN188_tree = (CommonTree)adaptor.dupNode(COLUMN188);

                    root_1 = (CommonTree)adaptor.becomeRoot(COLUMN188_tree, root_1);



                    match(input, Token.DOWN, null); 
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:715:12: ( table_alias )?
                    int alt53=2;
                    int LA53_0 = input.LA(1);

                    if ( (LA53_0==COL_TAB) ) {
                        alt53=1;
                    }
                    switch (alt53) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:715:12: table_alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_table_alias_in_displayed_column1893);
                            table_alias189=table_alias();

                            state._fsp--;

                            adaptor.addChild(root_1, table_alias189.getTree());

                            }
                            break;

                    }

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_columnAnt_in_displayed_column1896);
                    columnAnt190=columnAnt();

                    state._fsp--;

                    adaptor.addChild(root_1, columnAnt190.getTree());
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:715:35: ( alias )?
                    int alt54=2;
                    int LA54_0 = input.LA(1);

                    if ( (LA54_0==AS) ) {
                        alt54=1;
                    }
                    switch (alt54) {
                        case 1 :
                            // D:\\tools\\antlr\\test\\MySQLWalker.g:715:35: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_displayed_column1898);
                            alias191=alias();

                            state._fsp--;

                            adaptor.addChild(root_1, alias191.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	  Column columnImp=new ColumnImp((table_alias189!=null?table_alias189.aText:null),(columnAnt190!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(columnAnt190.start),
                      input.getTreeAdaptor().getTokenStopIndex(columnAnt190.start))):null),(alias191!=null?alias191.aliText:null));
                    	  //select.addColumn(column)
                    	  retval.column =columnImp;
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "displayed_column"

    public static class columnAnt_return extends TreeRuleReturnScope {
        public String aText;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnAnt"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:743:1: columnAnt returns [String aText] : ( ASTERISK | N | identifier );
    public final MySQLWalker.columnAnt_return columnAnt() throws RecognitionException {
        MySQLWalker.columnAnt_return retval = new MySQLWalker.columnAnt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ASTERISK192=null;
        CommonTree N193=null;
        MySQLWalker.identifier_return identifier194 = null;


        CommonTree ASTERISK192_tree=null;
        CommonTree N193_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:744:2: ( ASTERISK | N | identifier )
            int alt56=3;
            switch ( input.LA(1) ) {
            case ASTERISK:
                {
                alt56=1;
                }
                break;
            case N:
                {
                alt56=2;
                }
                break;
            case ID:
                {
                alt56=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 56, 0, input);

                throw nvae;
            }

            switch (alt56) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:744:3: ASTERISK
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    ASTERISK192=(CommonTree)match(input,ASTERISK,FOLLOW_ASTERISK_in_columnAnt1948); 
                    ASTERISK192_tree = (CommonTree)adaptor.dupNode(ASTERISK192);

                    adaptor.addChild(root_0, ASTERISK192_tree);

                    retval.aText =(ASTERISK192!=null?ASTERISK192.getText():null);

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:745:3: N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    N193=(CommonTree)match(input,N,FOLLOW_N_in_columnAnt1954); 
                    N193_tree = (CommonTree)adaptor.dupNode(N193);

                    adaptor.addChild(root_0, N193_tree);

                    retval.aText =(N193!=null?N193.getText():null);

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:746:3: identifier
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_identifier_in_columnAnt1960);
                    identifier194=identifier();

                    state._fsp--;

                    adaptor.addChild(root_0, identifier194.getTree());
                    retval.aText =(identifier194!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(identifier194.start),
                      input.getTreeAdaptor().getTokenStopIndex(identifier194.start))):null);

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnAnt"

    public static class quoted_string_return extends TreeRuleReturnScope {
        public Object aText;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "quoted_string"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:749:1: quoted_string returns [Object aText] : QUOTED_STRING ;
    public final MySQLWalker.quoted_string_return quoted_string() throws RecognitionException {
        MySQLWalker.quoted_string_return retval = new MySQLWalker.quoted_string_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree QUOTED_STRING195=null;

        CommonTree QUOTED_STRING195_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:750:2: ( QUOTED_STRING )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:750:4: QUOTED_STRING
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            QUOTED_STRING195=(CommonTree)match(input,QUOTED_STRING,FOLLOW_QUOTED_STRING_in_quoted_string1976); 
            QUOTED_STRING195_tree = (CommonTree)adaptor.dupNode(QUOTED_STRING195);

            adaptor.addChild(root_0, QUOTED_STRING195_tree);


                        String temp=(QUOTED_STRING195!=null?QUOTED_STRING195.getText():null);
                        AntlrStringStream st = new AntlrStringStream(
                        		temp);
                        MySQLDateWalker.quoted_string_return ret = null;
            			MySQLDateParserLexer pl = new MySQLDateParserLexer(
            					st);
            			TokenRewriteStream tokens = new TokenRewriteStream(
            					pl);
            			MySQLDateParserParser pa = new MySQLDateParserParser(
            					tokens);

            			MySQLDateParserParser.quoted_string_return beg = null;
            			beg = pa.quoted_string();
            			CommonTree tree = (CommonTree) beg
            					.getTree();
            			CommonTreeNodeStream nodes = new CommonTreeNodeStream(
            					tree);
            			nodes.setTokenStream(tokens);
            			MySQLDateWalker walker = new MySQLDateWalker(nodes);
            			ret = walker.quoted_string();
            			Object tempObj =ret.aText;
            			if(tempObj!=null){
            				 retval.aText=tempObj;
            			}else{
            				 retval.aText=temp.substring(1,temp.length() - 1);;
            			}
            	

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "quoted_string"

    public static class identifier_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "identifier"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:780:1: identifier : ID ;
    public final MySQLWalker.identifier_return identifier() throws RecognitionException {
        MySQLWalker.identifier_return retval = new MySQLWalker.identifier_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ID196=null;

        CommonTree ID196_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:781:2: ( ID )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:781:3: ID
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            ID196=(CommonTree)match(input,ID,FOLLOW_ID_in_identifier1988); 
            ID196_tree = (CommonTree)adaptor.dupNode(ID196);

            adaptor.addChild(root_0, ID196_tree);


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "identifier"

    public static class table_alias_return extends TreeRuleReturnScope {
        public String aText;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "table_alias"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:784:1: table_alias returns [String aText] : ^( COL_TAB identifier ) ;
    public final MySQLWalker.table_alias_return table_alias() throws RecognitionException {
        MySQLWalker.table_alias_return retval = new MySQLWalker.table_alias_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree COL_TAB197=null;
        MySQLWalker.identifier_return identifier198 = null;


        CommonTree COL_TAB197_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:785:2: ( ^( COL_TAB identifier ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:785:3: ^( COL_TAB identifier )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            COL_TAB197=(CommonTree)match(input,COL_TAB,FOLLOW_COL_TAB_in_table_alias2010); 
            COL_TAB197_tree = (CommonTree)adaptor.dupNode(COL_TAB197);

            root_1 = (CommonTree)adaptor.becomeRoot(COL_TAB197_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_table_alias2012);
            identifier198=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier198.getTree());
            retval.aText =(identifier198!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier198.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier198.start))):null);

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "table_alias"

    public static class alias_return extends TreeRuleReturnScope {
        public String aliText;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "alias"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:788:1: alias returns [String aliText] : ^( AS identifier ) ;
    public final MySQLWalker.alias_return alias() throws RecognitionException {
        MySQLWalker.alias_return retval = new MySQLWalker.alias_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree AS199=null;
        MySQLWalker.identifier_return identifier200 = null;


        CommonTree AS199_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:789:2: ( ^( AS identifier ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:789:3: ^( AS identifier )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            AS199=(CommonTree)match(input,AS,FOLLOW_AS_in_alias2031); 
            AS199_tree = (CommonTree)adaptor.dupNode(AS199);

            root_1 = (CommonTree)adaptor.becomeRoot(AS199_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_alias2033);
            identifier200=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier200.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }

            retval.aliText =(identifier200!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier200.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier200.start))):null);

            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "alias"

    public static class select_command_return extends TreeRuleReturnScope {
        public Select select;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "select_command"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:796:1: select_command[BindIndexHolder where] returns [Select select] : selectClause[$select] ( fromClause[$select] )? ( whereClause[$select.getWhere()] )? ( groupByClause[$select.getWhere()] )? ( havingClause[$select.getHaving()] )? ( orderByClause[$select.getWhere()] )? ( limitClause[(MyWhereCondition)$select.getWhere()] )? ( for_update[$select] )? ;
    public final MySQLWalker.select_command_return select_command(BindIndexHolder where) throws RecognitionException {
        MySQLWalker.select_command_return retval = new MySQLWalker.select_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        MySQLWalker.selectClause_return selectClause201 = null;

        MySQLWalker.fromClause_return fromClause202 = null;

        MySQLWalker.whereClause_return whereClause203 = null;

        MySQLWalker.groupByClause_return groupByClause204 = null;

        MySQLWalker.havingClause_return havingClause205 = null;

        MySQLWalker.orderByClause_return orderByClause206 = null;

        MySQLWalker.limitClause_return limitClause207 = null;

        MySQLWalker.for_update_return for_update208 = null;




        	  if(null==where){
        	   retval.select =new MySelect();
        	  }else{
        	   retval.select = new MySelect(where);
        	  }
        	 
        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:6: ( selectClause[$select] ( fromClause[$select] )? ( whereClause[$select.getWhere()] )? ( groupByClause[$select.getWhere()] )? ( havingClause[$select.getHaving()] )? ( orderByClause[$select.getWhere()] )? ( limitClause[(MyWhereCondition)$select.getWhere()] )? ( for_update[$select] )? )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:8: selectClause[$select] ( fromClause[$select] )? ( whereClause[$select.getWhere()] )? ( groupByClause[$select.getWhere()] )? ( havingClause[$select.getHaving()] )? ( orderByClause[$select.getWhere()] )? ( limitClause[(MyWhereCondition)$select.getWhere()] )? ( for_update[$select] )?
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_selectClause_in_select_command2063);
            selectClause201=selectClause(retval.select);

            state._fsp--;

            adaptor.addChild(root_0, selectClause201.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:30: ( fromClause[$select] )?
            int alt57=2;
            int LA57_0 = input.LA(1);

            if ( (LA57_0==TABLENAMES) ) {
                alt57=1;
            }
            switch (alt57) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:804:31: fromClause[$select]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_fromClause_in_select_command2067);
                    fromClause202=fromClause(retval.select);

                    state._fsp--;

                    adaptor.addChild(root_0, fromClause202.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:54: ( whereClause[$select.getWhere()] )?
            int alt58=2;
            int LA58_0 = input.LA(1);

            if ( (LA58_0==WHERE) ) {
                alt58=1;
            }
            switch (alt58) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:804:55: whereClause[$select.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_whereClause_in_select_command2074);
                    whereClause203=whereClause(retval.select.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_0, whereClause203.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:89: ( groupByClause[$select.getWhere()] )?
            int alt59=2;
            int LA59_0 = input.LA(1);

            if ( (LA59_0==GROUPBY) ) {
                alt59=1;
            }
            switch (alt59) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:804:90: groupByClause[$select.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_groupByClause_in_select_command2080);
                    groupByClause204=groupByClause(retval.select.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_0, groupByClause204.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:126: ( havingClause[$select.getHaving()] )?
            int alt60=2;
            int LA60_0 = input.LA(1);

            if ( (LA60_0==HAVING) ) {
                alt60=1;
            }
            switch (alt60) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:804:127: havingClause[$select.getHaving()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_havingClause_in_select_command2086);
                    havingClause205=havingClause(retval.select.getHaving());

                    state._fsp--;

                    adaptor.addChild(root_0, havingClause205.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:163: ( orderByClause[$select.getWhere()] )?
            int alt61=2;
            int LA61_0 = input.LA(1);

            if ( (LA61_0==ORDERBY) ) {
                alt61=1;
            }
            switch (alt61) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:804:164: orderByClause[$select.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_orderByClause_in_select_command2092);
                    orderByClause206=orderByClause(retval.select.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_0, orderByClause206.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:201: ( limitClause[(MyWhereCondition)$select.getWhere()] )?
            int alt62=2;
            int LA62_0 = input.LA(1);

            if ( (LA62_0==141) ) {
                alt62=1;
            }
            switch (alt62) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:804:202: limitClause[(MyWhereCondition)$select.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_limitClause_in_select_command2099);
                    limitClause207=limitClause((MyWhereCondition)retval.select.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_0, limitClause207.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:804:254: ( for_update[$select] )?
            int alt63=2;
            int LA63_0 = input.LA(1);

            if ( ((LA63_0>=SHAREMODE && LA63_0<=FORUPDATE)) ) {
                alt63=1;
            }
            switch (alt63) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:804:254: for_update[$select]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_for_update_in_select_command2104);
                    for_update208=for_update(retval.select);

                    state._fsp--;

                    adaptor.addChild(root_0, for_update208.getTree());

                    }
                    break;

            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "select_command"

    public static class delete_command_return extends TreeRuleReturnScope {
        public Delete del;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "delete_command"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:806:1: delete_command returns [Delete del] : ^( DELETE tables[$del] ( whereClause[$del.getWhere()] )? ( orderByClause[$del.getWhere()] )? ( limitClause[(MyWhereCondition)$del.getWhere()] )? ) ;
    public final MySQLWalker.delete_command_return delete_command() throws RecognitionException {
        MySQLWalker.delete_command_return retval = new MySQLWalker.delete_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree DELETE209=null;
        MySQLWalker.tables_return tables210 = null;

        MySQLWalker.whereClause_return whereClause211 = null;

        MySQLWalker.orderByClause_return orderByClause212 = null;

        MySQLWalker.limitClause_return limitClause213 = null;


        CommonTree DELETE209_tree=null;

        retval.del =new Delete();
        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:808:2: ( ^( DELETE tables[$del] ( whereClause[$del.getWhere()] )? ( orderByClause[$del.getWhere()] )? ( limitClause[(MyWhereCondition)$del.getWhere()] )? ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:808:3: ^( DELETE tables[$del] ( whereClause[$del.getWhere()] )? ( orderByClause[$del.getWhere()] )? ( limitClause[(MyWhereCondition)$del.getWhere()] )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            DELETE209=(CommonTree)match(input,DELETE,FOLLOW_DELETE_in_delete_command2127); 
            DELETE209_tree = (CommonTree)adaptor.dupNode(DELETE209);

            root_1 = (CommonTree)adaptor.becomeRoot(DELETE209_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_tables_in_delete_command2129);
            tables210=tables(retval.del);

            state._fsp--;

            adaptor.addChild(root_1, tables210.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:808:25: ( whereClause[$del.getWhere()] )?
            int alt64=2;
            int LA64_0 = input.LA(1);

            if ( (LA64_0==WHERE) ) {
                alt64=1;
            }
            switch (alt64) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:808:25: whereClause[$del.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_whereClause_in_delete_command2132);
                    whereClause211=whereClause(retval.del.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, whereClause211.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:808:55: ( orderByClause[$del.getWhere()] )?
            int alt65=2;
            int LA65_0 = input.LA(1);

            if ( (LA65_0==ORDERBY) ) {
                alt65=1;
            }
            switch (alt65) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:808:55: orderByClause[$del.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_orderByClause_in_delete_command2136);
                    orderByClause212=orderByClause(retval.del.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, orderByClause212.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:808:88: ( limitClause[(MyWhereCondition)$del.getWhere()] )?
            int alt66=2;
            int LA66_0 = input.LA(1);

            if ( (LA66_0==141) ) {
                alt66=1;
            }
            switch (alt66) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:808:89: limitClause[(MyWhereCondition)$del.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_limitClause_in_delete_command2142);
                    limitClause213=limitClause((MyWhereCondition)retval.del.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, limitClause213.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "delete_command"

    public static class update_command_return extends TreeRuleReturnScope {
        public Update update;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "update_command"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:810:1: update_command returns [Update update] : ^( UPDATE tables[$update] setclause[$update] ( whereClause[$update.getWhere()] )? ( orderByClause[$update.getWhere()] )? ( limitClause[(MyWhereCondition)$update.getWhere()] )? ) ;
    public final MySQLWalker.update_command_return update_command() throws RecognitionException {
        MySQLWalker.update_command_return retval = new MySQLWalker.update_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree UPDATE214=null;
        MySQLWalker.tables_return tables215 = null;

        MySQLWalker.setclause_return setclause216 = null;

        MySQLWalker.whereClause_return whereClause217 = null;

        MySQLWalker.orderByClause_return orderByClause218 = null;

        MySQLWalker.limitClause_return limitClause219 = null;


        CommonTree UPDATE214_tree=null;

        retval.update =new Update();
        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:812:2: ( ^( UPDATE tables[$update] setclause[$update] ( whereClause[$update.getWhere()] )? ( orderByClause[$update.getWhere()] )? ( limitClause[(MyWhereCondition)$update.getWhere()] )? ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:812:3: ^( UPDATE tables[$update] setclause[$update] ( whereClause[$update.getWhere()] )? ( orderByClause[$update.getWhere()] )? ( limitClause[(MyWhereCondition)$update.getWhere()] )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            UPDATE214=(CommonTree)match(input,UPDATE,FOLLOW_UPDATE_in_update_command2164); 
            UPDATE214_tree = (CommonTree)adaptor.dupNode(UPDATE214);

            root_1 = (CommonTree)adaptor.becomeRoot(UPDATE214_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_tables_in_update_command2166);
            tables215=tables(retval.update);

            state._fsp--;

            adaptor.addChild(root_1, tables215.getTree());
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_setclause_in_update_command2169);
            setclause216=setclause(retval.update);

            state._fsp--;

            adaptor.addChild(root_1, setclause216.getTree());
            // D:\\tools\\antlr\\test\\MySQLWalker.g:812:47: ( whereClause[$update.getWhere()] )?
            int alt67=2;
            int LA67_0 = input.LA(1);

            if ( (LA67_0==WHERE) ) {
                alt67=1;
            }
            switch (alt67) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:812:47: whereClause[$update.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_whereClause_in_update_command2172);
                    whereClause217=whereClause(retval.update.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, whereClause217.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:812:80: ( orderByClause[$update.getWhere()] )?
            int alt68=2;
            int LA68_0 = input.LA(1);

            if ( (LA68_0==ORDERBY) ) {
                alt68=1;
            }
            switch (alt68) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:812:80: orderByClause[$update.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_orderByClause_in_update_command2176);
                    orderByClause218=orderByClause(retval.update.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, orderByClause218.getTree());

                    }
                    break;

            }

            // D:\\tools\\antlr\\test\\MySQLWalker.g:812:116: ( limitClause[(MyWhereCondition)$update.getWhere()] )?
            int alt69=2;
            int LA69_0 = input.LA(1);

            if ( (LA69_0==141) ) {
                alt69=1;
            }
            switch (alt69) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:812:117: limitClause[(MyWhereCondition)$update.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_limitClause_in_update_command2182);
                    limitClause219=limitClause((MyWhereCondition)retval.update.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, limitClause219.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "update_command"

    public static class limitClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "limitClause"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:815:1: limitClause[MyWhereCondition where] : ^( 'LIMIT' ( skip[$where] )? range[$where] ) ;
    public final MySQLWalker.limitClause_return limitClause(MyWhereCondition where) throws RecognitionException {
        MySQLWalker.limitClause_return retval = new MySQLWalker.limitClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree string_literal220=null;
        MySQLWalker.skip_return skip221 = null;

        MySQLWalker.range_return range222 = null;


        CommonTree string_literal220_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:816:2: ( ^( 'LIMIT' ( skip[$where] )? range[$where] ) )
            // D:\\tools\\antlr\\test\\MySQLWalker.g:816:3: ^( 'LIMIT' ( skip[$where] )? range[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            string_literal220=(CommonTree)match(input,141,FOLLOW_141_in_limitClause2199); 
            string_literal220_tree = (CommonTree)adaptor.dupNode(string_literal220);

            root_1 = (CommonTree)adaptor.becomeRoot(string_literal220_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\tools\\antlr\\test\\MySQLWalker.g:816:13: ( skip[$where] )?
            int alt70=2;
            int LA70_0 = input.LA(1);

            if ( (LA70_0==SKIP) ) {
                alt70=1;
            }
            switch (alt70) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:816:13: skip[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_skip_in_limitClause2201);
                    skip221=skip(where);

                    state._fsp--;

                    adaptor.addChild(root_1, skip221.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_range_in_limitClause2205);
            range222=range(where);

            state._fsp--;

            adaptor.addChild(root_1, range222.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            }

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "limitClause"

    public static class skip_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "skip"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:818:1: skip[MyWhereCondition where] : ( ^( SKIP MINUS N ) | ^( SKIP N ) | ^( SKIP '?' ) );
    public final MySQLWalker.skip_return skip(MyWhereCondition where) throws RecognitionException {
        MySQLWalker.skip_return retval = new MySQLWalker.skip_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SKIP223=null;
        CommonTree MINUS224=null;
        CommonTree N225=null;
        CommonTree SKIP226=null;
        CommonTree N227=null;
        CommonTree SKIP228=null;
        CommonTree char_literal229=null;

        CommonTree SKIP223_tree=null;
        CommonTree MINUS224_tree=null;
        CommonTree N225_tree=null;
        CommonTree SKIP226_tree=null;
        CommonTree N227_tree=null;
        CommonTree SKIP228_tree=null;
        CommonTree char_literal229_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:819:2: ( ^( SKIP MINUS N ) | ^( SKIP N ) | ^( SKIP '?' ) )
            int alt71=3;
            int LA71_0 = input.LA(1);

            if ( (LA71_0==SKIP) ) {
                int LA71_1 = input.LA(2);

                if ( (LA71_1==DOWN) ) {
                    switch ( input.LA(3) ) {
                    case MINUS:
                        {
                        alt71=1;
                        }
                        break;
                    case N:
                        {
                        alt71=2;
                        }
                        break;
                    case 122:
                        {
                        alt71=3;
                        }
                        break;
                    default:
                        NoViableAltException nvae =
                            new NoViableAltException("", 71, 2, input);

                        throw nvae;
                    }

                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 71, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 71, 0, input);

                throw nvae;
            }
            switch (alt71) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:819:3: ^( SKIP MINUS N )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    SKIP223=(CommonTree)match(input,SKIP,FOLLOW_SKIP_in_skip2219); 
                    SKIP223_tree = (CommonTree)adaptor.dupNode(SKIP223);

                    root_1 = (CommonTree)adaptor.becomeRoot(SKIP223_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    MINUS224=(CommonTree)match(input,MINUS,FOLLOW_MINUS_in_skip2221); 
                    MINUS224_tree = (CommonTree)adaptor.dupNode(MINUS224);

                    adaptor.addChild(root_1, MINUS224_tree);

                    _last = (CommonTree)input.LT(1);
                    N225=(CommonTree)match(input,N,FOLLOW_N_in_skip2223); 
                    N225_tree = (CommonTree)adaptor.dupNode(N225);

                    adaptor.addChild(root_1, N225_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }

                    where.setStart(-Integer.valueOf((N225!=null?N225.getText():null)));

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:820:3: ^( SKIP N )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    SKIP226=(CommonTree)match(input,SKIP,FOLLOW_SKIP_in_skip2230); 
                    SKIP226_tree = (CommonTree)adaptor.dupNode(SKIP226);

                    root_1 = (CommonTree)adaptor.becomeRoot(SKIP226_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    N227=(CommonTree)match(input,N,FOLLOW_N_in_skip2232); 
                    N227_tree = (CommonTree)adaptor.dupNode(N227);

                    adaptor.addChild(root_1, N227_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }

                    where.setStart(Integer.valueOf((N227!=null?N227.getText():null)));

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:821:3: ^( SKIP '?' )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    SKIP228=(CommonTree)match(input,SKIP,FOLLOW_SKIP_in_skip2239); 
                    SKIP228_tree = (CommonTree)adaptor.dupNode(SKIP228);

                    root_1 = (CommonTree)adaptor.becomeRoot(SKIP228_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    char_literal229=(CommonTree)match(input,122,FOLLOW_122_in_skip2241); 
                    char_literal229_tree = (CommonTree)adaptor.dupNode(char_literal229);

                    adaptor.addChild(root_1, char_literal229_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		BindVar val=new BindVar(where.selfAddAndGet());
                    		where.setStart(val);
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "skip"

    public static class range_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "range"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:826:1: range[MyWhereCondition where] : ( ^( RANGE MINUS N ) | ^( RANGE N ) | ^( RANGE '?' ) );
    public final MySQLWalker.range_return range(MyWhereCondition where) throws RecognitionException {
        MySQLWalker.range_return retval = new MySQLWalker.range_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree RANGE230=null;
        CommonTree MINUS231=null;
        CommonTree N232=null;
        CommonTree RANGE233=null;
        CommonTree N234=null;
        CommonTree RANGE235=null;
        CommonTree char_literal236=null;

        CommonTree RANGE230_tree=null;
        CommonTree MINUS231_tree=null;
        CommonTree N232_tree=null;
        CommonTree RANGE233_tree=null;
        CommonTree N234_tree=null;
        CommonTree RANGE235_tree=null;
        CommonTree char_literal236_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:827:2: ( ^( RANGE MINUS N ) | ^( RANGE N ) | ^( RANGE '?' ) )
            int alt72=3;
            int LA72_0 = input.LA(1);

            if ( (LA72_0==RANGE) ) {
                int LA72_1 = input.LA(2);

                if ( (LA72_1==DOWN) ) {
                    switch ( input.LA(3) ) {
                    case MINUS:
                        {
                        alt72=1;
                        }
                        break;
                    case N:
                        {
                        alt72=2;
                        }
                        break;
                    case 122:
                        {
                        alt72=3;
                        }
                        break;
                    default:
                        NoViableAltException nvae =
                            new NoViableAltException("", 72, 2, input);

                        throw nvae;
                    }

                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 72, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 72, 0, input);

                throw nvae;
            }
            switch (alt72) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:827:3: ^( RANGE MINUS N )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    RANGE230=(CommonTree)match(input,RANGE,FOLLOW_RANGE_in_range2255); 
                    RANGE230_tree = (CommonTree)adaptor.dupNode(RANGE230);

                    root_1 = (CommonTree)adaptor.becomeRoot(RANGE230_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    MINUS231=(CommonTree)match(input,MINUS,FOLLOW_MINUS_in_range2257); 
                    MINUS231_tree = (CommonTree)adaptor.dupNode(MINUS231);

                    adaptor.addChild(root_1, MINUS231_tree);

                    _last = (CommonTree)input.LT(1);
                    N232=(CommonTree)match(input,N,FOLLOW_N_in_range2259); 
                    N232_tree = (CommonTree)adaptor.dupNode(N232);

                    adaptor.addChild(root_1, N232_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }

                    where.setRange(-Integer.valueOf((N232!=null?N232.getText():null)));

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:828:3: ^( RANGE N )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    RANGE233=(CommonTree)match(input,RANGE,FOLLOW_RANGE_in_range2266); 
                    RANGE233_tree = (CommonTree)adaptor.dupNode(RANGE233);

                    root_1 = (CommonTree)adaptor.becomeRoot(RANGE233_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    N234=(CommonTree)match(input,N,FOLLOW_N_in_range2268); 
                    N234_tree = (CommonTree)adaptor.dupNode(N234);

                    adaptor.addChild(root_1, N234_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }

                    where.setRange(Integer.valueOf((N234!=null?N234.getText():null)));

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:829:3: ^( RANGE '?' )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    RANGE235=(CommonTree)match(input,RANGE,FOLLOW_RANGE_in_range2275); 
                    RANGE235_tree = (CommonTree)adaptor.dupNode(RANGE235);

                    root_1 = (CommonTree)adaptor.becomeRoot(RANGE235_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    char_literal236=(CommonTree)match(input,122,FOLLOW_122_in_range2277); 
                    char_literal236_tree = (CommonTree)adaptor.dupNode(char_literal236);

                    adaptor.addChild(root_1, char_literal236_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		BindVar val=new BindVar(where.selfAddAndGet());
                    	where.setRange(val);
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "range"

    public static class for_update_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "for_update"
    // D:\\tools\\antlr\\test\\MySQLWalker.g:835:1: for_update[Select select] : ( ^( FORUPDATE ( N )? ) | ^( SHAREMODE ( N )? ) );
    public final MySQLWalker.for_update_return for_update(Select select) throws RecognitionException {
        MySQLWalker.for_update_return retval = new MySQLWalker.for_update_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree FORUPDATE237=null;
        CommonTree N238=null;
        CommonTree SHAREMODE239=null;
        CommonTree N240=null;

        CommonTree FORUPDATE237_tree=null;
        CommonTree N238_tree=null;
        CommonTree SHAREMODE239_tree=null;
        CommonTree N240_tree=null;

        try {
            // D:\\tools\\antlr\\test\\MySQLWalker.g:836:2: ( ^( FORUPDATE ( N )? ) | ^( SHAREMODE ( N )? ) )
            int alt75=2;
            int LA75_0 = input.LA(1);

            if ( (LA75_0==FORUPDATE) ) {
                alt75=1;
            }
            else if ( (LA75_0==SHAREMODE) ) {
                alt75=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 75, 0, input);

                throw nvae;
            }
            switch (alt75) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:836:3: ^( FORUPDATE ( N )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    FORUPDATE237=(CommonTree)match(input,FORUPDATE,FOLLOW_FORUPDATE_in_for_update2293); 
                    FORUPDATE237_tree = (CommonTree)adaptor.dupNode(FORUPDATE237);

                    root_1 = (CommonTree)adaptor.becomeRoot(FORUPDATE237_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        // D:\\tools\\antlr\\test\\MySQLWalker.g:836:15: ( N )?
                        int alt73=2;
                        int LA73_0 = input.LA(1);

                        if ( (LA73_0==N) ) {
                            alt73=1;
                        }
                        switch (alt73) {
                            case 1 :
                                // D:\\tools\\antlr\\test\\MySQLWalker.g:836:15: N
                                {
                                _last = (CommonTree)input.LT(1);
                                N238=(CommonTree)match(input,N,FOLLOW_N_in_for_update2295); 
                                N238_tree = (CommonTree)adaptor.dupNode(N238);

                                adaptor.addChild(root_1, N238_tree);


                                }
                                break;

                        }


                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		MySQLForUpdate forUpdate = new MySQLForUpdate();
                    		select.addForUpdate(forUpdate);
                    	

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLWalker.g:841:3: ^( SHAREMODE ( N )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    SHAREMODE239=(CommonTree)match(input,SHAREMODE,FOLLOW_SHAREMODE_in_for_update2305); 
                    SHAREMODE239_tree = (CommonTree)adaptor.dupNode(SHAREMODE239);

                    root_1 = (CommonTree)adaptor.becomeRoot(SHAREMODE239_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        // D:\\tools\\antlr\\test\\MySQLWalker.g:841:15: ( N )?
                        int alt74=2;
                        int LA74_0 = input.LA(1);

                        if ( (LA74_0==N) ) {
                            alt74=1;
                        }
                        switch (alt74) {
                            case 1 :
                                // D:\\tools\\antlr\\test\\MySQLWalker.g:841:15: N
                                {
                                _last = (CommonTree)input.LT(1);
                                N240=(CommonTree)match(input,N,FOLLOW_N_in_for_update2307); 
                                N240_tree = (CommonTree)adaptor.dupNode(N240);

                                adaptor.addChild(root_1, N240_tree);


                                }
                                break;

                        }


                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		MySQLLockInShareMode share = new MySQLLockInShareMode();
                    		select.addForUpdate(share);
                    	

                    }
                    break;

            }
            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "for_update"

    // Delegated rules


    protected DFA17 dfa17 = new DFA17(this);
    static final String DFA17_eotS =
        "\15\uffff";
    static final String DFA17_eofS =
        "\15\uffff";
    static final String DFA17_minS =
        "\1\3\14\uffff";
    static final String DFA17_maxS =
        "\1\161\14\uffff";
    static final String DFA17_acceptS =
        "\1\uffff\1\2\13\1";
    static final String DFA17_specialS =
        "\15\uffff}>";
    static final String[] DFA17_transitionS = {
            "\1\1\12\uffff\1\10\23\uffff\1\11\1\12\3\uffff\1\14\1\13\7\uffff"+
            "\1\1\24\uffff\1\2\16\uffff\1\4\1\5\1\3\1\6\1\7\27\uffff\2\1",
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

    static final short[] DFA17_eot = DFA.unpackEncodedString(DFA17_eotS);
    static final short[] DFA17_eof = DFA.unpackEncodedString(DFA17_eofS);
    static final char[] DFA17_min = DFA.unpackEncodedStringToUnsignedChars(DFA17_minS);
    static final char[] DFA17_max = DFA.unpackEncodedStringToUnsignedChars(DFA17_maxS);
    static final short[] DFA17_accept = DFA.unpackEncodedString(DFA17_acceptS);
    static final short[] DFA17_special = DFA.unpackEncodedString(DFA17_specialS);
    static final short[][] DFA17_transition;

    static {
        int numStates = DFA17_transitionS.length;
        DFA17_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA17_transition[i] = DFA.unpackEncodedString(DFA17_transitionS[i]);
        }
    }

    class DFA17 extends DFA {

        public DFA17(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 17;
            this.eot = DFA17_eot;
            this.eof = DFA17_eof;
            this.min = DFA17_min;
            this.max = DFA17_max;
            this.accept = DFA17_accept;
            this.special = DFA17_special;
            this.transition = DFA17_transition;
        }
        public String getDescription() {
            return "()+ loopback of 186:3: ( condition_expr[$where,$exp] )+";
        }
    }
 

    public static final BitSet FOLLOW_start_rule_in_beg57 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_select_command_in_start_rule74 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insert_command_in_start_rule80 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_update_command_in_start_rule85 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_delete_command_in_start_rule90 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_replace_command_in_start_rule95 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SET_in_setclause108 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_updateColumnSpecs_in_setclause110 = new BitSet(new long[]{0x0020000000000008L});
    public static final BitSet FOLLOW_SET_ELE_in_updateColumnSpecs127 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_updateColumnSpec_in_updateColumnSpecs129 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_EQ_in_updateColumnSpec145 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_alias_in_updateColumnSpec147 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_identifier_in_updateColumnSpec150 = new BitSet(new long[]{0xC102000000020180L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_in_updateColumnSpec152 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_INSERT_in_insert_command175 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_tables_in_insert_command177 = new BitSet(new long[]{0x0004800000000000L});
    public static final BitSet FOLLOW_column_specs_in_insert_command180 = new BitSet(new long[]{0x0004800000000000L});
    public static final BitSet FOLLOW_values_in_insert_command184 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_REPLACE_in_replace_command205 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_tables_in_replace_command207 = new BitSet(new long[]{0x0004800000000000L});
    public static final BitSet FOLLOW_column_specs_in_replace_command210 = new BitSet(new long[]{0x0004800000000000L});
    public static final BitSet FOLLOW_values_in_replace_command214 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_INSERT_VAL_in_values237 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_values240 = new BitSet(new long[]{0xC102000000020188L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_COLUMNS_in_column_specs258 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_column_spec_in_column_specs260 = new BitSet(new long[]{0x0000000000000108L});
    public static final BitSet FOLLOW_COLUMN_in_column_spec276 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_column_spec278 = new BitSet(new long[]{0x0000000000000008L,0x0000000000040000L});
    public static final BitSet FOLLOW_table_name_in_column_spec280 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_WHERE_in_whereClause297 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_sqlCondition_in_whereClause299 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_GROUPBY_in_groupByClause314 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_columnNamesAfterWhere_in_groupByClause316 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_HAVING_in_havingClause340 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_PAREN_in_havingClause342 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ORDERBY_in_orderByClause371 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_columnNamesAfterWhere_in_orderByClause373 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_columnNameAfterWhere_in_columnNamesAfterWhere396 = new BitSet(new long[]{0x0000000000000002L,0x0000000006000000L});
    public static final BitSet FOLLOW_ASC_in_columnNameAfterWhere410 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_columnNameAfterWhere413 = new BitSet(new long[]{0x0040000000000008L});
    public static final BitSet FOLLOW_table_alias_in_columnNameAfterWhere415 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_DESC_in_columnNameAfterWhere425 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_columnNameAfterWhere428 = new BitSet(new long[]{0x0040000000000008L});
    public static final BitSet FOLLOW_table_alias_in_columnNameAfterWhere430 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SELECT_in_selectClause454 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_select_list_in_selectClause457 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_CONDITION_OR_NOT_in_sqlCondition488 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_sqlCondition490 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_CONDITION_OR_in_sqlCondition497 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_sqlCondition499 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_112_in_condition517 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_condition521 = new BitSet(new long[]{0x0001018C00004008L,0x0003000001F00020L});
    public static final BitSet FOLLOW_113_in_condition533 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_condition535 = new BitSet(new long[]{0x0001018C00004008L,0x0003000001F00020L});
    public static final BitSet FOLLOW_condition_PAREN_in_condition542 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_PRIORITY_in_condition548 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_condition550 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_condition_expr_in_condition_PAREN563 = new BitSet(new long[]{0x0000018C00004002L,0x0000000001F00020L});
    public static final BitSet FOLLOW_comparisonCondition_in_condition_expr578 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_inCondition_in_condition_expr584 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_isCondition_in_condition_expr591 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_likeCondition_in_condition_expr598 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NOT_BETWEEN_in_betweenCondition612 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_between_and_in_betweenCondition614 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BETWEEN_in_betweenCondition620 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_between_and_in_betweenCondition622 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_NOT_LIKE_in_likeCondition647 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_likeCondition649 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_likeCondition652 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_LIKE_in_likeCondition660 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_likeCondition662 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_likeCondition665 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ISNOT_in_isCondition681 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_NULL_in_isCondition683 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_isCondition685 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_IS_in_isCondition693 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_NULL_in_isCondition695 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_isCondition697 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_EQ_in_comparisonCondition714 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition716 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition719 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_NOT_EQ_in_comparisonCondition729 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition731 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition734 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_LTH_in_comparisonCondition744 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition746 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition749 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_GTH_in_comparisonCondition759 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition761 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition764 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_LEQ_in_comparisonCondition774 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition776 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition779 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_GEQ_in_comparisonCondition789 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition791 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition794 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_CONDITION_LEFT_in_left_cond816 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_left_cond818 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_IN_LISTS_in_in_list839 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_inCondition_expr_adds_in_in_list841 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_IN_in_inCondition861 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_111_in_inCondition865 = new BitSet(new long[]{0xC102000000320180L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_subquery_in_inCondition868 = new BitSet(new long[]{0x0000000000300000L});
    public static final BitSet FOLLOW_in_list_in_inCondition872 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_inCondition877 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_expr_add_in_inCondition_expr_adds908 = new BitSet(new long[]{0xC102000000020102L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr933 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_subquery_in_expr940 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NEGATIVE_in_expr_add968 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add972 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_POSITIVE_in_expr_add981 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add985 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_PLUS_in_expr_add994 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add998 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1003 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_MINUS_in_expr_add1012 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1016 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1021 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BITOR_in_expr_add1029 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1033 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1038 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BITAND_in_expr_add1046 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1050 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1055 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BITXOR_in_expr_add1063 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1067 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1072 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SHIFTLEFT_in_expr_add1080 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1084 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1089 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SHIFTRIGHT_in_expr_add1097 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1101 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1106 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ASTERISK_in_expr_add1114 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1118 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1123 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_DIVIDE_in_expr_add1131 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1135 = new BitSet(new long[]{0xC102000000020100L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_expr_add_in_expr_add1140 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_N_in_expr_add1147 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NUMBER_in_expr_add1155 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_boolean_literal_in_expr_add1162 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_118_in_expr_add1166 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_121_in_expr_add1174 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_122_in_expr_add1178 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUTED_STR_in_expr_add1184 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_quoted_string_in_expr_add1186 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_COLUMN_in_expr_add1193 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_expr_add1195 = new BitSet(new long[]{0x0000000000000008L,0x0000000000040000L});
    public static final BitSet FOLLOW_table_name_in_expr_add1197 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_COLUMNAST_in_expr_add1207 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ASTERISK_in_expr_add1209 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_INTERVAL_in_expr_add1226 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_inner_value_in_expr_add1230 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_unit_in_expr_add1235 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ID_in_expr_add1244 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_expr_add1247 = new BitSet(new long[]{0xC102000000020188L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_CONSIST_in_expr_add1260 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ID_in_expr_add1262 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_122_in_inner_value1320 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_N_in_inner_value1325 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_MINUS_in_inner_value1330 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_N_in_inner_value1332 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UNIT_in_unit1357 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ID_in_unit1359 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_set_in_value0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_133_in_boolean_literal1406 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_134_in_boolean_literal1415 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SELECT_LIST_in_select_list1429 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_dis_col_in_select_list1431 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_displayed_column_in_dis_col1446 = new BitSet(new long[]{0x0100000000000102L,0x0000000000040000L});
    public static final BitSet FOLLOW_distinct_col_in_dis_col1455 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_TABLENAMES_in_fromClause1473 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_in_fromClause1475 = new BitSet(new long[]{0x0000000000000028L});
    public static final BitSet FOLLOW_JOIN_in_join_clause1502 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_name_in_join_clause1504 = new BitSet(new long[]{0x0000000000000300L});
    public static final BitSet FOLLOW_alias_in_join_clause1506 = new BitSet(new long[]{0x0000000000000300L});
    public static final BitSet FOLLOW_join_column_in_join_clause1511 = new BitSet(new long[]{0x0000000000000300L});
    public static final BitSet FOLLOW_join_column_in_join_clause1515 = new BitSet(new long[]{0x00000000DE000008L});
    public static final BitSet FOLLOW_join_type_in_join_clause1517 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_COLUMN_in_join_column1560 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_join_column1562 = new BitSet(new long[]{0x0000000000000008L,0x0000000000040000L});
    public static final BitSet FOLLOW_table_name_in_join_column1564 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_INNER_in_join_type1605 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LEFT_in_join_type1613 = new BitSet(new long[]{0x0000000001000002L});
    public static final BitSet FOLLOW_outer_in_join_type1615 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RIGHT_in_join_type1623 = new BitSet(new long[]{0x0000000001000002L});
    public static final BitSet FOLLOW_outer_in_join_type1625 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FULL_in_join_type1633 = new BitSet(new long[]{0x0000000001000002L});
    public static final BitSet FOLLOW_outer_in_join_type1635 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UNION_in_join_type1644 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_CROSS_in_join_type1652 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_OUTER_in_outer1671 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_TABLENAME_in_table1686 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_spec_in_table1688 = new BitSet(new long[]{0x0000000020000008L});
    public static final BitSet FOLLOW_join_clause_in_table1691 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_TABLENAMES_in_tables1710 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_in_tables1712 = new BitSet(new long[]{0x0000000000000028L});
    public static final BitSet FOLLOW_schema_name_in_table_spec1733 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_table_name_in_table_spec1737 = new BitSet(new long[]{0x0000000000000202L});
    public static final BitSet FOLLOW_alias_in_table_spec1740 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_subquery_in_table_spec1748 = new BitSet(new long[]{0x0000000000000202L});
    public static final BitSet FOLLOW_alias_in_table_spec1751 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_schema_name1766 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SUBQUERY_in_subquery1783 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_select_command_in_subquery1785 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_identifier_in_table_name1802 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DISTINCT_in_distinct_col1825 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_displayed_column_in_distinct_col1830 = new BitSet(new long[]{0x0100000000000108L,0x0000000000040000L});
    public static final BitSet FOLLOW_ID_in_displayed_column1860 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_displayed_column1862 = new BitSet(new long[]{0xC102000000020388L,0x06400000100DFF01L,0x0000000000000060L});
    public static final BitSet FOLLOW_alias_in_displayed_column1868 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_CONSIST_in_displayed_column1879 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ID_in_displayed_column1881 = new BitSet(new long[]{0x0000000000000208L});
    public static final BitSet FOLLOW_alias_in_displayed_column1883 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_COLUMN_in_displayed_column1891 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_alias_in_displayed_column1893 = new BitSet(new long[]{0x0000000000000000L,0x00000000000C8000L});
    public static final BitSet FOLLOW_columnAnt_in_displayed_column1896 = new BitSet(new long[]{0x0000000000000208L});
    public static final BitSet FOLLOW_alias_in_displayed_column1898 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ASTERISK_in_columnAnt1948 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_N_in_columnAnt1954 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_columnAnt1960 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_STRING_in_quoted_string1976 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_identifier1988 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_COL_TAB_in_table_alias2010 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_table_alias2012 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_AS_in_alias2031 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_alias2033 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_selectClause_in_select_command2063 = new BitSet(new long[]{0x3000380000040042L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_fromClause_in_select_command2067 = new BitSet(new long[]{0x3000380000040002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_whereClause_in_select_command2074 = new BitSet(new long[]{0x3000380000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_groupByClause_in_select_command2080 = new BitSet(new long[]{0x3000300000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_havingClause_in_select_command2086 = new BitSet(new long[]{0x3000200000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_orderByClause_in_select_command2092 = new BitSet(new long[]{0x3000000000000002L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_limitClause_in_select_command2099 = new BitSet(new long[]{0x3000000000000002L});
    public static final BitSet FOLLOW_for_update_in_select_command2104 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DELETE_in_delete_command2127 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_tables_in_delete_command2129 = new BitSet(new long[]{0x0000200000040008L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_whereClause_in_delete_command2132 = new BitSet(new long[]{0x0000200000000008L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_orderByClause_in_delete_command2136 = new BitSet(new long[]{0x0000000000000008L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_limitClause_in_delete_command2142 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_UPDATE_in_update_command2164 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_tables_in_update_command2166 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_setclause_in_update_command2169 = new BitSet(new long[]{0x0000200000040008L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_whereClause_in_update_command2172 = new BitSet(new long[]{0x0000200000000008L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_orderByClause_in_update_command2176 = new BitSet(new long[]{0x0000000000000008L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_limitClause_in_update_command2182 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_141_in_limitClause2199 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_skip_in_limitClause2201 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_range_in_limitClause2205 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SKIP_in_skip2219 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_skip2221 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_N_in_skip2223 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SKIP_in_skip2230 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_N_in_skip2232 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SKIP_in_skip2239 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_122_in_skip2241 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_RANGE_in_range2255 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_MINUS_in_range2257 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_N_in_range2259 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_RANGE_in_range2266 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_N_in_range2268 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_RANGE_in_range2275 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_122_in_range2277 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_FORUPDATE_in_for_update2293 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_N_in_for_update2295 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SHAREMODE_in_for_update2305 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_N_in_for_update2307 = new BitSet(new long[]{0x0000000000000008L});

}