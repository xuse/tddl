// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g 2010-12-07 20:43:39

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




import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;import java.util.Stack;
import java.util.List;
import java.util.ArrayList;


public class OracleWalker extends TreeParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "ALIAS", "TABLENAME", "TABLENAMES", "SUBQUERY", "COLUMN", "AS", "OUTER", "SELECT", "DISPLAYED_COUNT_COLUMN", "DISPLAYED_COLUMN", "IN", "NOT", "SELECT_LIST", "QUTED_STR", "WHERE", "CONDITION_OR", "CONDITION_LEFT", "INNER", "LEFT", "RIGHT", "FULL", "UNION", "CROSS", "IN_LISTS", "CONDITION_OR_NOT", "AND", "OR", "ISNOT", "IS", "NULL", "NAN", "INFINITE", "LIKE", "NOT_LIKE", "NOT_BETWEEN", "BETWEEN", "GROUPBY", "ORDERBY", "INSERT", "INSERT_VAL", "PRIORITY", "COLUMNAST", "COLUMNS", "UPDATE", "SET", "SET_ELE", "COL_TAB", "DELETE", "CONSIST", "JOIN", "FORUPDATE", "NOWAIT", "OF", "WAIT", "CAST", "NEGATIVE", "POSITIVE", "COMMA", "EQ", "LPAREN", "RPAREN", "BITOR", "BITAND", "BITXOR", "SHIFTLEFT", "SHIFTRIGHT", "PLUS", "MINUS", "ASTERISK", "DIVIDE", "EXPONENT", "ID", "LTH", "GTH", "NOT_EQ", "LEQ", "GEQ", "ASC", "DESC", "DOT", "N", "NUMBER", "POINT", "ARROW", "DOUBLEVERTBAR", "QUOTED_STRING", "DOUBLEQUOTED_STRING", "WS", "'SET'", "'GRUOP'", "'BY'", "'ORDER'", "'WHERE'", "'NOT'", "'OR'", "'AND'", "'BETWEEN'", "'IS'", "'NAN'", "'INFINITE'", "'NULL'", "'IN'", "'LIKE'", "'CAST'", "'AS'", "'FROM'", "'?'", "'JOIN'", "'ON'", "'INNER'", "'LEFT'", "'RIGHT'", "'FULL'", "'UNION'", "'CROSS'", "'OUTER'", "'TRUE'", "'FALSE'", "'SELECT'", "'/*'", "'*/'", "'INSERT'", "'INTO'", "'VALUES'", "'FOR'", "'UPDATE'", "'OF'", "'NOWAIT'", "'WAIT'", "'DELETE'"
    };
    public static final int CAST=58;
    public static final int COLUMNAST=45;
    public static final int EXPONENT=74;
    public static final int CONDITION_LEFT=20;
    public static final int NOT=15;
    public static final int CONSIST=52;
    public static final int EOF=-1;
    public static final int T__93=93;
    public static final int DISPLAYED_COUNT_COLUMN=12;
    public static final int T__94=94;
    public static final int RPAREN=64;
    public static final int T__92=92;
    public static final int FULL=24;
    public static final int INSERT=42;
    public static final int GEQ=80;
    public static final int EQ=62;
    public static final int SELECT=11;
    public static final int T__99=99;
    public static final int GROUPBY=40;
    public static final int T__98=98;
    public static final int T__97=97;
    public static final int DIVIDE=73;
    public static final int T__96=96;
    public static final int T__95=95;
    public static final int FORUPDATE=54;
    public static final int POSITIVE=60;
    public static final int DISPLAYED_COLUMN=13;
    public static final int ASC=81;
    public static final int N=84;
    public static final int NULL=33;
    public static final int NUMBER=85;
    public static final int CONDITION_OR=19;
    public static final int DELETE=51;
    public static final int DOUBLEVERTBAR=88;
    public static final int TABLENAMES=6;
    public static final int WAIT=57;
    public static final int OF=56;
    public static final int T__126=126;
    public static final int T__125=125;
    public static final int T__128=128;
    public static final int T__127=127;
    public static final int WS=91;
    public static final int BITOR=65;
    public static final int T__129=129;
    public static final int PRIORITY=44;
    public static final int SELECT_LIST=16;
    public static final int OR=30;
    public static final int ALIAS=4;
    public static final int T__130=130;
    public static final int T__131=131;
    public static final int T__132=132;
    public static final int T__133=133;
    public static final int NAN=34;
    public static final int WHERE=18;
    public static final int INNER=21;
    public static final int POINT=86;
    public static final int T__118=118;
    public static final int T__119=119;
    public static final int T__116=116;
    public static final int GTH=77;
    public static final int T__117=117;
    public static final int T__114=114;
    public static final int T__115=115;
    public static final int T__124=124;
    public static final int IN_LISTS=27;
    public static final int SHIFTLEFT=68;
    public static final int T__123=123;
    public static final int UPDATE=47;
    public static final int T__122=122;
    public static final int T__121=121;
    public static final int T__120=120;
    public static final int ORDERBY=41;
    public static final int ID=75;
    public static final int AND=29;
    public static final int CROSS=26;
    public static final int BITAND=66;
    public static final int ASTERISK=72;
    public static final int LPAREN=63;
    public static final int NOWAIT=55;
    public static final int SUBQUERY=7;
    public static final int AS=9;
    public static final int IN=14;
    public static final int QUTED_STR=17;
    public static final int T__107=107;
    public static final int COMMA=61;
    public static final int T__108=108;
    public static final int T__109=109;
    public static final int IS=32;
    public static final int LEFT=22;
    public static final int T__103=103;
    public static final int T__104=104;
    public static final int SHIFTRIGHT=69;
    public static final int T__105=105;
    public static final int BITXOR=67;
    public static final int COLUMN=8;
    public static final int T__106=106;
    public static final int T__111=111;
    public static final int T__110=110;
    public static final int T__113=113;
    public static final int T__112=112;
    public static final int PLUS=70;
    public static final int QUOTED_STRING=89;
    public static final int DOT=83;
    public static final int LIKE=36;
    public static final int OUTER=10;
    public static final int DOUBLEQUOTED_STRING=90;
    public static final int NEGATIVE=59;
    public static final int NOT_LIKE=37;
    public static final int NOT_BETWEEN=38;
    public static final int SET=48;
    public static final int INFINITE=35;
    public static final int RIGHT=23;
    public static final int T__102=102;
    public static final int INSERT_VAL=43;
    public static final int T__101=101;
    public static final int T__100=100;
    public static final int MINUS=71;
    public static final int CONDITION_OR_NOT=28;
    public static final int JOIN=53;
    public static final int UNION=25;
    public static final int NOT_EQ=78;
    public static final int LTH=76;
    public static final int COLUMNS=46;
    public static final int COL_TAB=50;
    public static final int SET_ELE=49;
    public static final int ARROW=87;
    public static final int ISNOT=31;
    public static final int DESC=82;
    public static final int BETWEEN=39;
    public static final int TABLENAME=5;
    public static final int LEQ=79;

    // delegates
    // delegators


        public OracleWalker(TreeNodeStream input) {
            this(input, new RecognizerSharedState());
        }
        public OracleWalker(TreeNodeStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return OracleWalker.tokenNames; }
    public String getGrammarFileName() { return "D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g"; }





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


    public static class beg_return extends TreeRuleReturnScope {
        public DMLCommon obj;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "beg"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:70:1: beg returns [DMLCommon obj] : start_rule ;
    public final OracleWalker.beg_return beg() throws RecognitionException {
        OracleWalker.beg_return retval = new OracleWalker.beg_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.start_rule_return start_rule1 = null;



        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:70:27: ( start_rule )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:71:1: start_rule
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_start_rule_in_beg48);
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:74:1: start_rule returns [DMLCommon obj] : ( select_command[null] | insert_command | update_command | delete_command );
    public final OracleWalker.start_rule_return start_rule() throws RecognitionException {
        OracleWalker.start_rule_return retval = new OracleWalker.start_rule_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.select_command_return select_command2 = null;

        OracleWalker.insert_command_return insert_command3 = null;

        OracleWalker.update_command_return update_command4 = null;

        OracleWalker.delete_command_return delete_command5 = null;



        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:75:2: ( select_command[null] | insert_command | update_command | delete_command )
            int alt1=4;
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
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }

            switch (alt1) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:75:3: select_command[null]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_select_command_in_start_rule65);
                    select_command2=select_command(null);

                    state._fsp--;

                    adaptor.addChild(root_0, select_command2.getTree());
                    retval.obj =(select_command2!=null?select_command2.select:null);

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:76:3: insert_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_insert_command_in_start_rule71);
                    insert_command3=insert_command();

                    state._fsp--;

                    adaptor.addChild(root_0, insert_command3.getTree());
                    retval.obj =(insert_command3!=null?insert_command3.ins:null);

                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:77:3: update_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_update_command_in_start_rule76);
                    update_command4=update_command();

                    state._fsp--;

                    adaptor.addChild(root_0, update_command4.getTree());
                    retval.obj =(update_command4!=null?update_command4.update:null);

                    }
                    break;
                case 4 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:78:3: delete_command
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_delete_command_in_start_rule81);
                    delete_command5=delete_command();

                    state._fsp--;

                    adaptor.addChild(root_0, delete_command5.getTree());
                    retval.obj =(delete_command5!=null?delete_command5.del:null);

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:81:1: setclause[Update update] : ^( SET ( updateColumnSpecs[$update] )+ ) ;
    public final OracleWalker.setclause_return setclause(Update update) throws RecognitionException {
        OracleWalker.setclause_return retval = new OracleWalker.setclause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SET6=null;
        OracleWalker.updateColumnSpecs_return updateColumnSpecs7 = null;


        CommonTree SET6_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:82:2: ( ^( SET ( updateColumnSpecs[$update] )+ ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:82:3: ^( SET ( updateColumnSpecs[$update] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SET6=(CommonTree)match(input,SET,FOLLOW_SET_in_setclause94); 
            SET6_tree = (CommonTree)adaptor.dupNode(SET6);

            root_1 = (CommonTree)adaptor.becomeRoot(SET6_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:82:9: ( updateColumnSpecs[$update] )+
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
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:82:9: updateColumnSpecs[$update]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_updateColumnSpecs_in_setclause96);
            	    updateColumnSpecs7=updateColumnSpecs(update);

            	    state._fsp--;

            	    adaptor.addChild(root_1, updateColumnSpecs7.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:84:1: updateColumnSpecs[Update update] : ^( SET_ELE updateColumnSpec[$update] ) ;
    public final OracleWalker.updateColumnSpecs_return updateColumnSpecs(Update update) throws RecognitionException {
        OracleWalker.updateColumnSpecs_return retval = new OracleWalker.updateColumnSpecs_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SET_ELE8=null;
        OracleWalker.updateColumnSpec_return updateColumnSpec9 = null;


        CommonTree SET_ELE8_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:85:2: ( ^( SET_ELE updateColumnSpec[$update] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:85:3: ^( SET_ELE updateColumnSpec[$update] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SET_ELE8=(CommonTree)match(input,SET_ELE,FOLLOW_SET_ELE_in_updateColumnSpecs111); 
            SET_ELE8_tree = (CommonTree)adaptor.dupNode(SET_ELE8);

            root_1 = (CommonTree)adaptor.becomeRoot(SET_ELE8_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_updateColumnSpec_in_updateColumnSpecs113);
            updateColumnSpec9=updateColumnSpec(update);

            state._fsp--;

            adaptor.addChild(root_1, updateColumnSpec9.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:87:1: updateColumnSpec[Update update] : ^( EQ ( table_alias )? identifier expr[$update.getIndexHolder()] ) ;
    public final OracleWalker.updateColumnSpec_return updateColumnSpec(Update update) throws RecognitionException {
        OracleWalker.updateColumnSpec_return retval = new OracleWalker.updateColumnSpec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree EQ10=null;
        OracleWalker.table_alias_return table_alias11 = null;

        OracleWalker.identifier_return identifier12 = null;

        OracleWalker.expr_return expr13 = null;


        CommonTree EQ10_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:88:2: ( ^( EQ ( table_alias )? identifier expr[$update.getIndexHolder()] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:88:3: ^( EQ ( table_alias )? identifier expr[$update.getIndexHolder()] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            EQ10=(CommonTree)match(input,EQ,FOLLOW_EQ_in_updateColumnSpec127); 
            EQ10_tree = (CommonTree)adaptor.dupNode(EQ10);

            root_1 = (CommonTree)adaptor.becomeRoot(EQ10_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:88:8: ( table_alias )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==COL_TAB) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:88:8: table_alias
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_table_alias_in_updateColumnSpec129);
                    table_alias11=table_alias();

                    state._fsp--;

                    adaptor.addChild(root_1, table_alias11.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_updateColumnSpec132);
            identifier12=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier12.getTree());
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_expr_in_updateColumnSpec134);
            expr13=expr(update.getIndexHolder());

            state._fsp--;

            adaptor.addChild(root_1, expr13.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            	update.addSetElement((identifier12!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier12.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier12.start))):null),(table_alias11!=null?table_alias11.aText:null),(expr13!=null?expr13.valueObj:null));
            	

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

    public static class values_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "values"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:95:1: values[Insert ins] : ^( INSERT_VAL ( expr[$ins.getIndexHolder()] )* ) ;
    public final OracleWalker.values_return values(Insert ins) throws RecognitionException {
        OracleWalker.values_return retval = new OracleWalker.values_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree INSERT_VAL14=null;
        OracleWalker.expr_return expr15 = null;


        CommonTree INSERT_VAL14_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:95:20: ( ^( INSERT_VAL ( expr[$ins.getIndexHolder()] )* ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:95:21: ^( INSERT_VAL ( expr[$ins.getIndexHolder()] )* )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            INSERT_VAL14=(CommonTree)match(input,INSERT_VAL,FOLLOW_INSERT_VAL_in_values151); 
            INSERT_VAL14_tree = (CommonTree)adaptor.dupNode(INSERT_VAL14);

            root_1 = (CommonTree)adaptor.becomeRoot(INSERT_VAL14_tree, root_1);



            if ( input.LA(1)==Token.DOWN ) {
                match(input, Token.DOWN, null); 
                // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:95:34: ( expr[$ins.getIndexHolder()] )*
                loop4:
                do {
                    int alt4=2;
                    int LA4_0 = input.LA(1);

                    if ( ((LA4_0>=SUBQUERY && LA4_0<=COLUMN)||LA4_0==QUTED_STR||LA4_0==COLUMNAST||LA4_0==CONSIST||(LA4_0>=CAST && LA4_0<=POSITIVE)||(LA4_0>=BITOR && LA4_0<=DIVIDE)||LA4_0==ID||(LA4_0>=N && LA4_0<=NUMBER)||LA4_0==104||LA4_0==110||(LA4_0>=120 && LA4_0<=121)) ) {
                        alt4=1;
                    }


                    switch (alt4) {
                	case 1 :
                	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:95:35: expr[$ins.getIndexHolder()]
                	    {
                	    _last = (CommonTree)input.LT(1);
                	    pushFollow(FOLLOW_expr_in_values154);
                	    expr15=expr(ins.getIndexHolder());

                	    state._fsp--;

                	    adaptor.addChild(root_1, expr15.getTree());
                	    ins.addValue((expr15!=null?expr15.valueObj:null));

                	    }
                	    break;

                	default :
                	    break loop4;
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:97:1: column_specs[Insert ins] : ^( COLUMNS ( column_spec[$ins] )+ ) ;
    public final OracleWalker.column_specs_return column_specs(Insert ins) throws RecognitionException {
        OracleWalker.column_specs_return retval = new OracleWalker.column_specs_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree COLUMNS16=null;
        OracleWalker.column_spec_return column_spec17 = null;


        CommonTree COLUMNS16_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:98:2: ( ^( COLUMNS ( column_spec[$ins] )+ ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:98:3: ^( COLUMNS ( column_spec[$ins] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            COLUMNS16=(CommonTree)match(input,COLUMNS,FOLLOW_COLUMNS_in_column_specs170); 
            COLUMNS16_tree = (CommonTree)adaptor.dupNode(COLUMNS16);

            root_1 = (CommonTree)adaptor.becomeRoot(COLUMNS16_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:98:13: ( column_spec[$ins] )+
            int cnt5=0;
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( (LA5_0==COLUMN) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:98:13: column_spec[$ins]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_column_spec_in_column_specs172);
            	    column_spec17=column_spec(ins);

            	    state._fsp--;

            	    adaptor.addChild(root_1, column_spec17.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt5 >= 1 ) break loop5;
                        EarlyExitException eee =
                            new EarlyExitException(5, input);
                        throw eee;
                }
                cnt5++;
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:100:1: column_spec[Insert ins] : ^( COLUMN identifier ( table_name )? ) ;
    public final OracleWalker.column_spec_return column_spec(Insert ins) throws RecognitionException {
        OracleWalker.column_spec_return retval = new OracleWalker.column_spec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree COLUMN18=null;
        OracleWalker.identifier_return identifier19 = null;

        OracleWalker.table_name_return table_name20 = null;


        CommonTree COLUMN18_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:101:2: ( ^( COLUMN identifier ( table_name )? ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:101:3: ^( COLUMN identifier ( table_name )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            COLUMN18=(CommonTree)match(input,COLUMN,FOLLOW_COLUMN_in_column_spec186); 
            COLUMN18_tree = (CommonTree)adaptor.dupNode(COLUMN18);

            root_1 = (CommonTree)adaptor.becomeRoot(COLUMN18_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_column_spec188);
            identifier19=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier19.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:101:23: ( table_name )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==ID) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:101:23: table_name
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_table_name_in_column_spec190);
                    table_name20=table_name();

                    state._fsp--;

                    adaptor.addChild(root_1, table_name20.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		ins.addColumnTandC((table_name20!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(table_name20.start),
              input.getTreeAdaptor().getTokenStopIndex(table_name20.start))):null),(identifier19!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier19.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier19.start))):null));
            	

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:109:1: whereClause[WhereCondition where] : ^( WHERE sqlCondition[$where] ) ;
    public final OracleWalker.whereClause_return whereClause(WhereCondition where) throws RecognitionException {
        OracleWalker.whereClause_return retval = new OracleWalker.whereClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree WHERE21=null;
        OracleWalker.sqlCondition_return sqlCondition22 = null;


        CommonTree WHERE21_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:110:2: ( ^( WHERE sqlCondition[$where] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:110:3: ^( WHERE sqlCondition[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            WHERE21=(CommonTree)match(input,WHERE,FOLLOW_WHERE_in_whereClause209); 
            WHERE21_tree = (CommonTree)adaptor.dupNode(WHERE21);

            root_1 = (CommonTree)adaptor.becomeRoot(WHERE21_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_sqlCondition_in_whereClause211);
            sqlCondition22=sqlCondition(where);

            state._fsp--;

            adaptor.addChild(root_1, sqlCondition22.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:113:1: groupByClause[WhereCondition where] : ^( GROUPBY columnNamesAfterWhere ) ;
    public final OracleWalker.groupByClause_return groupByClause(WhereCondition where) throws RecognitionException {
        OracleWalker.groupByClause_return retval = new OracleWalker.groupByClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree GROUPBY23=null;
        OracleWalker.columnNamesAfterWhere_return columnNamesAfterWhere24 = null;


        CommonTree GROUPBY23_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:114:2: ( ^( GROUPBY columnNamesAfterWhere ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:114:3: ^( GROUPBY columnNamesAfterWhere )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            GROUPBY23=(CommonTree)match(input,GROUPBY,FOLLOW_GROUPBY_in_groupByClause226); 
            GROUPBY23_tree = (CommonTree)adaptor.dupNode(GROUPBY23);

            root_1 = (CommonTree)adaptor.becomeRoot(GROUPBY23_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_columnNamesAfterWhere_in_groupByClause228);
            columnNamesAfterWhere24=columnNamesAfterWhere();

            state._fsp--;

            adaptor.addChild(root_1, columnNamesAfterWhere24.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		where.setGroupByColumns((columnNamesAfterWhere24!=null?columnNamesAfterWhere24.ret:null));
            	

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

    public static class orderByClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orderByClause"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:118:1: orderByClause[WhereCondition where] : ^( ORDERBY columnNamesAfterWhere ) ;
    public final OracleWalker.orderByClause_return orderByClause(WhereCondition where) throws RecognitionException {
        OracleWalker.orderByClause_return retval = new OracleWalker.orderByClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ORDERBY25=null;
        OracleWalker.columnNamesAfterWhere_return columnNamesAfterWhere26 = null;


        CommonTree ORDERBY25_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:119:2: ( ^( ORDERBY columnNamesAfterWhere ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:119:3: ^( ORDERBY columnNamesAfterWhere )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            ORDERBY25=(CommonTree)match(input,ORDERBY,FOLLOW_ORDERBY_in_orderByClause241); 
            ORDERBY25_tree = (CommonTree)adaptor.dupNode(ORDERBY25);

            root_1 = (CommonTree)adaptor.becomeRoot(ORDERBY25_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_columnNamesAfterWhere_in_orderByClause243);
            columnNamesAfterWhere26=columnNamesAfterWhere();

            state._fsp--;

            adaptor.addChild(root_1, columnNamesAfterWhere26.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		where.setOrderByColumns((columnNamesAfterWhere26!=null?columnNamesAfterWhere26.ret:null));
            	

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:123:1: columnNamesAfterWhere returns [List<OrderByEle> ret] : ( columnNameAfterWhere[$ret] )+ ;
    public final OracleWalker.columnNamesAfterWhere_return columnNamesAfterWhere() throws RecognitionException {
        OracleWalker.columnNamesAfterWhere_return retval = new OracleWalker.columnNamesAfterWhere_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.columnNameAfterWhere_return columnNameAfterWhere27 = null;




        	retval.ret = new ArrayList<OrderByEle>();

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:127:2: ( ( columnNameAfterWhere[$ret] )+ )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:127:3: ( columnNameAfterWhere[$ret] )+
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:127:3: ( columnNameAfterWhere[$ret] )+
            int cnt7=0;
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( ((LA7_0>=ASC && LA7_0<=DESC)) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:127:4: columnNameAfterWhere[$ret]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_columnNameAfterWhere_in_columnNamesAfterWhere262);
            	    columnNameAfterWhere27=columnNameAfterWhere(retval.ret);

            	    state._fsp--;

            	    adaptor.addChild(root_0, columnNameAfterWhere27.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:129:1: columnNameAfterWhere[List<OrderByEle> orderByEles] : ( ^( ASC identifier ( table_alias )? ) | ^( DESC identifier ( table_alias )? ) );
    public final OracleWalker.columnNameAfterWhere_return columnNameAfterWhere(List<OrderByEle> orderByEles) throws RecognitionException {
        OracleWalker.columnNameAfterWhere_return retval = new OracleWalker.columnNameAfterWhere_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ASC28=null;
        CommonTree DESC31=null;
        OracleWalker.identifier_return identifier29 = null;

        OracleWalker.table_alias_return table_alias30 = null;

        OracleWalker.identifier_return identifier32 = null;

        OracleWalker.table_alias_return table_alias33 = null;


        CommonTree ASC28_tree=null;
        CommonTree DESC31_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:130:2: ( ^( ASC identifier ( table_alias )? ) | ^( DESC identifier ( table_alias )? ) )
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==ASC) ) {
                alt10=1;
            }
            else if ( (LA10_0==DESC) ) {
                alt10=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;
            }
            switch (alt10) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:130:3: ^( ASC identifier ( table_alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ASC28=(CommonTree)match(input,ASC,FOLLOW_ASC_in_columnNameAfterWhere276); 
                    ASC28_tree = (CommonTree)adaptor.dupNode(ASC28);

                    root_1 = (CommonTree)adaptor.becomeRoot(ASC28_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_identifier_in_columnNameAfterWhere279);
                    identifier29=identifier();

                    state._fsp--;

                    adaptor.addChild(root_1, identifier29.getTree());
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:130:21: ( table_alias )?
                    int alt8=2;
                    int LA8_0 = input.LA(1);

                    if ( (LA8_0==COL_TAB) ) {
                        alt8=1;
                    }
                    switch (alt8) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:130:21: table_alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_table_alias_in_columnNameAfterWhere281);
                            table_alias30=table_alias();

                            state._fsp--;

                            adaptor.addChild(root_1, table_alias30.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		orderByEles.add(new OrderByEle((table_alias30!=null?table_alias30.aText:null),(identifier29!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(identifier29.start),
                      input.getTreeAdaptor().getTokenStopIndex(identifier29.start))):null),true));
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:133:3: ^( DESC identifier ( table_alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    DESC31=(CommonTree)match(input,DESC,FOLLOW_DESC_in_columnNameAfterWhere289); 
                    DESC31_tree = (CommonTree)adaptor.dupNode(DESC31);

                    root_1 = (CommonTree)adaptor.becomeRoot(DESC31_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_identifier_in_columnNameAfterWhere292);
                    identifier32=identifier();

                    state._fsp--;

                    adaptor.addChild(root_1, identifier32.getTree());
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:133:22: ( table_alias )?
                    int alt9=2;
                    int LA9_0 = input.LA(1);

                    if ( (LA9_0==COL_TAB) ) {
                        alt9=1;
                    }
                    switch (alt9) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:133:22: table_alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_table_alias_in_columnNameAfterWhere294);
                            table_alias33=table_alias();

                            state._fsp--;

                            adaptor.addChild(root_1, table_alias33.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		orderByEles.add(new OrderByEle((table_alias33!=null?table_alias33.aText:null),(identifier32!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(identifier32.start),
                      input.getTreeAdaptor().getTokenStopIndex(identifier32.start))):null),false));
                    	

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

    public static class sqlCondition_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "sqlCondition"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:138:1: sqlCondition[WhereCondition where] : ( ^( CONDITION_OR_NOT condition[where.getHolder(),where.getExpGroup(),false] ) | ^( CONDITION_OR condition[where.getHolder(),where.getExpGroup(),false] ) );
    public final OracleWalker.sqlCondition_return sqlCondition(WhereCondition where) throws RecognitionException {
        OracleWalker.sqlCondition_return retval = new OracleWalker.sqlCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree CONDITION_OR_NOT34=null;
        CommonTree CONDITION_OR36=null;
        OracleWalker.condition_return condition35 = null;

        OracleWalker.condition_return condition37 = null;


        CommonTree CONDITION_OR_NOT34_tree=null;
        CommonTree CONDITION_OR36_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:140:2: ( ^( CONDITION_OR_NOT condition[where.getHolder(),where.getExpGroup(),false] ) | ^( CONDITION_OR condition[where.getHolder(),where.getExpGroup(),false] ) )
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0==CONDITION_OR_NOT) ) {
                alt11=1;
            }
            else if ( (LA11_0==CONDITION_OR) ) {
                alt11=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;
            }
            switch (alt11) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:140:3: ^( CONDITION_OR_NOT condition[where.getHolder(),where.getExpGroup(),false] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CONDITION_OR_NOT34=(CommonTree)match(input,CONDITION_OR_NOT,FOLLOW_CONDITION_OR_NOT_in_sqlCondition311); 
                    CONDITION_OR_NOT34_tree = (CommonTree)adaptor.dupNode(CONDITION_OR_NOT34);

                    root_1 = (CommonTree)adaptor.becomeRoot(CONDITION_OR_NOT34_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_condition_in_sqlCondition313);
                    condition35=condition(where.getHolder(), where.getExpGroup(), false);

                    state._fsp--;

                    adaptor.addChild(root_1, condition35.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:141:3: ^( CONDITION_OR condition[where.getHolder(),where.getExpGroup(),false] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CONDITION_OR36=(CommonTree)match(input,CONDITION_OR,FOLLOW_CONDITION_OR_in_sqlCondition320); 
                    CONDITION_OR36_tree = (CommonTree)adaptor.dupNode(CONDITION_OR36);

                    root_1 = (CommonTree)adaptor.becomeRoot(CONDITION_OR36_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_condition_in_sqlCondition322);
                    condition37=condition(where.getHolder(), where.getExpGroup(), false);

                    state._fsp--;

                    adaptor.addChild(root_1, condition37.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:144:1: condition[BindIndexHolder where,ExpressionGroup eg,boolean isPriority] : ( ^( 'OR' (s1= condition[where,orExp,$isPriority] )+ ) | ^( 'AND' ( condition[where,andExp,$isPriority] )+ ) | condition_PAREN[where,$eg] | ^( PRIORITY condition[where,$eg,true] ) );
    public final OracleWalker.condition_return condition(BindIndexHolder where, ExpressionGroup eg, boolean isPriority) throws RecognitionException {
        OracleWalker.condition_return retval = new OracleWalker.condition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree string_literal38=null;
        CommonTree string_literal39=null;
        CommonTree PRIORITY42=null;
        OracleWalker.condition_return s1 = null;

        OracleWalker.condition_return condition40 = null;

        OracleWalker.condition_PAREN_return condition_PAREN41 = null;

        OracleWalker.condition_return condition43 = null;


        CommonTree string_literal38_tree=null;
        CommonTree string_literal39_tree=null;
        CommonTree PRIORITY42_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:148:2: ( ^( 'OR' (s1= condition[where,orExp,$isPriority] )+ ) | ^( 'AND' ( condition[where,andExp,$isPriority] )+ ) | condition_PAREN[where,$eg] | ^( PRIORITY condition[where,$eg,true] ) )
            int alt14=4;
            switch ( input.LA(1) ) {
            case 98:
                {
                alt14=1;
                }
                break;
            case 99:
                {
                alt14=2;
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
                alt14=3;
                }
                break;
            case PRIORITY:
                {
                alt14=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;
            }

            switch (alt14) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:149:2: ^( 'OR' (s1= condition[where,orExp,$isPriority] )+ )
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    		OrExpressionGroup orExp=new OrExpressionGroup();
                    		eg.addExpressionGroup(orExp);
                    	
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    string_literal38=(CommonTree)match(input,98,FOLLOW_98_in_condition343); 
                    string_literal38_tree = (CommonTree)adaptor.dupNode(string_literal38);

                    root_1 = (CommonTree)adaptor.becomeRoot(string_literal38_tree, root_1);



                    match(input, Token.DOWN, null); 
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:152:12: (s1= condition[where,orExp,$isPriority] )+
                    int cnt12=0;
                    loop12:
                    do {
                        int alt12=2;
                        int LA12_0 = input.LA(1);

                        if ( (LA12_0==IN||(LA12_0>=ISNOT && LA12_0<=IS)||(LA12_0>=LIKE && LA12_0<=NOT_LIKE)||LA12_0==PRIORITY||LA12_0==EQ||(LA12_0>=LTH && LA12_0<=GEQ)||(LA12_0>=98 && LA12_0<=99)) ) {
                            alt12=1;
                        }


                        switch (alt12) {
                    	case 1 :
                    	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:152:12: s1= condition[where,orExp,$isPriority]
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    pushFollow(FOLLOW_condition_in_condition347);
                    	    s1=condition(where, orExp, isPriority);

                    	    state._fsp--;

                    	    adaptor.addChild(root_1, s1.getTree());

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt12 >= 1 ) break loop12;
                                EarlyExitException eee =
                                    new EarlyExitException(12, input);
                                throw eee;
                        }
                        cnt12++;
                    } while (true);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:154:2: ^( 'AND' ( condition[where,andExp,$isPriority] )+ )
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    		ExpressionGroup andExp=new ExpressionGroup();
                    		eg.addExpressionGroup(andExp);
                    	
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    string_literal39=(CommonTree)match(input,99,FOLLOW_99_in_condition359); 
                    string_literal39_tree = (CommonTree)adaptor.dupNode(string_literal39);

                    root_1 = (CommonTree)adaptor.becomeRoot(string_literal39_tree, root_1);



                    match(input, Token.DOWN, null); 
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:157:11: ( condition[where,andExp,$isPriority] )+
                    int cnt13=0;
                    loop13:
                    do {
                        int alt13=2;
                        int LA13_0 = input.LA(1);

                        if ( (LA13_0==IN||(LA13_0>=ISNOT && LA13_0<=IS)||(LA13_0>=LIKE && LA13_0<=NOT_LIKE)||LA13_0==PRIORITY||LA13_0==EQ||(LA13_0>=LTH && LA13_0<=GEQ)||(LA13_0>=98 && LA13_0<=99)) ) {
                            alt13=1;
                        }


                        switch (alt13) {
                    	case 1 :
                    	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:157:11: condition[where,andExp,$isPriority]
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    pushFollow(FOLLOW_condition_in_condition361);
                    	    condition40=condition(where, andExp, isPriority);

                    	    state._fsp--;

                    	    adaptor.addChild(root_1, condition40.getTree());

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt13 >= 1 ) break loop13;
                                EarlyExitException eee =
                                    new EarlyExitException(13, input);
                                throw eee;
                        }
                        cnt13++;
                    } while (true);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:162:3: condition_PAREN[where,$eg]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_condition_PAREN_in_condition372);
                    condition_PAREN41=condition_PAREN(where, eg);

                    state._fsp--;

                    adaptor.addChild(root_0, condition_PAREN41.getTree());

                    }
                    break;
                case 4 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:163:3: ^( PRIORITY condition[where,$eg,true] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    PRIORITY42=(CommonTree)match(input,PRIORITY,FOLLOW_PRIORITY_in_condition378); 
                    PRIORITY42_tree = (CommonTree)adaptor.dupNode(PRIORITY42);

                    root_1 = (CommonTree)adaptor.becomeRoot(PRIORITY42_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_condition_in_condition380);
                    condition43=condition(where, eg, true);

                    state._fsp--;

                    adaptor.addChild(root_1, condition43.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:183:1: condition_PAREN[BindIndexHolder where,ExpressionGroup exp] : ( condition_expr[$where,$exp] )+ ;
    public final OracleWalker.condition_PAREN_return condition_PAREN(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        OracleWalker.condition_PAREN_return retval = new OracleWalker.condition_PAREN_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.condition_expr_return condition_expr44 = null;



        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:184:2: ( ( condition_expr[$where,$exp] )+ )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:184:3: ( condition_expr[$where,$exp] )+
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:184:3: ( condition_expr[$where,$exp] )+
            int cnt15=0;
            loop15:
            do {
                int alt15=2;
                alt15 = dfa15.predict(input);
                switch (alt15) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:184:3: condition_expr[$where,$exp]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_condition_expr_in_condition_PAREN395);
            	    condition_expr44=condition_expr(where, exp);

            	    state._fsp--;

            	    adaptor.addChild(root_0, condition_expr44.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:186:1: condition_expr[BindIndexHolder where,ExpressionGroup exp] : ( comparisonCondition[$where,$exp] | inCondition[$where,$exp] | isCondition[$where,$exp] | likeCondition[$where,$exp] );
    public final OracleWalker.condition_expr_return condition_expr(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        OracleWalker.condition_expr_return retval = new OracleWalker.condition_expr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.comparisonCondition_return comparisonCondition45 = null;

        OracleWalker.inCondition_return inCondition46 = null;

        OracleWalker.isCondition_return isCondition47 = null;

        OracleWalker.likeCondition_return likeCondition48 = null;



        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:187:2: ( comparisonCondition[$where,$exp] | inCondition[$where,$exp] | isCondition[$where,$exp] | likeCondition[$where,$exp] )
            int alt16=4;
            switch ( input.LA(1) ) {
            case EQ:
            case LTH:
            case GTH:
            case NOT_EQ:
            case LEQ:
            case GEQ:
                {
                alt16=1;
                }
                break;
            case IN:
                {
                alt16=2;
                }
                break;
            case ISNOT:
            case IS:
                {
                alt16=3;
                }
                break;
            case LIKE:
            case NOT_LIKE:
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:187:4: comparisonCondition[$where,$exp]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_comparisonCondition_in_condition_expr408);
                    comparisonCondition45=comparisonCondition(where, exp);

                    state._fsp--;

                    adaptor.addChild(root_0, comparisonCondition45.getTree());

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:188:4: inCondition[$where,$exp]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_inCondition_in_condition_expr414);
                    inCondition46=inCondition(where, exp);

                    state._fsp--;

                    adaptor.addChild(root_0, inCondition46.getTree());

                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:189:4: isCondition[$where,$exp]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_isCondition_in_condition_expr421);
                    isCondition47=isCondition(where, exp);

                    state._fsp--;

                    adaptor.addChild(root_0, isCondition47.getTree());

                    }
                    break;
                case 4 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:190:4: likeCondition[$where,$exp]
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_likeCondition_in_condition_expr428);
                    likeCondition48=likeCondition(where, exp);

                    state._fsp--;

                    adaptor.addChild(root_0, likeCondition48.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:193:1: betweenCondition : ( ^( NOT_BETWEEN between_and ) | ^( BETWEEN between_and ) );
    public final OracleWalker.betweenCondition_return betweenCondition() throws RecognitionException {
        OracleWalker.betweenCondition_return retval = new OracleWalker.betweenCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree NOT_BETWEEN49=null;
        CommonTree BETWEEN51=null;
        OracleWalker.between_and_return between_and50 = null;

        OracleWalker.between_and_return between_and52 = null;


        CommonTree NOT_BETWEEN49_tree=null;
        CommonTree BETWEEN51_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:194:2: ( ^( NOT_BETWEEN between_and ) | ^( BETWEEN between_and ) )
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==NOT_BETWEEN) ) {
                alt17=1;
            }
            else if ( (LA17_0==BETWEEN) ) {
                alt17=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 17, 0, input);

                throw nvae;
            }
            switch (alt17) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:194:3: ^( NOT_BETWEEN between_and )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NOT_BETWEEN49=(CommonTree)match(input,NOT_BETWEEN,FOLLOW_NOT_BETWEEN_in_betweenCondition440); 
                    NOT_BETWEEN49_tree = (CommonTree)adaptor.dupNode(NOT_BETWEEN49);

                    root_1 = (CommonTree)adaptor.becomeRoot(NOT_BETWEEN49_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        _last = (CommonTree)input.LT(1);
                        pushFollow(FOLLOW_between_and_in_betweenCondition442);
                        between_and50=between_and();

                        state._fsp--;

                        adaptor.addChild(root_1, between_and50.getTree());

                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:195:3: ^( BETWEEN between_and )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BETWEEN51=(CommonTree)match(input,BETWEEN,FOLLOW_BETWEEN_in_betweenCondition448); 
                    BETWEEN51_tree = (CommonTree)adaptor.dupNode(BETWEEN51);

                    root_1 = (CommonTree)adaptor.becomeRoot(BETWEEN51_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        _last = (CommonTree)input.LT(1);
                        pushFollow(FOLLOW_between_and_in_betweenCondition450);
                        between_and52=between_and();

                        state._fsp--;

                        adaptor.addChild(root_1, between_and52.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:197:1: between_and : ;
    public final OracleWalker.between_and_return between_and() throws RecognitionException {
        OracleWalker.between_and_return retval = new OracleWalker.between_and_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:198:2: ()
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:199:2: 
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:200:1: likeCondition[BindIndexHolder where,ExpressionGroup exp] : ( ^( NOT_LIKE expr[$where] left_cond[$where] ) | ^( LIKE expr[$where] left_cond[$where] ) );
    public final OracleWalker.likeCondition_return likeCondition(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        OracleWalker.likeCondition_return retval = new OracleWalker.likeCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree NOT_LIKE53=null;
        CommonTree LIKE56=null;
        OracleWalker.expr_return expr54 = null;

        OracleWalker.left_cond_return left_cond55 = null;

        OracleWalker.expr_return expr57 = null;

        OracleWalker.left_cond_return left_cond58 = null;


        CommonTree NOT_LIKE53_tree=null;
        CommonTree LIKE56_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:201:2: ( ^( NOT_LIKE expr[$where] left_cond[$where] ) | ^( LIKE expr[$where] left_cond[$where] ) )
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==NOT_LIKE) ) {
                alt18=1;
            }
            else if ( (LA18_0==LIKE) ) {
                alt18=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 18, 0, input);

                throw nvae;
            }
            switch (alt18) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:201:3: ^( NOT_LIKE expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NOT_LIKE53=(CommonTree)match(input,NOT_LIKE,FOLLOW_NOT_LIKE_in_likeCondition471); 
                    NOT_LIKE53_tree = (CommonTree)adaptor.dupNode(NOT_LIKE53);

                    root_1 = (CommonTree)adaptor.becomeRoot(NOT_LIKE53_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_likeCondition473);
                    expr54=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr54.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_likeCondition476);
                    left_cond55=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond55.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    			NotLike notlike=new NotLike();
                    		notlike.setLeft((left_cond55!=null?left_cond55.ret:null));
                    		notlike.setRight((expr54!=null?expr54.valueObj:null));
                    		exp.addExpression(notlike);
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:207:3: ^( LIKE expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    LIKE56=(CommonTree)match(input,LIKE,FOLLOW_LIKE_in_likeCondition484); 
                    LIKE56_tree = (CommonTree)adaptor.dupNode(LIKE56);

                    root_1 = (CommonTree)adaptor.becomeRoot(LIKE56_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_likeCondition486);
                    expr57=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr57.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_likeCondition489);
                    left_cond58=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond58.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		Like like=new Like();
                    		like.setLeft((left_cond58!=null?left_cond58.ret:null));
                    		like.setRight((expr57!=null?expr57.valueObj:null));
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:215:1: isCondition[BindIndexHolder where,ExpressionGroup exp] : ( ^( ISNOT NULL left_cond[$where] ) | ^( IS NULL left_cond[$where] ) );
    public final OracleWalker.isCondition_return isCondition(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        OracleWalker.isCondition_return retval = new OracleWalker.isCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ISNOT59=null;
        CommonTree NULL60=null;
        CommonTree IS62=null;
        CommonTree NULL63=null;
        OracleWalker.left_cond_return left_cond61 = null;

        OracleWalker.left_cond_return left_cond64 = null;


        CommonTree ISNOT59_tree=null;
        CommonTree NULL60_tree=null;
        CommonTree IS62_tree=null;
        CommonTree NULL63_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:216:2: ( ^( ISNOT NULL left_cond[$where] ) | ^( IS NULL left_cond[$where] ) )
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==ISNOT) ) {
                alt19=1;
            }
            else if ( (LA19_0==IS) ) {
                alt19=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 19, 0, input);

                throw nvae;
            }
            switch (alt19) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:216:3: ^( ISNOT NULL left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ISNOT59=(CommonTree)match(input,ISNOT,FOLLOW_ISNOT_in_isCondition505); 
                    ISNOT59_tree = (CommonTree)adaptor.dupNode(ISNOT59);

                    root_1 = (CommonTree)adaptor.becomeRoot(ISNOT59_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    NULL60=(CommonTree)match(input,NULL,FOLLOW_NULL_in_isCondition507); 
                    NULL60_tree = (CommonTree)adaptor.dupNode(NULL60);

                    adaptor.addChild(root_1, NULL60_tree);

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_isCondition509);
                    left_cond61=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond61.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		IsNot arg=new IsNot();
                    		arg.setLeft((left_cond61!=null?left_cond61.ret:null));
                    		arg.setRight(null);
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:222:3: ^( IS NULL left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    IS62=(CommonTree)match(input,IS,FOLLOW_IS_in_isCondition517); 
                    IS62_tree = (CommonTree)adaptor.dupNode(IS62);

                    root_1 = (CommonTree)adaptor.becomeRoot(IS62_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    NULL63=(CommonTree)match(input,NULL,FOLLOW_NULL_in_isCondition519); 
                    NULL63_tree = (CommonTree)adaptor.dupNode(NULL63);

                    adaptor.addChild(root_1, NULL63_tree);

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_isCondition521);
                    left_cond64=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond64.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		Is arg=new Is();
                    		arg.setLeft((left_cond64!=null?left_cond64.ret:null));
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:231:1: comparisonCondition[BindIndexHolder where,ExpressionGroup exp] : ( ^( EQ expr[$where] left_cond[$where] ) | ^( NOT_EQ expr[$where] left_cond[$where] ) | ^( LTH expr[$where] left_cond[$where] ) | ^( GTH expr[$where] left_cond[$where] ) | ^( LEQ expr[$where] left_cond[$where] ) | ^( GEQ expr[$where] left_cond[$where] ) );
    public final OracleWalker.comparisonCondition_return comparisonCondition(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        OracleWalker.comparisonCondition_return retval = new OracleWalker.comparisonCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree EQ65=null;
        CommonTree NOT_EQ68=null;
        CommonTree LTH71=null;
        CommonTree GTH74=null;
        CommonTree LEQ77=null;
        CommonTree GEQ80=null;
        OracleWalker.expr_return expr66 = null;

        OracleWalker.left_cond_return left_cond67 = null;

        OracleWalker.expr_return expr69 = null;

        OracleWalker.left_cond_return left_cond70 = null;

        OracleWalker.expr_return expr72 = null;

        OracleWalker.left_cond_return left_cond73 = null;

        OracleWalker.expr_return expr75 = null;

        OracleWalker.left_cond_return left_cond76 = null;

        OracleWalker.expr_return expr78 = null;

        OracleWalker.left_cond_return left_cond79 = null;

        OracleWalker.expr_return expr81 = null;

        OracleWalker.left_cond_return left_cond82 = null;


        CommonTree EQ65_tree=null;
        CommonTree NOT_EQ68_tree=null;
        CommonTree LTH71_tree=null;
        CommonTree GTH74_tree=null;
        CommonTree LEQ77_tree=null;
        CommonTree GEQ80_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:232:2: ( ^( EQ expr[$where] left_cond[$where] ) | ^( NOT_EQ expr[$where] left_cond[$where] ) | ^( LTH expr[$where] left_cond[$where] ) | ^( GTH expr[$where] left_cond[$where] ) | ^( LEQ expr[$where] left_cond[$where] ) | ^( GEQ expr[$where] left_cond[$where] ) )
            int alt20=6;
            switch ( input.LA(1) ) {
            case EQ:
                {
                alt20=1;
                }
                break;
            case NOT_EQ:
                {
                alt20=2;
                }
                break;
            case LTH:
                {
                alt20=3;
                }
                break;
            case GTH:
                {
                alt20=4;
                }
                break;
            case LEQ:
                {
                alt20=5;
                }
                break;
            case GEQ:
                {
                alt20=6;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 20, 0, input);

                throw nvae;
            }

            switch (alt20) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:232:3: ^( EQ expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    EQ65=(CommonTree)match(input,EQ,FOLLOW_EQ_in_comparisonCondition538); 
                    EQ65_tree = (CommonTree)adaptor.dupNode(EQ65);

                    root_1 = (CommonTree)adaptor.becomeRoot(EQ65_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition540);
                    expr66=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr66.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition543);
                    left_cond67=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond67.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		Equivalent arg=new Equivalent();
                    		arg.setLeft((left_cond67!=null?left_cond67.ret:null));
                    		arg.setRight((expr66!=null?expr66.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:239:3: ^( NOT_EQ expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NOT_EQ68=(CommonTree)match(input,NOT_EQ,FOLLOW_NOT_EQ_in_comparisonCondition553); 
                    NOT_EQ68_tree = (CommonTree)adaptor.dupNode(NOT_EQ68);

                    root_1 = (CommonTree)adaptor.becomeRoot(NOT_EQ68_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition555);
                    expr69=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr69.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition558);
                    left_cond70=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond70.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		NotEquivalent arg=new NotEquivalent();
                    		arg.setLeft((left_cond70!=null?left_cond70.ret:null));
                    		arg.setRight((expr69!=null?expr69.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:246:3: ^( LTH expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    LTH71=(CommonTree)match(input,LTH,FOLLOW_LTH_in_comparisonCondition568); 
                    LTH71_tree = (CommonTree)adaptor.dupNode(LTH71);

                    root_1 = (CommonTree)adaptor.becomeRoot(LTH71_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition570);
                    expr72=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr72.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition573);
                    left_cond73=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond73.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		LessThan arg=new LessThan();
                    		arg.setLeft((left_cond73!=null?left_cond73.ret:null));
                    		arg.setRight((expr72!=null?expr72.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 4 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:253:3: ^( GTH expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    GTH74=(CommonTree)match(input,GTH,FOLLOW_GTH_in_comparisonCondition583); 
                    GTH74_tree = (CommonTree)adaptor.dupNode(GTH74);

                    root_1 = (CommonTree)adaptor.becomeRoot(GTH74_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition585);
                    expr75=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr75.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition588);
                    left_cond76=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond76.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		GreaterThan arg=new GreaterThan();
                    		arg.setLeft((left_cond76!=null?left_cond76.ret:null));
                    		arg.setRight((expr75!=null?expr75.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 5 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:260:3: ^( LEQ expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    LEQ77=(CommonTree)match(input,LEQ,FOLLOW_LEQ_in_comparisonCondition598); 
                    LEQ77_tree = (CommonTree)adaptor.dupNode(LEQ77);

                    root_1 = (CommonTree)adaptor.becomeRoot(LEQ77_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition600);
                    expr78=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr78.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition603);
                    left_cond79=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond79.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		LessThanOrEquivalent arg=new LessThanOrEquivalent();
                    		arg.setLeft((left_cond79!=null?left_cond79.ret:null));
                    		arg.setRight((expr78!=null?expr78.valueObj:null));
                    		exp.addExpression(arg);
                    	

                    }
                    break;
                case 6 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:267:3: ^( GEQ expr[$where] left_cond[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    GEQ80=(CommonTree)match(input,GEQ,FOLLOW_GEQ_in_comparisonCondition613); 
                    GEQ80_tree = (CommonTree)adaptor.dupNode(GEQ80);

                    root_1 = (CommonTree)adaptor.becomeRoot(GEQ80_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_comparisonCondition615);
                    expr81=expr(where);

                    state._fsp--;

                    adaptor.addChild(root_1, expr81.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_left_cond_in_comparisonCondition618);
                    left_cond82=left_cond(where);

                    state._fsp--;

                    adaptor.addChild(root_1, left_cond82.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		GreaterThanOrEquivalent arg=new GreaterThanOrEquivalent();
                    		arg.setLeft((left_cond82!=null?left_cond82.ret:null));
                    		arg.setRight((expr81!=null?expr81.valueObj:null));
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:276:1: left_cond[BindIndexHolder where] returns [Object ret] : ^( CONDITION_LEFT expr[$where] ) ;
    public final OracleWalker.left_cond_return left_cond(BindIndexHolder where) throws RecognitionException {
        OracleWalker.left_cond_return retval = new OracleWalker.left_cond_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree CONDITION_LEFT83=null;
        OracleWalker.expr_return expr84 = null;


        CommonTree CONDITION_LEFT83_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:278:2: ( ^( CONDITION_LEFT expr[$where] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:278:3: ^( CONDITION_LEFT expr[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            CONDITION_LEFT83=(CommonTree)match(input,CONDITION_LEFT,FOLLOW_CONDITION_LEFT_in_left_cond639); 
            CONDITION_LEFT83_tree = (CommonTree)adaptor.dupNode(CONDITION_LEFT83);

            root_1 = (CommonTree)adaptor.becomeRoot(CONDITION_LEFT83_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_expr_in_left_cond641);
            expr84=expr(where);

            state._fsp--;

            adaptor.addChild(root_1, expr84.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		retval.ret =(expr84!=null?expr84.valueObj:null);
            	

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:283:1: in_list[BindIndexHolder where] returns [List list] : ^( IN_LISTS inCondition_expr_adds[$where] ) ;
    public final OracleWalker.in_list_return in_list(BindIndexHolder where) throws RecognitionException {
        OracleWalker.in_list_return retval = new OracleWalker.in_list_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree IN_LISTS85=null;
        OracleWalker.inCondition_expr_adds_return inCondition_expr_adds86 = null;


        CommonTree IN_LISTS85_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:284:2: ( ^( IN_LISTS inCondition_expr_adds[$where] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:284:3: ^( IN_LISTS inCondition_expr_adds[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            IN_LISTS85=(CommonTree)match(input,IN_LISTS,FOLLOW_IN_LISTS_in_in_list660); 
            IN_LISTS85_tree = (CommonTree)adaptor.dupNode(IN_LISTS85);

            root_1 = (CommonTree)adaptor.becomeRoot(IN_LISTS85_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_inCondition_expr_adds_in_in_list662);
            inCondition_expr_adds86=inCondition_expr_adds(where);

            state._fsp--;

            adaptor.addChild(root_1, inCondition_expr_adds86.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		retval.list =(inCondition_expr_adds86!=null?inCondition_expr_adds86.list:null);
            	

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:289:1: inCondition[BindIndexHolder where,ExpressionGroup exp] : ^( IN (not= 'NOT' )? ( subquery[$where] )? ( in_list[$where] )? left_cond[$where] ) ;
    public final OracleWalker.inCondition_return inCondition(BindIndexHolder where, ExpressionGroup exp) throws RecognitionException {
        OracleWalker.inCondition_return retval = new OracleWalker.inCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree not=null;
        CommonTree IN87=null;
        OracleWalker.subquery_return subquery88 = null;

        OracleWalker.in_list_return in_list89 = null;

        OracleWalker.left_cond_return left_cond90 = null;


        CommonTree not_tree=null;
        CommonTree IN87_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:290:2: ( ^( IN (not= 'NOT' )? ( subquery[$where] )? ( in_list[$where] )? left_cond[$where] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:290:3: ^( IN (not= 'NOT' )? ( subquery[$where] )? ( in_list[$where] )? left_cond[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            IN87=(CommonTree)match(input,IN,FOLLOW_IN_in_inCondition680); 
            IN87_tree = (CommonTree)adaptor.dupNode(IN87);

            root_1 = (CommonTree)adaptor.becomeRoot(IN87_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:290:11: (not= 'NOT' )?
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( (LA21_0==97) ) {
                alt21=1;
            }
            switch (alt21) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:290:11: not= 'NOT'
                    {
                    _last = (CommonTree)input.LT(1);
                    not=(CommonTree)match(input,97,FOLLOW_97_in_inCondition684); 
                    not_tree = (CommonTree)adaptor.dupNode(not);

                    adaptor.addChild(root_1, not_tree);


                    }
                    break;

            }

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:290:19: ( subquery[$where] )?
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0==SUBQUERY) ) {
                alt22=1;
            }
            switch (alt22) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:290:19: subquery[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_subquery_in_inCondition687);
                    subquery88=subquery(where);

                    state._fsp--;

                    adaptor.addChild(root_1, subquery88.getTree());

                    }
                    break;

            }

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:290:37: ( in_list[$where] )?
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( (LA23_0==IN_LISTS) ) {
                alt23=1;
            }
            switch (alt23) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:290:37: in_list[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_in_list_in_inCondition691);
                    in_list89=in_list(where);

                    state._fsp--;

                    adaptor.addChild(root_1, in_list89.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_left_cond_in_inCondition696);
            left_cond90=left_cond(where);

            state._fsp--;

            adaptor.addChild(root_1, left_cond90.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		if((not!=null?not.getText():null)!=null){
            			if((subquery88!=null?subquery88.subselect:null)!=null){
            				NotInExpression arg=new NotInExpression();
            				arg.setLeft((left_cond90!=null?left_cond90.ret:null));
            				arg.setRight((subquery88!=null?subquery88.subselect:null));
            				exp.addExpression(arg);
            			}
            			else if((in_list89!=null?in_list89.list:null)!=null){
            					NotInExpression arg=new NotInExpression();
            					arg.setLeft((left_cond90!=null?left_cond90.ret:null));
            					arg.setRight((in_list89!=null?in_list89.list:null));
            					exp.addExpression(arg);
            			}
            		}else{
            			if((subquery88!=null?subquery88.subselect:null)!=null){
            				InExpression arg=new InExpression();
            				arg.setLeft((left_cond90!=null?left_cond90.ret:null));
            				arg.setRight((subquery88!=null?subquery88.subselect:null));
            				exp.addExpression(arg);}
            			else if((in_list89!=null?in_list89.list:null)!=null){
            					InExpression arg=new InExpression();
            					arg.setLeft((left_cond90!=null?left_cond90.ret:null));
            					arg.setRight((in_list89!=null?in_list89.list:null));
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:326:1: inCondition_expr_adds[BindIndexHolder where] returns [List list] : ( expr_add[$where] )+ ;
    public final OracleWalker.inCondition_expr_adds_return inCondition_expr_adds(BindIndexHolder where) throws RecognitionException {
        OracleWalker.inCondition_expr_adds_return retval = new OracleWalker.inCondition_expr_adds_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.expr_add_return expr_add91 = null;



        retval.list =new ArrayList();
        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:327:31: ( ( expr_add[$where] )+ )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:328:2: ( expr_add[$where] )+
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:328:2: ( expr_add[$where] )+
            int cnt24=0;
            loop24:
            do {
                int alt24=2;
                int LA24_0 = input.LA(1);

                if ( (LA24_0==COLUMN||LA24_0==QUTED_STR||LA24_0==COLUMNAST||LA24_0==CONSIST||(LA24_0>=CAST && LA24_0<=POSITIVE)||(LA24_0>=BITOR && LA24_0<=DIVIDE)||LA24_0==ID||(LA24_0>=N && LA24_0<=NUMBER)||LA24_0==104||LA24_0==110||(LA24_0>=120 && LA24_0<=121)) ) {
                    alt24=1;
                }


                switch (alt24) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:328:3: expr_add[$where]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_expr_add_in_inCondition_expr_adds730);
            	    expr_add91=expr_add(where);

            	    state._fsp--;

            	    adaptor.addChild(root_0, expr_add91.getTree());

            	    		retval.list.add((expr_add91!=null?expr_add91.valueObjExpr:null));
            	    	

            	    }
            	    break;

            	default :
            	    if ( cnt24 >= 1 ) break loop24;
                        EarlyExitException eee =
                            new EarlyExitException(24, input);
                        throw eee;
                }
                cnt24++;
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:335:1: expr[BindIndexHolder where] returns [Object valueObj] : ( expr_add[$where] | subquery[$where] ) ;
    public final OracleWalker.expr_return expr(BindIndexHolder where) throws RecognitionException {
        OracleWalker.expr_return retval = new OracleWalker.expr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.expr_add_return expr_add92 = null;

        OracleWalker.subquery_return subquery93 = null;



        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:336:2: ( ( expr_add[$where] | subquery[$where] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:336:3: ( expr_add[$where] | subquery[$where] )
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:336:3: ( expr_add[$where] | subquery[$where] )
            int alt25=2;
            int LA25_0 = input.LA(1);

            if ( (LA25_0==COLUMN||LA25_0==QUTED_STR||LA25_0==COLUMNAST||LA25_0==CONSIST||(LA25_0>=CAST && LA25_0<=POSITIVE)||(LA25_0>=BITOR && LA25_0<=DIVIDE)||LA25_0==ID||(LA25_0>=N && LA25_0<=NUMBER)||LA25_0==104||LA25_0==110||(LA25_0>=120 && LA25_0<=121)) ) {
                alt25=1;
            }
            else if ( (LA25_0==SUBQUERY) ) {
                alt25=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 25, 0, input);

                throw nvae;
            }
            switch (alt25) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:336:4: expr_add[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr755);
                    expr_add92=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_0, expr_add92.getTree());
                    retval.valueObj =(expr_add92!=null?expr_add92.valueObjExpr:null);

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:337:3: subquery[$where]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_subquery_in_expr762);
                    subquery93=subquery(where);

                    state._fsp--;

                    adaptor.addChild(root_0, subquery93.getTree());
                    retval.valueObj =(subquery93!=null?subquery93.subselect:null);

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:341:1: expr_add[BindIndexHolder where] returns [Object valueObjExpr] : ( ^( NEGATIVE s1= expr_add[$where] ) | ^( POSITIVE s1= expr_add[$where] ) | ^( PLUS s1= expr_add[$where] s2= expr_add[$where] ) | ^( MINUS s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITOR s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITAND s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITXOR s1= expr_add[$where] s2= expr_add[$where] ) | ^( SHIFTLEFT s1= expr_add[$where] s2= expr_add[$where] ) | ^( SHIFTRIGHT s1= expr_add[$where] s2= expr_add[$where] ) | ^( ASTERISK s1= expr_add[$where] s2= expr_add[$where] ) | ^( DIVIDE s1= expr_add[$where] s2= expr_add[$where] ) | N | NUMBER | boolean_literal | 'NULL' | '?' | ^( QUTED_STR quoted_string ) | ^( COLUMN identifier ( table_name )? ) | ^( COLUMNAST ASTERISK ) | ^( ID ( expr[$where] )* ) | ^( CONSIST ID ) | ^( CAST ( expr[$where] )* ID ) );
    public final OracleWalker.expr_add_return expr_add(BindIndexHolder where) throws RecognitionException {
        OracleWalker.expr_add_return retval = new OracleWalker.expr_add_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree NEGATIVE94=null;
        CommonTree POSITIVE95=null;
        CommonTree PLUS96=null;
        CommonTree MINUS97=null;
        CommonTree BITOR98=null;
        CommonTree BITAND99=null;
        CommonTree BITXOR100=null;
        CommonTree SHIFTLEFT101=null;
        CommonTree SHIFTRIGHT102=null;
        CommonTree ASTERISK103=null;
        CommonTree DIVIDE104=null;
        CommonTree N105=null;
        CommonTree NUMBER106=null;
        CommonTree string_literal108=null;
        CommonTree char_literal109=null;
        CommonTree QUTED_STR110=null;
        CommonTree COLUMN112=null;
        CommonTree COLUMNAST115=null;
        CommonTree ASTERISK116=null;
        CommonTree ID117=null;
        CommonTree CONSIST119=null;
        CommonTree ID120=null;
        CommonTree CAST121=null;
        CommonTree ID123=null;
        OracleWalker.expr_add_return s1 = null;

        OracleWalker.expr_add_return s2 = null;

        OracleWalker.boolean_literal_return boolean_literal107 = null;

        OracleWalker.quoted_string_return quoted_string111 = null;

        OracleWalker.identifier_return identifier113 = null;

        OracleWalker.table_name_return table_name114 = null;

        OracleWalker.expr_return expr118 = null;

        OracleWalker.expr_return expr122 = null;


        CommonTree NEGATIVE94_tree=null;
        CommonTree POSITIVE95_tree=null;
        CommonTree PLUS96_tree=null;
        CommonTree MINUS97_tree=null;
        CommonTree BITOR98_tree=null;
        CommonTree BITAND99_tree=null;
        CommonTree BITXOR100_tree=null;
        CommonTree SHIFTLEFT101_tree=null;
        CommonTree SHIFTRIGHT102_tree=null;
        CommonTree ASTERISK103_tree=null;
        CommonTree DIVIDE104_tree=null;
        CommonTree N105_tree=null;
        CommonTree NUMBER106_tree=null;
        CommonTree string_literal108_tree=null;
        CommonTree char_literal109_tree=null;
        CommonTree QUTED_STR110_tree=null;
        CommonTree COLUMN112_tree=null;
        CommonTree COLUMNAST115_tree=null;
        CommonTree ASTERISK116_tree=null;
        CommonTree ID117_tree=null;
        CommonTree CONSIST119_tree=null;
        CommonTree ID120_tree=null;
        CommonTree CAST121_tree=null;
        CommonTree ID123_tree=null;


        List<Object> list=new ArrayList<Object>(2);

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:345:2: ( ^( NEGATIVE s1= expr_add[$where] ) | ^( POSITIVE s1= expr_add[$where] ) | ^( PLUS s1= expr_add[$where] s2= expr_add[$where] ) | ^( MINUS s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITOR s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITAND s1= expr_add[$where] s2= expr_add[$where] ) | ^( BITXOR s1= expr_add[$where] s2= expr_add[$where] ) | ^( SHIFTLEFT s1= expr_add[$where] s2= expr_add[$where] ) | ^( SHIFTRIGHT s1= expr_add[$where] s2= expr_add[$where] ) | ^( ASTERISK s1= expr_add[$where] s2= expr_add[$where] ) | ^( DIVIDE s1= expr_add[$where] s2= expr_add[$where] ) | N | NUMBER | boolean_literal | 'NULL' | '?' | ^( QUTED_STR quoted_string ) | ^( COLUMN identifier ( table_name )? ) | ^( COLUMNAST ASTERISK ) | ^( ID ( expr[$where] )* ) | ^( CONSIST ID ) | ^( CAST ( expr[$where] )* ID ) )
            int alt29=22;
            switch ( input.LA(1) ) {
            case NEGATIVE:
                {
                alt29=1;
                }
                break;
            case POSITIVE:
                {
                alt29=2;
                }
                break;
            case PLUS:
                {
                alt29=3;
                }
                break;
            case MINUS:
                {
                alt29=4;
                }
                break;
            case BITOR:
                {
                alt29=5;
                }
                break;
            case BITAND:
                {
                alt29=6;
                }
                break;
            case BITXOR:
                {
                alt29=7;
                }
                break;
            case SHIFTLEFT:
                {
                alt29=8;
                }
                break;
            case SHIFTRIGHT:
                {
                alt29=9;
                }
                break;
            case ASTERISK:
                {
                alt29=10;
                }
                break;
            case DIVIDE:
                {
                alt29=11;
                }
                break;
            case N:
                {
                alt29=12;
                }
                break;
            case NUMBER:
                {
                alt29=13;
                }
                break;
            case 120:
            case 121:
                {
                alt29=14;
                }
                break;
            case 104:
                {
                alt29=15;
                }
                break;
            case 110:
                {
                alt29=16;
                }
                break;
            case QUTED_STR:
                {
                alt29=17;
                }
                break;
            case COLUMN:
                {
                alt29=18;
                }
                break;
            case COLUMNAST:
                {
                alt29=19;
                }
                break;
            case ID:
                {
                alt29=20;
                }
                break;
            case CONSIST:
                {
                alt29=21;
                }
                break;
            case CAST:
                {
                alt29=22;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 29, 0, input);

                throw nvae;
            }

            switch (alt29) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:345:3: ^( NEGATIVE s1= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NEGATIVE94=(CommonTree)match(input,NEGATIVE,FOLLOW_NEGATIVE_in_expr_add790); 
                    NEGATIVE94_tree = (CommonTree)adaptor.dupNode(NEGATIVE94);

                    root_1 = (CommonTree)adaptor.becomeRoot(NEGATIVE94_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add794);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	retval.valueObjExpr =((BigDecimal)(s1!=null?s1.valueObjExpr:null)).negate();
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:348:3: ^( POSITIVE s1= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    POSITIVE95=(CommonTree)match(input,POSITIVE,FOLLOW_POSITIVE_in_expr_add803); 
                    POSITIVE95_tree = (CommonTree)adaptor.dupNode(POSITIVE95);

                    root_1 = (CommonTree)adaptor.becomeRoot(POSITIVE95_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add807);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	retval.valueObjExpr =(s1!=null?s1.valueObjExpr:null);
                    	

                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:351:3: ^( PLUS s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    PLUS96=(CommonTree)match(input,PLUS,FOLLOW_PLUS_in_expr_add816); 
                    PLUS96_tree = (CommonTree)adaptor.dupNode(PLUS96);

                    root_1 = (CommonTree)adaptor.becomeRoot(PLUS96_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add820);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add825);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:358:3: ^( MINUS s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    MINUS97=(CommonTree)match(input,MINUS,FOLLOW_MINUS_in_expr_add834); 
                    MINUS97_tree = (CommonTree)adaptor.dupNode(MINUS97);

                    root_1 = (CommonTree)adaptor.becomeRoot(MINUS97_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add838);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add843);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:365:3: ^( BITOR s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BITOR98=(CommonTree)match(input,BITOR,FOLLOW_BITOR_in_expr_add851); 
                    BITOR98_tree = (CommonTree)adaptor.dupNode(BITOR98);

                    root_1 = (CommonTree)adaptor.becomeRoot(BITOR98_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add855);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add860);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:372:3: ^( BITAND s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BITAND99=(CommonTree)match(input,BITAND,FOLLOW_BITAND_in_expr_add868); 
                    BITAND99_tree = (CommonTree)adaptor.dupNode(BITAND99);

                    root_1 = (CommonTree)adaptor.becomeRoot(BITAND99_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add872);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add877);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:379:3: ^( BITXOR s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BITXOR100=(CommonTree)match(input,BITXOR,FOLLOW_BITXOR_in_expr_add885); 
                    BITXOR100_tree = (CommonTree)adaptor.dupNode(BITXOR100);

                    root_1 = (CommonTree)adaptor.becomeRoot(BITXOR100_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add889);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add894);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:386:3: ^( SHIFTLEFT s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    SHIFTLEFT101=(CommonTree)match(input,SHIFTLEFT,FOLLOW_SHIFTLEFT_in_expr_add902); 
                    SHIFTLEFT101_tree = (CommonTree)adaptor.dupNode(SHIFTLEFT101);

                    root_1 = (CommonTree)adaptor.becomeRoot(SHIFTLEFT101_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add906);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add911);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:393:3: ^( SHIFTRIGHT s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    SHIFTRIGHT102=(CommonTree)match(input,SHIFTRIGHT,FOLLOW_SHIFTRIGHT_in_expr_add919); 
                    SHIFTRIGHT102_tree = (CommonTree)adaptor.dupNode(SHIFTRIGHT102);

                    root_1 = (CommonTree)adaptor.becomeRoot(SHIFTRIGHT102_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add923);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add928);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:400:3: ^( ASTERISK s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ASTERISK103=(CommonTree)match(input,ASTERISK,FOLLOW_ASTERISK_in_expr_add936); 
                    ASTERISK103_tree = (CommonTree)adaptor.dupNode(ASTERISK103);

                    root_1 = (CommonTree)adaptor.becomeRoot(ASTERISK103_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add940);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add945);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:407:3: ^( DIVIDE s1= expr_add[$where] s2= expr_add[$where] )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    DIVIDE104=(CommonTree)match(input,DIVIDE,FOLLOW_DIVIDE_in_expr_add953); 
                    DIVIDE104_tree = (CommonTree)adaptor.dupNode(DIVIDE104);

                    root_1 = (CommonTree)adaptor.becomeRoot(DIVIDE104_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add957);
                    s1=expr_add(where);

                    state._fsp--;

                    adaptor.addChild(root_1, s1.getTree());
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_add_in_expr_add962);
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
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:414:3: N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    N105=(CommonTree)match(input,N,FOLLOW_N_in_expr_add969); 
                    N105_tree = (CommonTree)adaptor.dupNode(N105);

                    adaptor.addChild(root_0, N105_tree);

                    retval.valueObjExpr =new BigDecimal((N105!=null?N105.getText():null));

                    }
                    break;
                case 13 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:415:3: NUMBER
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    NUMBER106=(CommonTree)match(input,NUMBER,FOLLOW_NUMBER_in_expr_add975); 
                    NUMBER106_tree = (CommonTree)adaptor.dupNode(NUMBER106);

                    adaptor.addChild(root_0, NUMBER106_tree);

                    retval.valueObjExpr =new BigDecimal((NUMBER106!=null?NUMBER106.getText():null));

                    }
                    break;
                case 14 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:416:3: boolean_literal
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_boolean_literal_in_expr_add980);
                    boolean_literal107=boolean_literal();

                    state._fsp--;

                    adaptor.addChild(root_0, boolean_literal107.getTree());

                    }
                    break;
                case 15 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:417:3: 'NULL'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    string_literal108=(CommonTree)match(input,104,FOLLOW_104_in_expr_add984); 
                    string_literal108_tree = (CommonTree)adaptor.dupNode(string_literal108);

                    adaptor.addChild(root_0, string_literal108_tree);

                    retval.valueObjExpr =null;

                    }
                    break;
                case 16 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:418:3: '?'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    char_literal109=(CommonTree)match(input,110,FOLLOW_110_in_expr_add990); 
                    char_literal109_tree = (CommonTree)adaptor.dupNode(char_literal109);

                    adaptor.addChild(root_0, char_literal109_tree);


                    	BindVar val=new BindVar(where.selfAddAndGet());
                    	retval.valueObjExpr =val;
                    	

                    }
                    break;
                case 17 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:422:3: ^( QUTED_STR quoted_string )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    QUTED_STR110=(CommonTree)match(input,QUTED_STR,FOLLOW_QUTED_STR_in_expr_add996); 
                    QUTED_STR110_tree = (CommonTree)adaptor.dupNode(QUTED_STR110);

                    root_1 = (CommonTree)adaptor.becomeRoot(QUTED_STR110_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_quoted_string_in_expr_add998);
                    quoted_string111=quoted_string();

                    state._fsp--;

                    adaptor.addChild(root_1, quoted_string111.getTree());

                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }

                    retval.valueObjExpr =(quoted_string111!=null?quoted_string111.aText:null);

                    }
                    break;
                case 18 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:423:3: ^( COLUMN identifier ( table_name )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    COLUMN112=(CommonTree)match(input,COLUMN,FOLLOW_COLUMN_in_expr_add1005); 
                    COLUMN112_tree = (CommonTree)adaptor.dupNode(COLUMN112);

                    root_1 = (CommonTree)adaptor.becomeRoot(COLUMN112_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_identifier_in_expr_add1007);
                    identifier113=identifier();

                    state._fsp--;

                    adaptor.addChild(root_1, identifier113.getTree());
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:423:23: ( table_name )?
                    int alt26=2;
                    int LA26_0 = input.LA(1);

                    if ( (LA26_0==ID) ) {
                        alt26=1;
                    }
                    switch (alt26) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:423:23: table_name
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_table_name_in_expr_add1009);
                            table_name114=table_name();

                            state._fsp--;

                            adaptor.addChild(root_1, table_name114.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	Column col=new ColumnImp((table_name114!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(table_name114.start),
                      input.getTreeAdaptor().getTokenStopIndex(table_name114.start))):null),(identifier113!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(identifier113.start),
                      input.getTreeAdaptor().getTokenStopIndex(identifier113.start))):null),null);
                    	retval.valueObjExpr =col;
                    	

                    }
                    break;
                case 19 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:428:3: ^( COLUMNAST ASTERISK )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    COLUMNAST115=(CommonTree)match(input,COLUMNAST,FOLLOW_COLUMNAST_in_expr_add1019); 
                    COLUMNAST115_tree = (CommonTree)adaptor.dupNode(COLUMNAST115);

                    root_1 = (CommonTree)adaptor.becomeRoot(COLUMNAST115_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    ASTERISK116=(CommonTree)match(input,ASTERISK,FOLLOW_ASTERISK_in_expr_add1021); 
                    ASTERISK116_tree = (CommonTree)adaptor.dupNode(ASTERISK116);

                    adaptor.addChild(root_1, ASTERISK116_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	Column col=new ColumnImp(null,(ASTERISK116!=null?ASTERISK116.getText():null),null);
                    	retval.valueObjExpr =col;
                    	

                    }
                    break;
                case 20 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:433:3: ^( ID ( expr[$where] )* )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ID117=(CommonTree)match(input,ID,FOLLOW_ID_in_expr_add1030); 
                    ID117_tree = (CommonTree)adaptor.dupNode(ID117);

                    root_1 = (CommonTree)adaptor.becomeRoot(ID117_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:434:2: ( expr[$where] )*
                        loop27:
                        do {
                            int alt27=2;
                            int LA27_0 = input.LA(1);

                            if ( ((LA27_0>=SUBQUERY && LA27_0<=COLUMN)||LA27_0==QUTED_STR||LA27_0==COLUMNAST||LA27_0==CONSIST||(LA27_0>=CAST && LA27_0<=POSITIVE)||(LA27_0>=BITOR && LA27_0<=DIVIDE)||LA27_0==ID||(LA27_0>=N && LA27_0<=NUMBER)||LA27_0==104||LA27_0==110||(LA27_0>=120 && LA27_0<=121)) ) {
                                alt27=1;
                            }


                            switch (alt27) {
                        	case 1 :
                        	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:434:3: expr[$where]
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    pushFollow(FOLLOW_expr_in_expr_add1035);
                        	    expr118=expr(where);

                        	    state._fsp--;

                        	    adaptor.addChild(root_1, expr118.getTree());
                        	    list.add((expr118!=null?expr118.valueObj:null));

                        	    }
                        	    break;

                        	default :
                        	    break loop27;
                            }
                        } while (true);


                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	Function func=functionMap.get(((ID117!=null?ID117.getText():null)).toUpperCase());
                    	func.setValue(list);
                    	retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 21 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:439:3: ^( CONSIST ID )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CONSIST119=(CommonTree)match(input,CONSIST,FOLLOW_CONSIST_in_expr_add1046); 
                    CONSIST119_tree = (CommonTree)adaptor.dupNode(CONSIST119);

                    root_1 = (CommonTree)adaptor.becomeRoot(CONSIST119_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    ID120=(CommonTree)match(input,ID,FOLLOW_ID_in_expr_add1048); 
                    ID120_tree = (CommonTree)adaptor.dupNode(ID120);

                    adaptor.addChild(root_1, ID120_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	Function func=consistStr.get(((ID120!=null?ID120.getText():null)).toUpperCase());
                    	retval.valueObjExpr =func;
                    	

                    }
                    break;
                case 22 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:443:3: ^( CAST ( expr[$where] )* ID )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CAST121=(CommonTree)match(input,CAST,FOLLOW_CAST_in_expr_add1055); 
                    CAST121_tree = (CommonTree)adaptor.dupNode(CAST121);

                    root_1 = (CommonTree)adaptor.becomeRoot(CAST121_tree, root_1);



                    match(input, Token.DOWN, null); 
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:443:10: ( expr[$where] )*
                    loop28:
                    do {
                        int alt28=2;
                        int LA28_0 = input.LA(1);

                        if ( (LA28_0==ID) ) {
                            int LA28_1 = input.LA(2);

                            if ( (LA28_1==DOWN) ) {
                                alt28=1;
                            }


                        }
                        else if ( ((LA28_0>=SUBQUERY && LA28_0<=COLUMN)||LA28_0==QUTED_STR||LA28_0==COLUMNAST||LA28_0==CONSIST||(LA28_0>=CAST && LA28_0<=POSITIVE)||(LA28_0>=BITOR && LA28_0<=DIVIDE)||(LA28_0>=N && LA28_0<=NUMBER)||LA28_0==104||LA28_0==110||(LA28_0>=120 && LA28_0<=121)) ) {
                            alt28=1;
                        }


                        switch (alt28) {
                    	case 1 :
                    	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:443:11: expr[$where]
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    pushFollow(FOLLOW_expr_in_expr_add1058);
                    	    expr122=expr(where);

                    	    state._fsp--;

                    	    adaptor.addChild(root_1, expr122.getTree());
                    	    list.add((expr122!=null?expr122.valueObj:null));

                    	    }
                    	    break;

                    	default :
                    	    break loop28;
                        }
                    } while (true);

                    _last = (CommonTree)input.LT(1);
                    ID123=(CommonTree)match(input,ID,FOLLOW_ID_in_expr_add1064); 
                    ID123_tree = (CommonTree)adaptor.dupNode(ID123);

                    adaptor.addChild(root_1, ID123_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	//list.add("vartabletype");
                    	Cast func= new Cast();
                    	func.setType((ID123!=null?ID123.getText():null));
                    	func.setValue(list);
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

    public static class value_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "value"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:451:1: value : ( N | NUMBER | '?' );
    public final OracleWalker.value_return value() throws RecognitionException {
        OracleWalker.value_return retval = new OracleWalker.value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree set124=null;

        CommonTree set124_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:451:7: ( N | NUMBER | '?' )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            set124=(CommonTree)input.LT(1);
            if ( (input.LA(1)>=N && input.LA(1)<=NUMBER)||input.LA(1)==110 ) {
                input.consume();

                set124_tree = (CommonTree)adaptor.dupNode(set124);

                adaptor.addChild(root_0, set124_tree);

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:456:1: boolean_literal returns [Object valueObj] : (s1= 'TRUE' | s1= 'FALSE' );
    public final OracleWalker.boolean_literal_return boolean_literal() throws RecognitionException {
        OracleWalker.boolean_literal_return retval = new OracleWalker.boolean_literal_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree s1=null;

        CommonTree s1_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:457:2: (s1= 'TRUE' | s1= 'FALSE' )
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==120) ) {
                alt30=1;
            }
            else if ( (LA30_0==121) ) {
                alt30=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 30, 0, input);

                throw nvae;
            }
            switch (alt30) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:457:3: s1= 'TRUE'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    s1=(CommonTree)match(input,120,FOLLOW_120_in_boolean_literal1098); 
                    s1_tree = (CommonTree)adaptor.dupNode(s1);

                    adaptor.addChild(root_0, s1_tree);

                    retval.valueObj =Boolean.parseBoolean((s1!=null?s1.getText():null));

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:458:4: s1= 'FALSE'
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    s1=(CommonTree)match(input,121,FOLLOW_121_in_boolean_literal1107); 
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:462:1: select_list[Select select] : ^( SELECT_LIST ( displayed_column[$select] )+ ) ;
    public final OracleWalker.select_list_return select_list(Select select) throws RecognitionException {
        OracleWalker.select_list_return retval = new OracleWalker.select_list_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SELECT_LIST125=null;
        OracleWalker.displayed_column_return displayed_column126 = null;


        CommonTree SELECT_LIST125_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:463:2: ( ^( SELECT_LIST ( displayed_column[$select] )+ ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:463:3: ^( SELECT_LIST ( displayed_column[$select] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SELECT_LIST125=(CommonTree)match(input,SELECT_LIST,FOLLOW_SELECT_LIST_in_select_list1123); 
            SELECT_LIST125_tree = (CommonTree)adaptor.dupNode(SELECT_LIST125);

            root_1 = (CommonTree)adaptor.becomeRoot(SELECT_LIST125_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:463:17: ( displayed_column[$select] )+
            int cnt31=0;
            loop31:
            do {
                int alt31=2;
                int LA31_0 = input.LA(1);

                if ( (LA31_0==COLUMN||LA31_0==CONSIST||LA31_0==ID) ) {
                    alt31=1;
                }


                switch (alt31) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:463:17: displayed_column[$select]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_displayed_column_in_select_list1125);
            	    displayed_column126=displayed_column(select);

            	    state._fsp--;

            	    adaptor.addChild(root_1, displayed_column126.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt31 >= 1 ) break loop31;
                        EarlyExitException eee =
                            new EarlyExitException(31, input);
                        throw eee;
                }
                cnt31++;
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
    // $ANTLR end "select_list"

    public static class fromClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fromClause"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:465:1: fromClause[Select select] : ^( TABLENAMES ( table[$select] )+ ) ;
    public final OracleWalker.fromClause_return fromClause(Select select) throws RecognitionException {
        OracleWalker.fromClause_return retval = new OracleWalker.fromClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree TABLENAMES127=null;
        OracleWalker.table_return table128 = null;


        CommonTree TABLENAMES127_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:466:2: ( ^( TABLENAMES ( table[$select] )+ ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:466:3: ^( TABLENAMES ( table[$select] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            TABLENAMES127=(CommonTree)match(input,TABLENAMES,FOLLOW_TABLENAMES_in_fromClause1140); 
            TABLENAMES127_tree = (CommonTree)adaptor.dupNode(TABLENAMES127);

            root_1 = (CommonTree)adaptor.becomeRoot(TABLENAMES127_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:466:16: ( table[$select] )+
            int cnt32=0;
            loop32:
            do {
                int alt32=2;
                int LA32_0 = input.LA(1);

                if ( (LA32_0==TABLENAME) ) {
                    alt32=1;
                }


                switch (alt32) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:466:16: table[$select]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_table_in_fromClause1142);
            	    table128=table(select);

            	    state._fsp--;

            	    adaptor.addChild(root_1, table128.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt32 >= 1 ) break loop32;
                        EarlyExitException eee =
                            new EarlyExitException(32, input);
                        throw eee;
                }
                cnt32++;
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:468:1: join_clause[DMLCommon common] returns [JoinClause joinClause] : ^( JOIN table_name ( alias )? temp1= join_column temp2= join_column ( join_type )? ) ;
    public final OracleWalker.join_clause_return join_clause(DMLCommon common) throws RecognitionException {
        OracleWalker.join_clause_return retval = new OracleWalker.join_clause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree JOIN129=null;
        OracleWalker.join_column_return temp1 = null;

        OracleWalker.join_column_return temp2 = null;

        OracleWalker.table_name_return table_name130 = null;

        OracleWalker.alias_return alias131 = null;

        OracleWalker.join_type_return join_type132 = null;


        CommonTree JOIN129_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:469:2: ( ^( JOIN table_name ( alias )? temp1= join_column temp2= join_column ( join_type )? ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:469:3: ^( JOIN table_name ( alias )? temp1= join_column temp2= join_column ( join_type )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            JOIN129=(CommonTree)match(input,JOIN,FOLLOW_JOIN_in_join_clause1160); 
            JOIN129_tree = (CommonTree)adaptor.dupNode(JOIN129);

            root_1 = (CommonTree)adaptor.becomeRoot(JOIN129_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_table_name_in_join_clause1162);
            table_name130=table_name();

            state._fsp--;

            adaptor.addChild(root_1, table_name130.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:469:21: ( alias )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==AS) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:469:21: alias
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_alias_in_join_clause1164);
                    alias131=alias();

                    state._fsp--;

                    adaptor.addChild(root_1, alias131.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_join_column_in_join_clause1169);
            temp1=join_column();

            state._fsp--;

            adaptor.addChild(root_1, temp1.getTree());
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_join_column_in_join_clause1173);
            temp2=join_column();

            state._fsp--;

            adaptor.addChild(root_1, temp2.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:469:64: ( join_type )?
            int alt34=2;
            int LA34_0 = input.LA(1);

            if ( ((LA34_0>=INNER && LA34_0<=CROSS)) ) {
                alt34=1;
            }
            switch (alt34) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:469:64: join_type
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_join_type_in_join_clause1175);
                    join_type132=join_type();

                    state._fsp--;

                    adaptor.addChild(root_1, join_type132.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		retval.joinClause = new JoinClause();
            		TableNameImp tableName = new TableNameImp();
            		tableName.setTablename((table_name130!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(table_name130.start),
              input.getTreeAdaptor().getTokenStopIndex(table_name130.start))):null));
            		tableName.setAlias((alias131!=null?alias131.aliText:null));
            		retval.joinClause.setTableName(tableName);
            		retval.joinClause.setLeftCondition((temp1!=null?temp1.col:null));
            		retval.joinClause.setRightCondition((temp2!=null?temp2.col:null));
            		retval.joinClause.setJoinType((join_type132!=null?join_type132.joinType:null));
            	

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

    public static class join_type_return extends TreeRuleReturnScope {
        public JOIN_TYPE joinType;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "join_type"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:482:1: join_type returns [JOIN_TYPE joinType] : ( INNER | LEFT ( outer )? | RIGHT ( outer )? | FULL ( outer )? | UNION | CROSS );
    public final OracleWalker.join_type_return join_type() throws RecognitionException {
        OracleWalker.join_type_return retval = new OracleWalker.join_type_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree INNER133=null;
        CommonTree LEFT134=null;
        CommonTree RIGHT136=null;
        CommonTree FULL138=null;
        CommonTree UNION140=null;
        CommonTree CROSS141=null;
        OracleWalker.outer_return outer135 = null;

        OracleWalker.outer_return outer137 = null;

        OracleWalker.outer_return outer139 = null;


        CommonTree INNER133_tree=null;
        CommonTree LEFT134_tree=null;
        CommonTree RIGHT136_tree=null;
        CommonTree FULL138_tree=null;
        CommonTree UNION140_tree=null;
        CommonTree CROSS141_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:483:2: ( INNER | LEFT ( outer )? | RIGHT ( outer )? | FULL ( outer )? | UNION | CROSS )
            int alt38=6;
            switch ( input.LA(1) ) {
            case INNER:
                {
                alt38=1;
                }
                break;
            case LEFT:
                {
                alt38=2;
                }
                break;
            case RIGHT:
                {
                alt38=3;
                }
                break;
            case FULL:
                {
                alt38=4;
                }
                break;
            case UNION:
                {
                alt38=5;
                }
                break;
            case CROSS:
                {
                alt38=6;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 38, 0, input);

                throw nvae;
            }

            switch (alt38) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:483:3: INNER
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    INNER133=(CommonTree)match(input,INNER,FOLLOW_INNER_in_join_type1194); 
                    INNER133_tree = (CommonTree)adaptor.dupNode(INNER133);

                    adaptor.addChild(root_0, INNER133_tree);


                    		retval.joinType = JOIN_TYPE.INNER;
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:487:3: LEFT ( outer )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    LEFT134=(CommonTree)match(input,LEFT,FOLLOW_LEFT_in_join_type1202); 
                    LEFT134_tree = (CommonTree)adaptor.dupNode(LEFT134);

                    adaptor.addChild(root_0, LEFT134_tree);

                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:487:8: ( outer )?
                    int alt35=2;
                    int LA35_0 = input.LA(1);

                    if ( (LA35_0==OUTER) ) {
                        alt35=1;
                    }
                    switch (alt35) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:487:8: outer
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_outer_in_join_type1204);
                            outer135=outer();

                            state._fsp--;

                            adaptor.addChild(root_0, outer135.getTree());

                            }
                            break;

                    }


                    		boolean outter = (outer135!=null?outer135.outter:false);
                    		if(outter){
                    			retval.joinType = JOIN_TYPE.LEFT_OUTER;
                    		}else{
                    			retval.joinType = JOIN_TYPE.LEFT;
                    		}
                    	

                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:496:3: RIGHT ( outer )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    RIGHT136=(CommonTree)match(input,RIGHT,FOLLOW_RIGHT_in_join_type1212); 
                    RIGHT136_tree = (CommonTree)adaptor.dupNode(RIGHT136);

                    adaptor.addChild(root_0, RIGHT136_tree);

                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:496:9: ( outer )?
                    int alt36=2;
                    int LA36_0 = input.LA(1);

                    if ( (LA36_0==OUTER) ) {
                        alt36=1;
                    }
                    switch (alt36) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:496:9: outer
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_outer_in_join_type1214);
                            outer137=outer();

                            state._fsp--;

                            adaptor.addChild(root_0, outer137.getTree());

                            }
                            break;

                    }


                    		boolean outter = (outer137!=null?outer137.outter:false);
                    		if(outter){
                    			retval.joinType = JOIN_TYPE.RIGHT_OUTER;
                    		}else{
                    			retval.joinType = JOIN_TYPE.RIGHT;
                    		}
                    	

                    }
                    break;
                case 4 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:505:3: FULL ( outer )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    FULL138=(CommonTree)match(input,FULL,FOLLOW_FULL_in_join_type1222); 
                    FULL138_tree = (CommonTree)adaptor.dupNode(FULL138);

                    adaptor.addChild(root_0, FULL138_tree);

                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:505:8: ( outer )?
                    int alt37=2;
                    int LA37_0 = input.LA(1);

                    if ( (LA37_0==OUTER) ) {
                        alt37=1;
                    }
                    switch (alt37) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:505:8: outer
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_outer_in_join_type1224);
                            outer139=outer();

                            state._fsp--;

                            adaptor.addChild(root_0, outer139.getTree());

                            }
                            break;

                    }


                    		boolean outter = (outer139!=null?outer139.outter:false);
                    		if(outter){
                    			retval.joinType = JOIN_TYPE.FULL_OUTER;
                    		}else{
                    			retval.joinType = JOIN_TYPE.FULL;
                    		}
                    	

                    }
                    break;
                case 5 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:514:3: UNION
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    UNION140=(CommonTree)match(input,UNION,FOLLOW_UNION_in_join_type1233); 
                    UNION140_tree = (CommonTree)adaptor.dupNode(UNION140);

                    adaptor.addChild(root_0, UNION140_tree);


                    		retval.joinType = JOIN_TYPE.UNION;
                    	

                    }
                    break;
                case 6 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:518:3: CROSS
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    CROSS141=(CommonTree)match(input,CROSS,FOLLOW_CROSS_in_join_type1241); 
                    CROSS141_tree = (CommonTree)adaptor.dupNode(CROSS141);

                    adaptor.addChild(root_0, CROSS141_tree);


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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:523:1: outer returns [boolean outter] : OUTER ;
    public final OracleWalker.outer_return outer() throws RecognitionException {
        OracleWalker.outer_return retval = new OracleWalker.outer_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree OUTER142=null;

        CommonTree OUTER142_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:524:2: ( OUTER )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:524:3: OUTER
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            OUTER142=(CommonTree)match(input,OUTER,FOLLOW_OUTER_in_outer1258); 
            OUTER142_tree = (CommonTree)adaptor.dupNode(OUTER142);

            adaptor.addChild(root_0, OUTER142_tree);


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

    public static class join_column_return extends TreeRuleReturnScope {
        public Column col;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "join_column"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:529:1: join_column returns [Column col] : ^( COLUMN identifier ( table_name )? ) ;
    public final OracleWalker.join_column_return join_column() throws RecognitionException {
        OracleWalker.join_column_return retval = new OracleWalker.join_column_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree COLUMN143=null;
        OracleWalker.identifier_return identifier144 = null;

        OracleWalker.table_name_return table_name145 = null;


        CommonTree COLUMN143_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:530:2: ( ^( COLUMN identifier ( table_name )? ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:530:3: ^( COLUMN identifier ( table_name )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            COLUMN143=(CommonTree)match(input,COLUMN,FOLLOW_COLUMN_in_join_column1274); 
            COLUMN143_tree = (CommonTree)adaptor.dupNode(COLUMN143);

            root_1 = (CommonTree)adaptor.becomeRoot(COLUMN143_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_join_column1276);
            identifier144=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier144.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:530:23: ( table_name )?
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==ID) ) {
                alt39=1;
            }
            switch (alt39) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:530:23: table_name
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_table_name_in_join_column1278);
                    table_name145=table_name();

                    state._fsp--;

                    adaptor.addChild(root_1, table_name145.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		    retval.col =new ColumnImp((table_name145!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(table_name145.start),
              input.getTreeAdaptor().getTokenStopIndex(table_name145.start))):null),(identifier144!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier144.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier144.start))):null),null);
            			
            		

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

    public static class table_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "table"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:536:1: table[DMLCommon common] : ^( TABLENAME table_spec[$common] ( join_clause[$common] )? ) ;
    public final OracleWalker.table_return table(DMLCommon common) throws RecognitionException {
        OracleWalker.table_return retval = new OracleWalker.table_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree TABLENAME146=null;
        OracleWalker.table_spec_return table_spec147 = null;

        OracleWalker.join_clause_return join_clause148 = null;


        CommonTree TABLENAME146_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:537:2: ( ^( TABLENAME table_spec[$common] ( join_clause[$common] )? ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:537:3: ^( TABLENAME table_spec[$common] ( join_clause[$common] )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            TABLENAME146=(CommonTree)match(input,TABLENAME,FOLLOW_TABLENAME_in_table1298); 
            TABLENAME146_tree = (CommonTree)adaptor.dupNode(TABLENAME146);

            root_1 = (CommonTree)adaptor.becomeRoot(TABLENAME146_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_table_spec_in_table1300);
            table_spec147=table_spec(common);

            state._fsp--;

            adaptor.addChild(root_1, table_spec147.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:537:35: ( join_clause[$common] )?
            int alt40=2;
            int LA40_0 = input.LA(1);

            if ( (LA40_0==JOIN) ) {
                alt40=1;
            }
            switch (alt40) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:537:35: join_clause[$common]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_join_clause_in_table1303);
                    join_clause148=join_clause(common);

                    state._fsp--;

                    adaptor.addChild(root_1, join_clause148.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		TableName tableName = (table_spec147!=null?table_spec147.tableName:null);
            		tableName.setJoinClause((join_clause148!=null?join_clause148.joinClause:null));
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:544:1: tables[DMLCommon common] : ^( TABLENAMES ( table[$common] )+ ) ;
    public final OracleWalker.tables_return tables(DMLCommon common) throws RecognitionException {
        OracleWalker.tables_return retval = new OracleWalker.tables_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree TABLENAMES149=null;
        OracleWalker.table_return table150 = null;


        CommonTree TABLENAMES149_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:545:2: ( ^( TABLENAMES ( table[$common] )+ ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:545:3: ^( TABLENAMES ( table[$common] )+ )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            TABLENAMES149=(CommonTree)match(input,TABLENAMES,FOLLOW_TABLENAMES_in_tables1320); 
            TABLENAMES149_tree = (CommonTree)adaptor.dupNode(TABLENAMES149);

            root_1 = (CommonTree)adaptor.becomeRoot(TABLENAMES149_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:545:16: ( table[$common] )+
            int cnt41=0;
            loop41:
            do {
                int alt41=2;
                int LA41_0 = input.LA(1);

                if ( (LA41_0==TABLENAME) ) {
                    alt41=1;
                }


                switch (alt41) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:545:16: table[$common]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_table_in_tables1322);
            	    table150=table(common);

            	    state._fsp--;

            	    adaptor.addChild(root_1, table150.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt41 >= 1 ) break loop41;
                        EarlyExitException eee =
                            new EarlyExitException(41, input);
                        throw eee;
                }
                cnt41++;
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:547:1: table_spec[DMLCommon common] returns [TableName tableName] : ( ( schema_name )? table_name ( alias )? | subquery[$common.getIndexHolder()] ( alias )? | ^( ID ( expr[$common.getIndexHolder()] ) ) ( alias )? );
    public final OracleWalker.table_spec_return table_spec(DMLCommon common) throws RecognitionException {
        OracleWalker.table_spec_return retval = new OracleWalker.table_spec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ID156=null;
        OracleWalker.schema_name_return schema_name151 = null;

        OracleWalker.table_name_return table_name152 = null;

        OracleWalker.alias_return alias153 = null;

        OracleWalker.subquery_return subquery154 = null;

        OracleWalker.alias_return alias155 = null;

        OracleWalker.expr_return expr157 = null;

        OracleWalker.alias_return alias158 = null;


        CommonTree ID156_tree=null;


        	List<Object> list=new ArrayList<Object>(2);
        	
        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:551:2: ( ( schema_name )? table_name ( alias )? | subquery[$common.getIndexHolder()] ( alias )? | ^( ID ( expr[$common.getIndexHolder()] ) ) ( alias )? )
            int alt46=3;
            int LA46_0 = input.LA(1);

            if ( (LA46_0==ID) ) {
                int LA46_1 = input.LA(2);

                if ( (LA46_1==DOWN) ) {
                    alt46=3;
                }
                else if ( (LA46_1==UP||LA46_1==AS||LA46_1==JOIN||LA46_1==ID) ) {
                    alt46=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 46, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA46_0==SUBQUERY) ) {
                alt46=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 46, 0, input);

                throw nvae;
            }
            switch (alt46) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:551:3: ( schema_name )? table_name ( alias )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:551:3: ( schema_name )?
                    int alt42=2;
                    int LA42_0 = input.LA(1);

                    if ( (LA42_0==ID) ) {
                        int LA42_1 = input.LA(2);

                        if ( (LA42_1==ID) ) {
                            alt42=1;
                        }
                    }
                    switch (alt42) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:551:5: schema_name
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_schema_name_in_table_spec1346);
                            schema_name151=schema_name();

                            state._fsp--;

                            adaptor.addChild(root_0, schema_name151.getTree());

                            }
                            break;

                    }

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_table_name_in_table_spec1350);
                    table_name152=table_name();

                    state._fsp--;

                    adaptor.addChild(root_0, table_name152.getTree());
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:551:31: ( alias )?
                    int alt43=2;
                    int LA43_0 = input.LA(1);

                    if ( (LA43_0==AS) ) {
                        alt43=1;
                    }
                    switch (alt43) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:551:31: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_table_spec1353);
                            alias153=alias();

                            state._fsp--;

                            adaptor.addChild(root_0, alias153.getTree());

                            }
                            break;

                    }


                    		retval.tableName = getTableNameAndSchemaName((table_name152!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(table_name152.start),
                      input.getTreeAdaptor().getTokenStopIndex(table_name152.start))):null),(schema_name151!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(schema_name151.start),
                      input.getTreeAdaptor().getTokenStopIndex(schema_name151.start))):null),(alias153!=null?alias153.aliText:null), true);
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:555:3: subquery[$common.getIndexHolder()] ( alias )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_subquery_in_table_spec1361);
                    subquery154=subquery(common.getIndexHolder());

                    state._fsp--;

                    adaptor.addChild(root_0, subquery154.getTree());
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:555:38: ( alias )?
                    int alt44=2;
                    int LA44_0 = input.LA(1);

                    if ( (LA44_0==AS) ) {
                        alt44=1;
                    }
                    switch (alt44) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:555:38: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_table_spec1364);
                            alias155=alias();

                            state._fsp--;

                            adaptor.addChild(root_0, alias155.getTree());

                            }
                            break;

                    }


                    		retval.tableName = getTableSubQuery((subquery154!=null?subquery154.subselect:null),(alias155!=null?alias155.aliText:null), true);

                    	

                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:560:3: ^( ID ( expr[$common.getIndexHolder()] ) ) ( alias )?
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ID156=(CommonTree)match(input,ID,FOLLOW_ID_in_table_spec1373); 
                    ID156_tree = (CommonTree)adaptor.dupNode(ID156);

                    root_1 = (CommonTree)adaptor.becomeRoot(ID156_tree, root_1);



                    match(input, Token.DOWN, null); 
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:560:8: ( expr[$common.getIndexHolder()] )
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:560:9: expr[$common.getIndexHolder()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_expr_in_table_spec1376);
                    expr157=expr(common.getIndexHolder());

                    state._fsp--;

                    adaptor.addChild(root_1, expr157.getTree());
                    list.add((expr157!=null?expr157.valueObj:null));

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }

                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:560:70: ( alias )?
                    int alt45=2;
                    int LA45_0 = input.LA(1);

                    if ( (LA45_0==AS) ) {
                        alt45=1;
                    }
                    switch (alt45) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:560:70: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_table_spec1383);
                            alias158=alias();

                            state._fsp--;

                            adaptor.addChild(root_0, alias158.getTree());

                            }
                            break;

                    }


                    		Function func=functionMap.get(((ID156!=null?ID156.getText():null)).toUpperCase());
                    		func.setValue(list);
                    		retval.tableName = getTableFunction(func,(alias158!=null?alias158.aliText:null), true);
                    	

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:568:1: schema_name : identifier ;
    public final OracleWalker.schema_name_return schema_name() throws RecognitionException {
        OracleWalker.schema_name_return retval = new OracleWalker.schema_name_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.identifier_return identifier159 = null;



        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:569:2: ( identifier )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:569:3: identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_schema_name1398);
            identifier159=identifier();

            state._fsp--;

            adaptor.addChild(root_0, identifier159.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:571:1: subquery[BindIndexHolder holder] returns [Select subselect] : ^( SUBQUERY select_command[$holder] ) ;
    public final OracleWalker.subquery_return subquery(BindIndexHolder holder) throws RecognitionException {
        OracleWalker.subquery_return retval = new OracleWalker.subquery_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SUBQUERY160=null;
        OracleWalker.select_command_return select_command161 = null;


        CommonTree SUBQUERY160_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:571:60: ( ^( SUBQUERY select_command[$holder] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:572:2: ^( SUBQUERY select_command[$holder] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SUBQUERY160=(CommonTree)match(input,SUBQUERY,FOLLOW_SUBQUERY_in_subquery1413); 
            SUBQUERY160_tree = (CommonTree)adaptor.dupNode(SUBQUERY160);

            root_1 = (CommonTree)adaptor.becomeRoot(SUBQUERY160_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_select_command_in_subquery1415);
            select_command161=select_command(holder);

            state._fsp--;

            adaptor.addChild(root_1, select_command161.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            	retval.subselect =(select_command161!=null?select_command161.select:null);
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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:578:1: table_name : identifier ;
    public final OracleWalker.table_name_return table_name() throws RecognitionException {
        OracleWalker.table_name_return retval = new OracleWalker.table_name_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.identifier_return identifier162 = null;



        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:579:2: ( identifier )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:579:3: identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_table_name1430);
            identifier162=identifier();

            state._fsp--;

            adaptor.addChild(root_0, identifier162.getTree());

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

    public static class displayed_column_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "displayed_column"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:581:1: displayed_column[Select select] : ( ^( ID ( expr[$select.getIndexHolder()] ( alias )? )* ) | ^( CONSIST ID ( alias )? ) | ^( COLUMN ( table_alias )? columnAnt ( alias )? ) );
    public final OracleWalker.displayed_column_return displayed_column(Select select) throws RecognitionException {
        OracleWalker.displayed_column_return retval = new OracleWalker.displayed_column_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ID163=null;
        CommonTree CONSIST166=null;
        CommonTree ID167=null;
        CommonTree COLUMN169=null;
        OracleWalker.expr_return expr164 = null;

        OracleWalker.alias_return alias165 = null;

        OracleWalker.alias_return alias168 = null;

        OracleWalker.table_alias_return table_alias170 = null;

        OracleWalker.columnAnt_return columnAnt171 = null;

        OracleWalker.alias_return alias172 = null;


        CommonTree ID163_tree=null;
        CommonTree CONSIST166_tree=null;
        CommonTree ID167_tree=null;
        CommonTree COLUMN169_tree=null;


        List<Object> list=new ArrayList<Object>(10);

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:585:2: ( ^( ID ( expr[$select.getIndexHolder()] ( alias )? )* ) | ^( CONSIST ID ( alias )? ) | ^( COLUMN ( table_alias )? columnAnt ( alias )? ) )
            int alt52=3;
            switch ( input.LA(1) ) {
            case ID:
                {
                alt52=1;
                }
                break;
            case CONSIST:
                {
                alt52=2;
                }
                break;
            case COLUMN:
                {
                alt52=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 52, 0, input);

                throw nvae;
            }

            switch (alt52) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:586:2: ^( ID ( expr[$select.getIndexHolder()] ( alias )? )* )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ID163=(CommonTree)match(input,ID,FOLLOW_ID_in_displayed_column1448); 
                    ID163_tree = (CommonTree)adaptor.dupNode(ID163);

                    root_1 = (CommonTree)adaptor.becomeRoot(ID163_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:586:8: ( expr[$select.getIndexHolder()] ( alias )? )*
                        loop48:
                        do {
                            int alt48=2;
                            int LA48_0 = input.LA(1);

                            if ( ((LA48_0>=SUBQUERY && LA48_0<=COLUMN)||LA48_0==QUTED_STR||LA48_0==COLUMNAST||LA48_0==CONSIST||(LA48_0>=CAST && LA48_0<=POSITIVE)||(LA48_0>=BITOR && LA48_0<=DIVIDE)||LA48_0==ID||(LA48_0>=N && LA48_0<=NUMBER)||LA48_0==104||LA48_0==110||(LA48_0>=120 && LA48_0<=121)) ) {
                                alt48=1;
                            }


                            switch (alt48) {
                        	case 1 :
                        	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:586:9: expr[$select.getIndexHolder()] ( alias )?
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    pushFollow(FOLLOW_expr_in_displayed_column1452);
                        	    expr164=expr(select.getIndexHolder());

                        	    state._fsp--;

                        	    adaptor.addChild(root_1, expr164.getTree());
                        	    list.add((expr164!=null?expr164.valueObj:null));
                        	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:586:67: ( alias )?
                        	    int alt47=2;
                        	    int LA47_0 = input.LA(1);

                        	    if ( (LA47_0==AS) ) {
                        	        alt47=1;
                        	    }
                        	    switch (alt47) {
                        	        case 1 :
                        	            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:586:67: alias
                        	            {
                        	            _last = (CommonTree)input.LT(1);
                        	            pushFollow(FOLLOW_alias_in_displayed_column1456);
                        	            alias165=alias();

                        	            state._fsp--;

                        	            adaptor.addChild(root_1, alias165.getTree());

                        	            }
                        	            break;

                        	    }


                        	    }
                        	    break;

                        	default :
                        	    break loop48;
                            }
                        } while (true);


                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	Function func=functionMap.get(((ID163!=null?ID163.getText():null)).toUpperCase());
                    	func.setValue(list);
                    	FunctionColumn funcolumn=new FunctionColumn();
                    	funcolumn.setFunction(func);
                    	funcolumn.setAlias((alias165!=null?alias165.aliText:null));
                    	select.addColumn(funcolumn);
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:594:3: ^( CONSIST ID ( alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    CONSIST166=(CommonTree)match(input,CONSIST,FOLLOW_CONSIST_in_displayed_column1468); 
                    CONSIST166_tree = (CommonTree)adaptor.dupNode(CONSIST166);

                    root_1 = (CommonTree)adaptor.becomeRoot(CONSIST166_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    ID167=(CommonTree)match(input,ID,FOLLOW_ID_in_displayed_column1470); 
                    ID167_tree = (CommonTree)adaptor.dupNode(ID167);

                    adaptor.addChild(root_1, ID167_tree);

                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:594:16: ( alias )?
                    int alt49=2;
                    int LA49_0 = input.LA(1);

                    if ( (LA49_0==AS) ) {
                        alt49=1;
                    }
                    switch (alt49) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:594:16: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_displayed_column1472);
                            alias168=alias();

                            state._fsp--;

                            adaptor.addChild(root_1, alias168.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    	Function func=consistStr.get(((ID167!=null?ID167.getText():null)).toUpperCase());
                    	FunctionColumn funcolumn=new FunctionColumn();
                    	funcolumn.setFunction(func);
                    	funcolumn.setAlias((alias168!=null?alias168.aliText:null));
                    	select.addColumn(funcolumn);
                    	

                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:601:3: ^( COLUMN ( table_alias )? columnAnt ( alias )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    COLUMN169=(CommonTree)match(input,COLUMN,FOLLOW_COLUMN_in_displayed_column1480); 
                    COLUMN169_tree = (CommonTree)adaptor.dupNode(COLUMN169);

                    root_1 = (CommonTree)adaptor.becomeRoot(COLUMN169_tree, root_1);



                    match(input, Token.DOWN, null); 
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:601:12: ( table_alias )?
                    int alt50=2;
                    int LA50_0 = input.LA(1);

                    if ( (LA50_0==COL_TAB) ) {
                        alt50=1;
                    }
                    switch (alt50) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:601:12: table_alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_table_alias_in_displayed_column1482);
                            table_alias170=table_alias();

                            state._fsp--;

                            adaptor.addChild(root_1, table_alias170.getTree());

                            }
                            break;

                    }

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_columnAnt_in_displayed_column1485);
                    columnAnt171=columnAnt();

                    state._fsp--;

                    adaptor.addChild(root_1, columnAnt171.getTree());
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:601:35: ( alias )?
                    int alt51=2;
                    int LA51_0 = input.LA(1);

                    if ( (LA51_0==AS) ) {
                        alt51=1;
                    }
                    switch (alt51) {
                        case 1 :
                            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:601:35: alias
                            {
                            _last = (CommonTree)input.LT(1);
                            pushFollow(FOLLOW_alias_in_displayed_column1487);
                            alias172=alias();

                            state._fsp--;

                            adaptor.addChild(root_1, alias172.getTree());

                            }
                            break;

                    }


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }

                    select.addColumn((table_alias170!=null?table_alias170.aText:null),(columnAnt171!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(columnAnt171.start),
                      input.getTreeAdaptor().getTokenStopIndex(columnAnt171.start))):null),(alias172!=null?alias172.aliText:null));

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:606:1: columnAnt returns [String aText] : ( ASTERISK | N | identifier );
    public final OracleWalker.columnAnt_return columnAnt() throws RecognitionException {
        OracleWalker.columnAnt_return retval = new OracleWalker.columnAnt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ASTERISK173=null;
        CommonTree N174=null;
        OracleWalker.identifier_return identifier175 = null;


        CommonTree ASTERISK173_tree=null;
        CommonTree N174_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:607:2: ( ASTERISK | N | identifier )
            int alt53=3;
            switch ( input.LA(1) ) {
            case ASTERISK:
                {
                alt53=1;
                }
                break;
            case N:
                {
                alt53=2;
                }
                break;
            case ID:
                {
                alt53=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 53, 0, input);

                throw nvae;
            }

            switch (alt53) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:607:3: ASTERISK
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    ASTERISK173=(CommonTree)match(input,ASTERISK,FOLLOW_ASTERISK_in_columnAnt1511); 
                    ASTERISK173_tree = (CommonTree)adaptor.dupNode(ASTERISK173);

                    adaptor.addChild(root_0, ASTERISK173_tree);

                    retval.aText =(ASTERISK173!=null?ASTERISK173.getText():null);

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:608:3: N
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    N174=(CommonTree)match(input,N,FOLLOW_N_in_columnAnt1517); 
                    N174_tree = (CommonTree)adaptor.dupNode(N174);

                    adaptor.addChild(root_0, N174_tree);

                    retval.aText =(N174!=null?N174.getText():null);

                    }
                    break;
                case 3 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:609:3: identifier
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_identifier_in_columnAnt1523);
                    identifier175=identifier();

                    state._fsp--;

                    adaptor.addChild(root_0, identifier175.getTree());
                    retval.aText =(identifier175!=null?(input.getTokenStream().toString(
                      input.getTreeAdaptor().getTokenStartIndex(identifier175.start),
                      input.getTreeAdaptor().getTokenStopIndex(identifier175.start))):null);

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
        public String aText;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "quoted_string"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:612:1: quoted_string returns [String aText] : QUOTED_STRING ;
    public final OracleWalker.quoted_string_return quoted_string() throws RecognitionException {
        OracleWalker.quoted_string_return retval = new OracleWalker.quoted_string_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree QUOTED_STRING176=null;

        CommonTree QUOTED_STRING176_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:613:2: ( QUOTED_STRING )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:613:4: QUOTED_STRING
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            QUOTED_STRING176=(CommonTree)match(input,QUOTED_STRING,FOLLOW_QUOTED_STRING_in_quoted_string1539); 
            QUOTED_STRING176_tree = (CommonTree)adaptor.dupNode(QUOTED_STRING176);

            adaptor.addChild(root_0, QUOTED_STRING176_tree);

            retval.aText = (QUOTED_STRING176!=null?QUOTED_STRING176.getText():null).substring(1, (QUOTED_STRING176!=null?QUOTED_STRING176.getText():null).length() - 1);

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:616:1: identifier : ID ;
    public final OracleWalker.identifier_return identifier() throws RecognitionException {
        OracleWalker.identifier_return retval = new OracleWalker.identifier_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ID177=null;

        CommonTree ID177_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:617:2: ( ID )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:617:3: ID
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            ID177=(CommonTree)match(input,ID,FOLLOW_ID_in_identifier1551); 
            ID177_tree = (CommonTree)adaptor.dupNode(ID177);

            adaptor.addChild(root_0, ID177_tree);


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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:619:1: table_alias returns [String aText] : ^( COL_TAB identifier ) ;
    public final OracleWalker.table_alias_return table_alias() throws RecognitionException {
        OracleWalker.table_alias_return retval = new OracleWalker.table_alias_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree COL_TAB178=null;
        OracleWalker.identifier_return identifier179 = null;


        CommonTree COL_TAB178_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:620:2: ( ^( COL_TAB identifier ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:620:3: ^( COL_TAB identifier )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            COL_TAB178=(CommonTree)match(input,COL_TAB,FOLLOW_COL_TAB_in_table_alias1568); 
            COL_TAB178_tree = (CommonTree)adaptor.dupNode(COL_TAB178);

            root_1 = (CommonTree)adaptor.becomeRoot(COL_TAB178_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_table_alias1570);
            identifier179=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier179.getTree());
            retval.aText =(identifier179!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier179.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier179.start))):null);

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:623:1: alias returns [String aliText] : ^( AS identifier ) ;
    public final OracleWalker.alias_return alias() throws RecognitionException {
        OracleWalker.alias_return retval = new OracleWalker.alias_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree AS180=null;
        OracleWalker.identifier_return identifier181 = null;


        CommonTree AS180_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:624:2: ( ^( AS identifier ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:624:3: ^( AS identifier )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            AS180=(CommonTree)match(input,AS,FOLLOW_AS_in_alias1589); 
            AS180_tree = (CommonTree)adaptor.dupNode(AS180);

            root_1 = (CommonTree)adaptor.becomeRoot(AS180_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_identifier_in_alias1591);
            identifier181=identifier();

            state._fsp--;

            adaptor.addChild(root_1, identifier181.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }

            retval.aliText =(identifier181!=null?(input.getTokenStream().toString(
              input.getTreeAdaptor().getTokenStartIndex(identifier181.start),
              input.getTreeAdaptor().getTokenStopIndex(identifier181.start))):null);

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

    public static class selectClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "selectClause"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:630:1: selectClause[Select select] : ^( SELECT ( indexClause[select] )? select_list[$select] ) ;
    public final OracleWalker.selectClause_return selectClause(Select select) throws RecognitionException {
        OracleWalker.selectClause_return retval = new OracleWalker.selectClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree SELECT182=null;
        OracleWalker.indexClause_return indexClause183 = null;

        OracleWalker.select_list_return select_list184 = null;


        CommonTree SELECT182_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:631:5: ( ^( SELECT ( indexClause[select] )? select_list[$select] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:631:6: ^( SELECT ( indexClause[select] )? select_list[$select] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            SELECT182=(CommonTree)match(input,SELECT,FOLLOW_SELECT_in_selectClause1612); 
            SELECT182_tree = (CommonTree)adaptor.dupNode(SELECT182);

            root_1 = (CommonTree)adaptor.becomeRoot(SELECT182_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:631:15: ( indexClause[select] )?
            int alt54=2;
            int LA54_0 = input.LA(1);

            if ( (LA54_0==ID) ) {
                alt54=1;
            }
            switch (alt54) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:631:15: indexClause[select]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_indexClause_in_selectClause1614);
                    indexClause183=indexClause(select);

                    state._fsp--;

                    adaptor.addChild(root_1, indexClause183.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_select_list_in_selectClause1618);
            select_list184=select_list(select);

            state._fsp--;

            adaptor.addChild(root_1, select_list184.getTree());

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

    public static class indexClause_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "indexClause"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:634:1: indexClause[DMLCommon dmlCommon] : ( hints[$dmlCommon] )+ ;
    public final OracleWalker.indexClause_return indexClause(DMLCommon dmlCommon) throws RecognitionException {
        OracleWalker.indexClause_return retval = new OracleWalker.indexClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.hints_return hints185 = null;



        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:635:2: ( ( hints[$dmlCommon] )+ )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:635:3: ( hints[$dmlCommon] )+
            {
            root_0 = (CommonTree)adaptor.nil();

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:635:3: ( hints[$dmlCommon] )+
            int cnt55=0;
            loop55:
            do {
                int alt55=2;
                int LA55_0 = input.LA(1);

                if ( (LA55_0==ID) ) {
                    alt55=1;
                }


                switch (alt55) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:635:3: hints[$dmlCommon]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_hints_in_indexClause1637);
            	    hints185=hints(dmlCommon);

            	    state._fsp--;

            	    adaptor.addChild(root_0, hints185.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt55 >= 1 ) break loop55;
                        EarlyExitException eee =
                            new EarlyExitException(55, input);
                        throw eee;
                }
                cnt55++;
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
    // $ANTLR end "indexClause"

    public static class hints_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "hints"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:638:1: hints[DMLCommon dmlCommon] : ^( ID ( identifier )* ) ;
    public final OracleWalker.hints_return hints(DMLCommon dmlCommon) throws RecognitionException {
        OracleWalker.hints_return retval = new OracleWalker.hints_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ID186=null;
        OracleWalker.identifier_return identifier187 = null;


        CommonTree ID186_tree=null;


        	List<String> list=new ArrayList<String>();
        	
        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:642:2: ( ^( ID ( identifier )* ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:642:3: ^( ID ( identifier )* )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            ID186=(CommonTree)match(input,ID,FOLLOW_ID_in_hints1657); 
            ID186_tree = (CommonTree)adaptor.dupNode(ID186);

            root_1 = (CommonTree)adaptor.becomeRoot(ID186_tree, root_1);



            if ( input.LA(1)==Token.DOWN ) {
                match(input, Token.DOWN, null); 
                // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:642:9: ( identifier )*
                loop56:
                do {
                    int alt56=2;
                    int LA56_0 = input.LA(1);

                    if ( (LA56_0==ID) ) {
                        alt56=1;
                    }


                    switch (alt56) {
                	case 1 :
                	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:642:10: identifier
                	    {
                	    _last = (CommonTree)input.LT(1);
                	    pushFollow(FOLLOW_identifier_in_hints1661);
                	    identifier187=identifier();

                	    state._fsp--;

                	    adaptor.addChild(root_1, identifier187.getTree());
                	    list.add(((identifier187!=null?(input.getTokenStream().toString(
                	      input.getTreeAdaptor().getTokenStartIndex(identifier187.start),
                	      input.getTreeAdaptor().getTokenStopIndex(identifier187.start))):null)));

                	    }
                	    break;

                	default :
                	    break loop56;
                    }
                } while (true);


                match(input, Token.UP, null); 
            }adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            	HintSetter hint=hintReg.get(((ID186!=null?ID186.getText():null)).toUpperCase());
            	hint.addHint(list);
            	dmlCommon.addHint(hint);
            	

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
    // $ANTLR end "hints"

    public static class select_command_return extends TreeRuleReturnScope {
        public Select select;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "select_command"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:649:1: select_command[BindIndexHolder holder] returns [Select select] : selectClause[$select] ( fromClause[$select] )? ( whereClause[$select.getWhere()] )? ( groupByClause[$select.getWhere()] )? ( orderByClause[$select.getWhere()] )? ( for_update[$select] )? ;
    public final OracleWalker.select_command_return select_command(BindIndexHolder holder) throws RecognitionException {
        OracleWalker.select_command_return retval = new OracleWalker.select_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        OracleWalker.selectClause_return selectClause188 = null;

        OracleWalker.fromClause_return fromClause189 = null;

        OracleWalker.whereClause_return whereClause190 = null;

        OracleWalker.groupByClause_return groupByClause191 = null;

        OracleWalker.orderByClause_return orderByClause192 = null;

        OracleWalker.for_update_return for_update193 = null;




        if(holder ==null){
        	retval.select =new OracleSelect();
        	}
        else{
        	retval.select = new OracleSelect(holder);
        }


        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:6: ( selectClause[$select] ( fromClause[$select] )? ( whereClause[$select.getWhere()] )? ( groupByClause[$select.getWhere()] )? ( orderByClause[$select.getWhere()] )? ( for_update[$select] )? )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:8: selectClause[$select] ( fromClause[$select] )? ( whereClause[$select.getWhere()] )? ( groupByClause[$select.getWhere()] )? ( orderByClause[$select.getWhere()] )? ( for_update[$select] )?
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_selectClause_in_select_command1689);
            selectClause188=selectClause(retval.select);

            state._fsp--;

            adaptor.addChild(root_0, selectClause188.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:30: ( fromClause[$select] )?
            int alt57=2;
            int LA57_0 = input.LA(1);

            if ( (LA57_0==TABLENAMES) ) {
                alt57=1;
            }
            switch (alt57) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:31: fromClause[$select]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_fromClause_in_select_command1693);
                    fromClause189=fromClause(retval.select);

                    state._fsp--;

                    adaptor.addChild(root_0, fromClause189.getTree());

                    }
                    break;

            }

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:54: ( whereClause[$select.getWhere()] )?
            int alt58=2;
            int LA58_0 = input.LA(1);

            if ( (LA58_0==WHERE) ) {
                alt58=1;
            }
            switch (alt58) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:55: whereClause[$select.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_whereClause_in_select_command1700);
                    whereClause190=whereClause(retval.select.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_0, whereClause190.getTree());

                    }
                    break;

            }

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:89: ( groupByClause[$select.getWhere()] )?
            int alt59=2;
            int LA59_0 = input.LA(1);

            if ( (LA59_0==GROUPBY) ) {
                alt59=1;
            }
            switch (alt59) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:90: groupByClause[$select.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_groupByClause_in_select_command1706);
                    groupByClause191=groupByClause(retval.select.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_0, groupByClause191.getTree());

                    }
                    break;

            }

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:126: ( orderByClause[$select.getWhere()] )?
            int alt60=2;
            int LA60_0 = input.LA(1);

            if ( (LA60_0==ORDERBY) ) {
                alt60=1;
            }
            switch (alt60) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:127: orderByClause[$select.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_orderByClause_in_select_command1712);
                    orderByClause192=orderByClause(retval.select.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_0, orderByClause192.getTree());

                    }
                    break;

            }

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:163: ( for_update[$select] )?
            int alt61=2;
            int LA61_0 = input.LA(1);

            if ( (LA61_0==FORUPDATE) ) {
                alt61=1;
            }
            switch (alt61) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:659:164: for_update[$select]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_for_update_in_select_command1718);
                    for_update193=for_update(retval.select);

                    state._fsp--;

                    adaptor.addChild(root_0, for_update193.getTree());

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

    public static class for_update_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "for_update"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:661:1: for_update[Select select] : ^( FORUPDATE ( of_statement[update] )? ( wait_statement[update] )? ) ;
    public final OracleWalker.for_update_return for_update(Select select) throws RecognitionException {
        OracleWalker.for_update_return retval = new OracleWalker.for_update_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree FORUPDATE194=null;
        OracleWalker.of_statement_return of_statement195 = null;

        OracleWalker.wait_statement_return wait_statement196 = null;


        CommonTree FORUPDATE194_tree=null;


        		OracleForUpdate update = new OracleForUpdate();
        	
        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:665:2: ( ^( FORUPDATE ( of_statement[update] )? ( wait_statement[update] )? ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:665:3: ^( FORUPDATE ( of_statement[update] )? ( wait_statement[update] )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            FORUPDATE194=(CommonTree)match(input,FORUPDATE,FOLLOW_FORUPDATE_in_for_update1741); 
            FORUPDATE194_tree = (CommonTree)adaptor.dupNode(FORUPDATE194);

            root_1 = (CommonTree)adaptor.becomeRoot(FORUPDATE194_tree, root_1);



            if ( input.LA(1)==Token.DOWN ) {
                match(input, Token.DOWN, null); 
                // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:665:15: ( of_statement[update] )?
                int alt62=2;
                int LA62_0 = input.LA(1);

                if ( (LA62_0==OF) ) {
                    alt62=1;
                }
                switch (alt62) {
                    case 1 :
                        // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:665:15: of_statement[update]
                        {
                        _last = (CommonTree)input.LT(1);
                        pushFollow(FOLLOW_of_statement_in_for_update1743);
                        of_statement195=of_statement(update);

                        state._fsp--;

                        adaptor.addChild(root_1, of_statement195.getTree());

                        }
                        break;

                }

                // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:665:37: ( wait_statement[update] )?
                int alt63=2;
                int LA63_0 = input.LA(1);

                if ( (LA63_0==NOWAIT||LA63_0==WAIT) ) {
                    alt63=1;
                }
                switch (alt63) {
                    case 1 :
                        // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:665:37: wait_statement[update]
                        {
                        _last = (CommonTree)input.LT(1);
                        pushFollow(FOLLOW_wait_statement_in_for_update1747);
                        wait_statement196=wait_statement(update);

                        state._fsp--;

                        adaptor.addChild(root_1, wait_statement196.getTree());

                        }
                        break;

                }


                match(input, Token.UP, null); 
            }adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		select.addForUpdate(update);
            	

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

    public static class of_statement_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "of_statement"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:670:1: of_statement[OracleForUpdate update] : ^( OF expr[null] ) ;
    public final OracleWalker.of_statement_return of_statement(OracleForUpdate update) throws RecognitionException {
        OracleWalker.of_statement_return retval = new OracleWalker.of_statement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree OF197=null;
        OracleWalker.expr_return expr198 = null;


        CommonTree OF197_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:671:2: ( ^( OF expr[null] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:671:3: ^( OF expr[null] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            OF197=(CommonTree)match(input,OF,FOLLOW_OF_in_of_statement1765); 
            OF197_tree = (CommonTree)adaptor.dupNode(OF197);

            root_1 = (CommonTree)adaptor.becomeRoot(OF197_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_expr_in_of_statement1767);
            expr198=expr(null);

            state._fsp--;

            adaptor.addChild(root_1, expr198.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		update.setOfColumn((Column)(expr198!=null?expr198.valueObj:null));
            	

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
    // $ANTLR end "of_statement"

    public static class wait_statement_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "wait_statement"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:676:1: wait_statement[OracleForUpdate update] : ( ^( NOWAIT ( N )? ) | ^( WAIT N ) );
    public final OracleWalker.wait_statement_return wait_statement(OracleForUpdate update) throws RecognitionException {
        OracleWalker.wait_statement_return retval = new OracleWalker.wait_statement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree NOWAIT199=null;
        CommonTree N200=null;
        CommonTree WAIT201=null;
        CommonTree N202=null;

        CommonTree NOWAIT199_tree=null;
        CommonTree N200_tree=null;
        CommonTree WAIT201_tree=null;
        CommonTree N202_tree=null;

        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:677:2: ( ^( NOWAIT ( N )? ) | ^( WAIT N ) )
            int alt65=2;
            int LA65_0 = input.LA(1);

            if ( (LA65_0==NOWAIT) ) {
                alt65=1;
            }
            else if ( (LA65_0==WAIT) ) {
                alt65=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 65, 0, input);

                throw nvae;
            }
            switch (alt65) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:677:3: ^( NOWAIT ( N )? )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    NOWAIT199=(CommonTree)match(input,NOWAIT,FOLLOW_NOWAIT_in_wait_statement1783); 
                    NOWAIT199_tree = (CommonTree)adaptor.dupNode(NOWAIT199);

                    root_1 = (CommonTree)adaptor.becomeRoot(NOWAIT199_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:677:12: ( N )?
                        int alt64=2;
                        int LA64_0 = input.LA(1);

                        if ( (LA64_0==N) ) {
                            alt64=1;
                        }
                        switch (alt64) {
                            case 1 :
                                // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:677:12: N
                                {
                                _last = (CommonTree)input.LT(1);
                                N200=(CommonTree)match(input,N,FOLLOW_N_in_wait_statement1785); 
                                N200_tree = (CommonTree)adaptor.dupNode(N200);

                                adaptor.addChild(root_1, N200_tree);


                                }
                                break;

                        }


                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		update.setWait(0);
                    	

                    }
                    break;
                case 2 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:681:3: ^( WAIT N )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    WAIT201=(CommonTree)match(input,WAIT,FOLLOW_WAIT_in_wait_statement1795); 
                    WAIT201_tree = (CommonTree)adaptor.dupNode(WAIT201);

                    root_1 = (CommonTree)adaptor.becomeRoot(WAIT201_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    N202=(CommonTree)match(input,N,FOLLOW_N_in_wait_statement1797); 
                    N202_tree = (CommonTree)adaptor.dupNode(N202);

                    adaptor.addChild(root_1, N202_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		update.setWait(Integer.valueOf((N202!=null?N202.getText():null)));
                    	

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
    // $ANTLR end "wait_statement"

    public static class delete_command_return extends TreeRuleReturnScope {
        public Delete del;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "delete_command"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:686:1: delete_command returns [Delete del] : ^( DELETE ( indexClause[$del] )? tables[$del] ( whereClause[$del.getWhere()] )? ( orderByClause[$del.getWhere()] )? ) ;
    public final OracleWalker.delete_command_return delete_command() throws RecognitionException {
        OracleWalker.delete_command_return retval = new OracleWalker.delete_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree DELETE203=null;
        OracleWalker.indexClause_return indexClause204 = null;

        OracleWalker.tables_return tables205 = null;

        OracleWalker.whereClause_return whereClause206 = null;

        OracleWalker.orderByClause_return orderByClause207 = null;


        CommonTree DELETE203_tree=null;

        retval.del =new OracleDelete();
        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:688:2: ( ^( DELETE ( indexClause[$del] )? tables[$del] ( whereClause[$del.getWhere()] )? ( orderByClause[$del.getWhere()] )? ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:688:3: ^( DELETE ( indexClause[$del] )? tables[$del] ( whereClause[$del.getWhere()] )? ( orderByClause[$del.getWhere()] )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            DELETE203=(CommonTree)match(input,DELETE,FOLLOW_DELETE_in_delete_command1818); 
            DELETE203_tree = (CommonTree)adaptor.dupNode(DELETE203);

            root_1 = (CommonTree)adaptor.becomeRoot(DELETE203_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:688:12: ( indexClause[$del] )?
            int alt66=2;
            int LA66_0 = input.LA(1);

            if ( (LA66_0==ID) ) {
                alt66=1;
            }
            switch (alt66) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:688:12: indexClause[$del]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_indexClause_in_delete_command1820);
                    indexClause204=indexClause(retval.del);

                    state._fsp--;

                    adaptor.addChild(root_1, indexClause204.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_tables_in_delete_command1824);
            tables205=tables(retval.del);

            state._fsp--;

            adaptor.addChild(root_1, tables205.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:688:44: ( whereClause[$del.getWhere()] )?
            int alt67=2;
            int LA67_0 = input.LA(1);

            if ( (LA67_0==WHERE) ) {
                alt67=1;
            }
            switch (alt67) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:688:44: whereClause[$del.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_whereClause_in_delete_command1827);
                    whereClause206=whereClause(retval.del.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, whereClause206.getTree());

                    }
                    break;

            }

            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:688:74: ( orderByClause[$del.getWhere()] )?
            int alt68=2;
            int LA68_0 = input.LA(1);

            if ( (LA68_0==ORDERBY) ) {
                alt68=1;
            }
            switch (alt68) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:688:74: orderByClause[$del.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_orderByClause_in_delete_command1831);
                    orderByClause207=orderByClause(retval.del.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, orderByClause207.getTree());

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
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:690:1: update_command returns [Update update] : ^( UPDATE ( indexClause[$update] )? tables[$update] setclause[$update] ( whereClause[$update.getWhere()] )? ) ;
    public final OracleWalker.update_command_return update_command() throws RecognitionException {
        OracleWalker.update_command_return retval = new OracleWalker.update_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree UPDATE208=null;
        OracleWalker.indexClause_return indexClause209 = null;

        OracleWalker.tables_return tables210 = null;

        OracleWalker.setclause_return setclause211 = null;

        OracleWalker.whereClause_return whereClause212 = null;


        CommonTree UPDATE208_tree=null;

        retval.update =new OracleUpdate();
        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:692:2: ( ^( UPDATE ( indexClause[$update] )? tables[$update] setclause[$update] ( whereClause[$update.getWhere()] )? ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:692:3: ^( UPDATE ( indexClause[$update] )? tables[$update] setclause[$update] ( whereClause[$update.getWhere()] )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            UPDATE208=(CommonTree)match(input,UPDATE,FOLLOW_UPDATE_in_update_command1851); 
            UPDATE208_tree = (CommonTree)adaptor.dupNode(UPDATE208);

            root_1 = (CommonTree)adaptor.becomeRoot(UPDATE208_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:692:12: ( indexClause[$update] )?
            int alt69=2;
            int LA69_0 = input.LA(1);

            if ( (LA69_0==ID) ) {
                alt69=1;
            }
            switch (alt69) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:692:12: indexClause[$update]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_indexClause_in_update_command1853);
                    indexClause209=indexClause(retval.update);

                    state._fsp--;

                    adaptor.addChild(root_1, indexClause209.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_tables_in_update_command1857);
            tables210=tables(retval.update);

            state._fsp--;

            adaptor.addChild(root_1, tables210.getTree());
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_setclause_in_update_command1860);
            setclause211=setclause(retval.update);

            state._fsp--;

            adaptor.addChild(root_1, setclause211.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:692:69: ( whereClause[$update.getWhere()] )?
            int alt70=2;
            int LA70_0 = input.LA(1);

            if ( (LA70_0==WHERE) ) {
                alt70=1;
            }
            switch (alt70) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:692:69: whereClause[$update.getWhere()]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_whereClause_in_update_command1863);
                    whereClause212=whereClause(retval.update.getWhere());

                    state._fsp--;

                    adaptor.addChild(root_1, whereClause212.getTree());

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

    public static class insert_command_return extends TreeRuleReturnScope {
        public Insert ins;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "insert_command"
    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:696:1: insert_command returns [Insert ins] : ^( INSERT ( indexClause[$ins] )? tables[$ins] ( column_specs[$ins] )* values[$ins] ) ;
    public final OracleWalker.insert_command_return insert_command() throws RecognitionException {
        OracleWalker.insert_command_return retval = new OracleWalker.insert_command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree INSERT213=null;
        OracleWalker.indexClause_return indexClause214 = null;

        OracleWalker.tables_return tables215 = null;

        OracleWalker.column_specs_return column_specs216 = null;

        OracleWalker.values_return values217 = null;


        CommonTree INSERT213_tree=null;

        retval.ins =new Insert();
        try {
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:698:2: ( ^( INSERT ( indexClause[$ins] )? tables[$ins] ( column_specs[$ins] )* values[$ins] ) )
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:698:3: ^( INSERT ( indexClause[$ins] )? tables[$ins] ( column_specs[$ins] )* values[$ins] )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            INSERT213=(CommonTree)match(input,INSERT,FOLLOW_INSERT_in_insert_command1887); 
            INSERT213_tree = (CommonTree)adaptor.dupNode(INSERT213);

            root_1 = (CommonTree)adaptor.becomeRoot(INSERT213_tree, root_1);



            match(input, Token.DOWN, null); 
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:698:12: ( indexClause[$ins] )?
            int alt71=2;
            int LA71_0 = input.LA(1);

            if ( (LA71_0==ID) ) {
                alt71=1;
            }
            switch (alt71) {
                case 1 :
                    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:698:12: indexClause[$ins]
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_indexClause_in_insert_command1889);
                    indexClause214=indexClause(retval.ins);

                    state._fsp--;

                    adaptor.addChild(root_1, indexClause214.getTree());

                    }
                    break;

            }

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_tables_in_insert_command1893);
            tables215=tables(retval.ins);

            state._fsp--;

            adaptor.addChild(root_1, tables215.getTree());
            // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:698:44: ( column_specs[$ins] )*
            loop72:
            do {
                int alt72=2;
                int LA72_0 = input.LA(1);

                if ( (LA72_0==COLUMNS) ) {
                    alt72=1;
                }


                switch (alt72) {
            	case 1 :
            	    // D:\\source\\tddl-trunk\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\oracle\\OracleWalker.g:698:44: column_specs[$ins]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_column_specs_in_insert_command1896);
            	    column_specs216=column_specs(retval.ins);

            	    state._fsp--;

            	    adaptor.addChild(root_1, column_specs216.getTree());

            	    }
            	    break;

            	default :
            	    break loop72;
                }
            } while (true);

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_values_in_insert_command1900);
            values217=values(retval.ins);

            state._fsp--;

            adaptor.addChild(root_1, values217.getTree());

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

    // Delegated rules


    protected DFA15 dfa15 = new DFA15(this);
    static final String DFA15_eotS =
        "\15\uffff";
    static final String DFA15_eofS =
        "\15\uffff";
    static final String DFA15_minS =
        "\1\3\14\uffff";
    static final String DFA15_maxS =
        "\1\143\14\uffff";
    static final String DFA15_acceptS =
        "\1\uffff\1\2\13\1";
    static final String DFA15_specialS =
        "\15\uffff}>";
    static final String[] DFA15_transitionS = {
            "\1\1\12\uffff\1\10\20\uffff\1\11\1\12\3\uffff\1\14\1\13\6\uffff"+
            "\1\1\21\uffff\1\2\15\uffff\1\4\1\5\1\3\1\6\1\7\21\uffff\2\1",
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

    static final short[] DFA15_eot = DFA.unpackEncodedString(DFA15_eotS);
    static final short[] DFA15_eof = DFA.unpackEncodedString(DFA15_eofS);
    static final char[] DFA15_min = DFA.unpackEncodedStringToUnsignedChars(DFA15_minS);
    static final char[] DFA15_max = DFA.unpackEncodedStringToUnsignedChars(DFA15_maxS);
    static final short[] DFA15_accept = DFA.unpackEncodedString(DFA15_acceptS);
    static final short[] DFA15_special = DFA.unpackEncodedString(DFA15_specialS);
    static final short[][] DFA15_transition;

    static {
        int numStates = DFA15_transitionS.length;
        DFA15_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA15_transition[i] = DFA.unpackEncodedString(DFA15_transitionS[i]);
        }
    }

    class DFA15 extends DFA {

        public DFA15(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 15;
            this.eot = DFA15_eot;
            this.eof = DFA15_eof;
            this.min = DFA15_min;
            this.max = DFA15_max;
            this.accept = DFA15_accept;
            this.special = DFA15_special;
            this.transition = DFA15_transition;
        }
        public String getDescription() {
            return "()+ loopback of 184:3: ( condition_expr[$where,$exp] )+";
        }
    }
 

    public static final BitSet FOLLOW_start_rule_in_beg48 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_select_command_in_start_rule65 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insert_command_in_start_rule71 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_update_command_in_start_rule76 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_delete_command_in_start_rule81 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SET_in_setclause94 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_updateColumnSpecs_in_setclause96 = new BitSet(new long[]{0x0002000000000008L});
    public static final BitSet FOLLOW_SET_ELE_in_updateColumnSpecs111 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_updateColumnSpec_in_updateColumnSpecs113 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_EQ_in_updateColumnSpec127 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_alias_in_updateColumnSpec129 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_identifier_in_updateColumnSpec132 = new BitSet(new long[]{0x1C10200000020180L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_in_updateColumnSpec134 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_INSERT_VAL_in_values151 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_values154 = new BitSet(new long[]{0x1C10200000020188L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_COLUMNS_in_column_specs170 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_column_spec_in_column_specs172 = new BitSet(new long[]{0x0000000000000108L});
    public static final BitSet FOLLOW_COLUMN_in_column_spec186 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_column_spec188 = new BitSet(new long[]{0x0000000000000008L,0x0000000000000800L});
    public static final BitSet FOLLOW_table_name_in_column_spec190 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_WHERE_in_whereClause209 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_sqlCondition_in_whereClause211 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_GROUPBY_in_groupByClause226 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_columnNamesAfterWhere_in_groupByClause228 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ORDERBY_in_orderByClause241 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_columnNamesAfterWhere_in_orderByClause243 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_columnNameAfterWhere_in_columnNamesAfterWhere262 = new BitSet(new long[]{0x0000000000000002L,0x0000000000060000L});
    public static final BitSet FOLLOW_ASC_in_columnNameAfterWhere276 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_columnNameAfterWhere279 = new BitSet(new long[]{0x0004000000000008L});
    public static final BitSet FOLLOW_table_alias_in_columnNameAfterWhere281 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_DESC_in_columnNameAfterWhere289 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_columnNameAfterWhere292 = new BitSet(new long[]{0x0004000000000008L});
    public static final BitSet FOLLOW_table_alias_in_columnNameAfterWhere294 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_CONDITION_OR_NOT_in_sqlCondition311 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_sqlCondition313 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_CONDITION_OR_in_sqlCondition320 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_sqlCondition322 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_98_in_condition343 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_condition347 = new BitSet(new long[]{0x4000103180004008L,0x0000000C0001F000L});
    public static final BitSet FOLLOW_99_in_condition359 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_condition361 = new BitSet(new long[]{0x4000103180004008L,0x0000000C0001F000L});
    public static final BitSet FOLLOW_condition_PAREN_in_condition372 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_PRIORITY_in_condition378 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_condition_in_condition380 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_condition_expr_in_condition_PAREN395 = new BitSet(new long[]{0x4000003180004002L,0x000000000001F000L});
    public static final BitSet FOLLOW_comparisonCondition_in_condition_expr408 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_inCondition_in_condition_expr414 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_isCondition_in_condition_expr421 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_likeCondition_in_condition_expr428 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NOT_BETWEEN_in_betweenCondition440 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_between_and_in_betweenCondition442 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BETWEEN_in_betweenCondition448 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_between_and_in_betweenCondition450 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_NOT_LIKE_in_likeCondition471 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_likeCondition473 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_likeCondition476 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_LIKE_in_likeCondition484 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_likeCondition486 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_likeCondition489 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ISNOT_in_isCondition505 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_NULL_in_isCondition507 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_isCondition509 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_IS_in_isCondition517 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_NULL_in_isCondition519 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_isCondition521 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_EQ_in_comparisonCondition538 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition540 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition543 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_NOT_EQ_in_comparisonCondition553 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition555 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition558 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_LTH_in_comparisonCondition568 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition570 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition573 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_GTH_in_comparisonCondition583 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition585 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition588 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_LEQ_in_comparisonCondition598 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition600 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition603 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_GEQ_in_comparisonCondition613 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_comparisonCondition615 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_comparisonCondition618 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_CONDITION_LEFT_in_left_cond639 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_left_cond641 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_IN_LISTS_in_in_list660 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_inCondition_expr_adds_in_in_list662 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_IN_in_inCondition680 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_97_in_inCondition684 = new BitSet(new long[]{0x1C10200008120180L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_subquery_in_inCondition687 = new BitSet(new long[]{0x0000000008100000L});
    public static final BitSet FOLLOW_in_list_in_inCondition691 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_left_cond_in_inCondition696 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_expr_add_in_inCondition_expr_adds730 = new BitSet(new long[]{0x1C10200000020102L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr755 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_subquery_in_expr762 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NEGATIVE_in_expr_add790 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add794 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_POSITIVE_in_expr_add803 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add807 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_PLUS_in_expr_add816 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add820 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add825 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_MINUS_in_expr_add834 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add838 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add843 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BITOR_in_expr_add851 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add855 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add860 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BITAND_in_expr_add868 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add872 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add877 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BITXOR_in_expr_add885 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add889 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add894 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SHIFTLEFT_in_expr_add902 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add906 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add911 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SHIFTRIGHT_in_expr_add919 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add923 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add928 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ASTERISK_in_expr_add936 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add940 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add945 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_DIVIDE_in_expr_add953 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_add_in_expr_add957 = new BitSet(new long[]{0x1C10200000020100L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_expr_add_in_expr_add962 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_N_in_expr_add969 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NUMBER_in_expr_add975 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_boolean_literal_in_expr_add980 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_104_in_expr_add984 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_110_in_expr_add990 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUTED_STR_in_expr_add996 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_quoted_string_in_expr_add998 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_COLUMN_in_expr_add1005 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_expr_add1007 = new BitSet(new long[]{0x0000000000000008L,0x0000000000000800L});
    public static final BitSet FOLLOW_table_name_in_expr_add1009 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_COLUMNAST_in_expr_add1019 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ASTERISK_in_expr_add1021 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ID_in_expr_add1030 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_expr_add1035 = new BitSet(new long[]{0x1C10200000020188L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_CONSIST_in_expr_add1046 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ID_in_expr_add1048 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_CAST_in_expr_add1055 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_expr_add1058 = new BitSet(new long[]{0x1C10200000020180L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_ID_in_expr_add1064 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_set_in_value0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_120_in_boolean_literal1098 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_121_in_boolean_literal1107 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SELECT_LIST_in_select_list1123 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_displayed_column_in_select_list1125 = new BitSet(new long[]{0x0010000000000108L,0x0000000000000800L});
    public static final BitSet FOLLOW_TABLENAMES_in_fromClause1140 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_in_fromClause1142 = new BitSet(new long[]{0x0000000000000028L});
    public static final BitSet FOLLOW_JOIN_in_join_clause1160 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_name_in_join_clause1162 = new BitSet(new long[]{0x0000000000000300L});
    public static final BitSet FOLLOW_alias_in_join_clause1164 = new BitSet(new long[]{0x0000000000000300L});
    public static final BitSet FOLLOW_join_column_in_join_clause1169 = new BitSet(new long[]{0x0000000000000300L});
    public static final BitSet FOLLOW_join_column_in_join_clause1173 = new BitSet(new long[]{0x0000000007E00008L});
    public static final BitSet FOLLOW_join_type_in_join_clause1175 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_INNER_in_join_type1194 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LEFT_in_join_type1202 = new BitSet(new long[]{0x0000000000000402L});
    public static final BitSet FOLLOW_outer_in_join_type1204 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RIGHT_in_join_type1212 = new BitSet(new long[]{0x0000000000000402L});
    public static final BitSet FOLLOW_outer_in_join_type1214 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FULL_in_join_type1222 = new BitSet(new long[]{0x0000000000000402L});
    public static final BitSet FOLLOW_outer_in_join_type1224 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UNION_in_join_type1233 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_CROSS_in_join_type1241 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_OUTER_in_outer1258 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_COLUMN_in_join_column1274 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_join_column1276 = new BitSet(new long[]{0x0000000000000008L,0x0000000000000800L});
    public static final BitSet FOLLOW_table_name_in_join_column1278 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_TABLENAME_in_table1298 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_spec_in_table1300 = new BitSet(new long[]{0x0020000000000008L});
    public static final BitSet FOLLOW_join_clause_in_table1303 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_TABLENAMES_in_tables1320 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_in_tables1322 = new BitSet(new long[]{0x0000000000000028L});
    public static final BitSet FOLLOW_schema_name_in_table_spec1346 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000800L});
    public static final BitSet FOLLOW_table_name_in_table_spec1350 = new BitSet(new long[]{0x0000000000000202L});
    public static final BitSet FOLLOW_alias_in_table_spec1353 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_subquery_in_table_spec1361 = new BitSet(new long[]{0x0000000000000202L});
    public static final BitSet FOLLOW_alias_in_table_spec1364 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_table_spec1373 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_table_spec1376 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_alias_in_table_spec1383 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_schema_name1398 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SUBQUERY_in_subquery1413 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_select_command_in_subquery1415 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_identifier_in_table_name1430 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_displayed_column1448 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_displayed_column1452 = new BitSet(new long[]{0x1C10200000020388L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_alias_in_displayed_column1456 = new BitSet(new long[]{0x1C10200000020188L,0x0300410000300BFEL});
    public static final BitSet FOLLOW_CONSIST_in_displayed_column1468 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ID_in_displayed_column1470 = new BitSet(new long[]{0x0000000000000208L});
    public static final BitSet FOLLOW_alias_in_displayed_column1472 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_COLUMN_in_displayed_column1480 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_table_alias_in_displayed_column1482 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100900L});
    public static final BitSet FOLLOW_columnAnt_in_displayed_column1485 = new BitSet(new long[]{0x0000000000000208L});
    public static final BitSet FOLLOW_alias_in_displayed_column1487 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ASTERISK_in_columnAnt1511 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_N_in_columnAnt1517 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_columnAnt1523 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTED_STRING_in_quoted_string1539 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_identifier1551 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_COL_TAB_in_table_alias1568 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_table_alias1570 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_AS_in_alias1589 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_alias1591 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_SELECT_in_selectClause1612 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_indexClause_in_selectClause1614 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_select_list_in_selectClause1618 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_hints_in_indexClause1637 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000800L});
    public static final BitSet FOLLOW_ID_in_hints1657 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_identifier_in_hints1661 = new BitSet(new long[]{0x0000000000000008L,0x0000000000000800L});
    public static final BitSet FOLLOW_selectClause_in_select_command1689 = new BitSet(new long[]{0x0040030000040042L});
    public static final BitSet FOLLOW_fromClause_in_select_command1693 = new BitSet(new long[]{0x0040030000040002L});
    public static final BitSet FOLLOW_whereClause_in_select_command1700 = new BitSet(new long[]{0x0040030000000002L});
    public static final BitSet FOLLOW_groupByClause_in_select_command1706 = new BitSet(new long[]{0x0040020000000002L});
    public static final BitSet FOLLOW_orderByClause_in_select_command1712 = new BitSet(new long[]{0x0040000000000002L});
    public static final BitSet FOLLOW_for_update_in_select_command1718 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FORUPDATE_in_for_update1741 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_of_statement_in_for_update1743 = new BitSet(new long[]{0x0280000000000008L});
    public static final BitSet FOLLOW_wait_statement_in_for_update1747 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_OF_in_of_statement1765 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expr_in_of_statement1767 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_NOWAIT_in_wait_statement1783 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_N_in_wait_statement1785 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_WAIT_in_wait_statement1795 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_N_in_wait_statement1797 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_DELETE_in_delete_command1818 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_indexClause_in_delete_command1820 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_tables_in_delete_command1824 = new BitSet(new long[]{0x0000020000040008L});
    public static final BitSet FOLLOW_whereClause_in_delete_command1827 = new BitSet(new long[]{0x0000020000000008L});
    public static final BitSet FOLLOW_orderByClause_in_delete_command1831 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_UPDATE_in_update_command1851 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_indexClause_in_update_command1853 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_tables_in_update_command1857 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_setclause_in_update_command1860 = new BitSet(new long[]{0x0000000000040008L});
    public static final BitSet FOLLOW_whereClause_in_update_command1863 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_INSERT_in_insert_command1887 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_indexClause_in_insert_command1889 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_tables_in_insert_command1893 = new BitSet(new long[]{0x0000480000000000L});
    public static final BitSet FOLLOW_column_specs_in_insert_command1896 = new BitSet(new long[]{0x0000480000000000L});
    public static final BitSet FOLLOW_values_in_insert_command1900 = new BitSet(new long[]{0x0000000000000008L});

}
