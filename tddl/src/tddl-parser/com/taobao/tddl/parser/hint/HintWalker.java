// $ANTLR 3.1.1 HintWalker.g 2009-08-21 16:17:04

	package com.taobao.tddl.parser.hint;
	
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.BitSet;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.antlr.runtime.tree.TreeNodeStream;
import org.antlr.runtime.tree.TreeParser;
import org.antlr.runtime.tree.TreeRuleReturnScope;

import com.taobao.tddl.sqlobjecttree.common.value.BindVar;


public class HintWalker extends TreeParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "HINT", "EXP_LIST", "EXP", "ARGS", "BIND_VAR", "ARG", "DIVIDE", "ASTERISK", "ID", "COLON", "SEMICOLON", "COMMA", "WS", "INT", "EXPONENT", "FLOAT", "'{'", "'}'", "'?'", "'['", "']'"
    };
    public static final int EXPONENT=18;
    public static final int T__24=24;
    public static final int T__23=23;
    public static final int T__22=22;
    public static final int T__21=21;
    public static final int T__20=20;
    public static final int EXP_LIST=5;
    public static final int FLOAT=19;
    public static final int INT=17;
    public static final int SEMICOLON=14;
    public static final int ID=12;
    public static final int EOF=-1;
    public static final int BIND_VAR=8;
    public static final int ASTERISK=11;
    public static final int COLON=13;
    public static final int WS=16;
    public static final int ARG=9;
    public static final int EXP=6;
    public static final int COMMA=15;
    public static final int ARGS=7;
    public static final int HINT=4;
    public static final int DIVIDE=10;

    // delegates
    // delegators


        public HintWalker(TreeNodeStream input) {
            this(input, new RecognizerSharedState());
        }
        public HintWalker(TreeNodeStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return HintWalker.tokenNames; }
    public String getGrammarFileName() { return "HintWalker.g"; }


    	public int index=0;


    public static class beg_return extends TreeRuleReturnScope {
        public Map map;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "beg"
    // HintWalker.g:22:1: beg returns [Map map] : ( hints[$map] )* ;
    public final HintWalker.beg_return beg() throws RecognitionException {
        HintWalker.beg_return retval = new HintWalker.beg_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        HintWalker.hints_return hints1 = null;




        	retval.map =new HashMap();
        	
        try {
            // HintWalker.g:25:3: ( ( hints[$map] )* )
            // HintWalker.g:26:2: ( hints[$map] )*
            {
            root_0 = (CommonTree)adaptor.nil();

            // HintWalker.g:26:2: ( hints[$map] )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==HINT) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // HintWalker.g:26:2: hints[$map]
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_hints_in_beg55);
            	    hints1=hints(retval.map);

            	    state._fsp--;

            	    adaptor.addChild(root_0, hints1.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
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
    // $ANTLR end "beg"

    public static class hints_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "hints"
    // HintWalker.g:28:1: hints[Map map] : ^( HINT ID value ) ;
    public final HintWalker.hints_return hints(Map map) throws RecognitionException {
        HintWalker.hints_return retval = new HintWalker.hints_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree HINT2=null;
        CommonTree ID3=null;
        HintWalker.value_return value4 = null;


        CommonTree HINT2_tree=null;
        CommonTree ID3_tree=null;

        try {
            // HintWalker.g:28:17: ( ^( HINT ID value ) )
            // HintWalker.g:29:2: ^( HINT ID value )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            HINT2=(CommonTree)match(input,HINT,FOLLOW_HINT_in_hints70); 
            HINT2_tree = (CommonTree)adaptor.dupNode(HINT2);

            root_1 = (CommonTree)adaptor.becomeRoot(HINT2_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            ID3=(CommonTree)match(input,ID,FOLLOW_ID_in_hints72); 
            ID3_tree = (CommonTree)adaptor.dupNode(ID3);

            adaptor.addChild(root_1, ID3_tree);

            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_value_in_hints74);
            value4=value();

            state._fsp--;

            adaptor.addChild(root_1, value4.getTree());

            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		map.put((ID3!=null?ID3.getText():null),(value4!=null?value4.retVal:null));
            	

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

    public static class value_return extends TreeRuleReturnScope {
        public Object retVal;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "value"
    // HintWalker.g:33:1: value returns [Object retVal] : ( ^( EXP_LIST exp_list ) | exp );
    public final HintWalker.value_return value() throws RecognitionException {
        HintWalker.value_return retval = new HintWalker.value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree EXP_LIST5=null;
        HintWalker.exp_list_return exp_list6 = null;

        HintWalker.exp_return exp7 = null;


        CommonTree EXP_LIST5_tree=null;

        try {
            // HintWalker.g:34:2: ( ^( EXP_LIST exp_list ) | exp )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==EXP_LIST) ) {
                alt2=1;
            }
            else if ( (LA2_0==EXP) ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // HintWalker.g:35:2: ^( EXP_LIST exp_list )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    EXP_LIST5=(CommonTree)match(input,EXP_LIST,FOLLOW_EXP_LIST_in_value92); 
                    EXP_LIST5_tree = (CommonTree)adaptor.dupNode(EXP_LIST5);

                    root_1 = (CommonTree)adaptor.becomeRoot(EXP_LIST5_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        _last = (CommonTree)input.LT(1);
                        pushFollow(FOLLOW_exp_list_in_value94);
                        exp_list6=exp_list();

                        state._fsp--;

                        adaptor.addChild(root_1, exp_list6.getTree());

                        match(input, Token.UP, null); 
                    }adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		retval.retVal =(exp_list6!=null?exp_list6.retVal:null);
                    	

                    }
                    break;
                case 2 :
                    // HintWalker.g:39:3: exp
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_exp_in_value102);
                    exp7=exp();

                    state._fsp--;

                    adaptor.addChild(root_0, exp7.getTree());
                    	
                    		HintElement tempMap=null;
                    		if((exp7!=null?exp7.retKey:null)!=null){
                    			tempMap=new HintElement((exp7!=null?exp7.retKey:null),(exp7!=null?exp7.retVal:null));
                    		}else{
                    			throw new IllegalArgumentException("key is null,value is "+ (exp7!=null?exp7.retVal:null));
                    		}
                    		retval.retVal =tempMap;
                    	

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
    // $ANTLR end "value"

    public static class exp_list_return extends TreeRuleReturnScope {
        public Object retVal;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "exp_list"
    // HintWalker.g:51:1: exp_list returns [Object retVal] : ( exp )* ;
    public final HintWalker.exp_list_return exp_list() throws RecognitionException {
        HintWalker.exp_list_return retval = new HintWalker.exp_list_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        HintWalker.exp_return exp8 = null;




        		List tempList=new ArrayList();
        	
        try {
            // HintWalker.g:54:3: ( ( exp )* )
            // HintWalker.g:55:3: ( exp )*
            {
            root_0 = (CommonTree)adaptor.nil();

            // HintWalker.g:55:3: ( exp )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==EXP) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // HintWalker.g:55:4: exp
            	    {
            	    _last = (CommonTree)input.LT(1);
            	    pushFollow(FOLLOW_exp_in_exp_list127);
            	    exp8=exp();

            	    state._fsp--;

            	    adaptor.addChild(root_0, exp8.getTree());

            	    			tempList.add(new HintElement((exp8!=null?exp8.retKey:null),(exp8!=null?exp8.retVal:null)));
            	    		

            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);


            			retval.retVal =tempList;
            		

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
    // $ANTLR end "exp_list"

    public static class exp_return extends TreeRuleReturnScope {
        public Object retKey;
        public Object retVal;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "exp"
    // HintWalker.g:63:1: exp returns [Object retKey,Object retVal] : ^( EXP expKey ( args )? ) ;
    public final HintWalker.exp_return exp() throws RecognitionException {
        HintWalker.exp_return retval = new HintWalker.exp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree EXP9=null;
        HintWalker.expKey_return expKey10 = null;

        HintWalker.args_return args11 = null;


        CommonTree EXP9_tree=null;

        try {
            // HintWalker.g:64:2: ( ^( EXP expKey ( args )? ) )
            // HintWalker.g:64:3: ^( EXP expKey ( args )? )
            {
            root_0 = (CommonTree)adaptor.nil();

            _last = (CommonTree)input.LT(1);
            {
            CommonTree _save_last_1 = _last;
            CommonTree _first_1 = null;
            CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
            EXP9=(CommonTree)match(input,EXP,FOLLOW_EXP_in_exp150); 
            EXP9_tree = (CommonTree)adaptor.dupNode(EXP9);

            root_1 = (CommonTree)adaptor.becomeRoot(EXP9_tree, root_1);



            match(input, Token.DOWN, null); 
            _last = (CommonTree)input.LT(1);
            pushFollow(FOLLOW_expKey_in_exp152);
            expKey10=expKey();

            state._fsp--;

            adaptor.addChild(root_1, expKey10.getTree());
            // HintWalker.g:64:16: ( args )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( ((LA4_0>=ARGS && LA4_0<=ARG)) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // HintWalker.g:64:16: args
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_args_in_exp154);
                    args11=args();

                    state._fsp--;

                    adaptor.addChild(root_1, args11.getTree());

                    }
                    break;

            }


            match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
            }


            		retval.retKey =(expKey10!=null?expKey10.retVal:null);
            		if((args11!=null?args11.list:null)!=null){
            			retval.retVal =(args11!=null?args11.list:null);
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
    // $ANTLR end "exp"

    public static class expKey_return extends TreeRuleReturnScope {
        public Object retVal;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expKey"
    // HintWalker.g:72:1: expKey returns [Object retVal] : ( ID | ^( BIND_VAR '?' ) );
    public final HintWalker.expKey_return expKey() throws RecognitionException {
        HintWalker.expKey_return retval = new HintWalker.expKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ID12=null;
        CommonTree BIND_VAR13=null;
        CommonTree char_literal14=null;

        CommonTree ID12_tree=null;
        CommonTree BIND_VAR13_tree=null;
        CommonTree char_literal14_tree=null;

        try {
            // HintWalker.g:73:2: ( ID | ^( BIND_VAR '?' ) )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==ID) ) {
                alt5=1;
            }
            else if ( (LA5_0==BIND_VAR) ) {
                alt5=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;
            }
            switch (alt5) {
                case 1 :
                    // HintWalker.g:73:3: ID
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    ID12=(CommonTree)match(input,ID,FOLLOW_ID_in_expKey171); 
                    ID12_tree = (CommonTree)adaptor.dupNode(ID12);

                    adaptor.addChild(root_0, ID12_tree);


                    		retval.retVal =(ID12!=null?ID12.getText():null);
                    	

                    }
                    break;
                case 2 :
                    // HintWalker.g:76:3: ^( BIND_VAR '?' )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BIND_VAR13=(CommonTree)match(input,BIND_VAR,FOLLOW_BIND_VAR_in_expKey177); 
                    BIND_VAR13_tree = (CommonTree)adaptor.dupNode(BIND_VAR13);

                    root_1 = (CommonTree)adaptor.becomeRoot(BIND_VAR13_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    char_literal14=(CommonTree)match(input,22,FOLLOW_22_in_expKey179); 
                    char_literal14_tree = (CommonTree)adaptor.dupNode(char_literal14);

                    adaptor.addChild(root_1, char_literal14_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		retval.retVal =new BindVar(index);
                    		index++;
                    	

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
    // $ANTLR end "expKey"

    public static class args_return extends TreeRuleReturnScope {
        public List list;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "args"
    // HintWalker.g:82:1: args returns [List list] : ( ^( ARGS ( arg )* ) | arg );
    public final HintWalker.args_return args() throws RecognitionException {
        HintWalker.args_return retval = new HintWalker.args_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ARGS15=null;
        HintWalker.arg_return arg16 = null;

        HintWalker.arg_return arg17 = null;


        CommonTree ARGS15_tree=null;


        		retval.list =new ArrayList();
        	
        try {
            // HintWalker.g:85:3: ( ^( ARGS ( arg )* ) | arg )
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==ARGS) ) {
                alt7=1;
            }
            else if ( ((LA7_0>=BIND_VAR && LA7_0<=ARG)) ) {
                alt7=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 7, 0, input);

                throw nvae;
            }
            switch (alt7) {
                case 1 :
                    // HintWalker.g:85:4: ^( ARGS ( arg )* )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ARGS15=(CommonTree)match(input,ARGS,FOLLOW_ARGS_in_args200); 
                    ARGS15_tree = (CommonTree)adaptor.dupNode(ARGS15);

                    root_1 = (CommonTree)adaptor.becomeRoot(ARGS15_tree, root_1);



                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); 
                        // HintWalker.g:85:11: ( arg )*
                        loop6:
                        do {
                            int alt6=2;
                            int LA6_0 = input.LA(1);

                            if ( ((LA6_0>=BIND_VAR && LA6_0<=ARG)) ) {
                                alt6=1;
                            }


                            switch (alt6) {
                        	case 1 :
                        	    // HintWalker.g:85:12: arg
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    pushFollow(FOLLOW_arg_in_args203);
                        	    arg16=arg();

                        	    state._fsp--;

                        	    adaptor.addChild(root_1, arg16.getTree());
                        	    retval.list.add((arg16!=null?arg16.retVal:null));

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
                    break;
                case 2 :
                    // HintWalker.g:86:3: arg
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_arg_in_args211);
                    arg17=arg();

                    state._fsp--;

                    adaptor.addChild(root_0, arg17.getTree());

                    		retval.list.add((arg17!=null?arg17.retVal:null));
                    	

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
    // $ANTLR end "args"

    public static class arg_return extends TreeRuleReturnScope {
        public Object retVal;
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arg"
    // HintWalker.g:90:1: arg returns [Object retVal] : ( ^( ARG ID ) | ^( BIND_VAR '?' ) );
    public final HintWalker.arg_return arg() throws RecognitionException {
        HintWalker.arg_return retval = new HintWalker.arg_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree ARG18=null;
        CommonTree ID19=null;
        CommonTree BIND_VAR20=null;
        CommonTree char_literal21=null;

        CommonTree ARG18_tree=null;
        CommonTree ID19_tree=null;
        CommonTree BIND_VAR20_tree=null;
        CommonTree char_literal21_tree=null;

        try {
            // HintWalker.g:90:28: ( ^( ARG ID ) | ^( BIND_VAR '?' ) )
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==ARG) ) {
                alt8=1;
            }
            else if ( (LA8_0==BIND_VAR) ) {
                alt8=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 8, 0, input);

                throw nvae;
            }
            switch (alt8) {
                case 1 :
                    // HintWalker.g:91:2: ^( ARG ID )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    ARG18=(CommonTree)match(input,ARG,FOLLOW_ARG_in_arg227); 
                    ARG18_tree = (CommonTree)adaptor.dupNode(ARG18);

                    root_1 = (CommonTree)adaptor.becomeRoot(ARG18_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    ID19=(CommonTree)match(input,ID,FOLLOW_ID_in_arg229); 
                    ID19_tree = (CommonTree)adaptor.dupNode(ID19);

                    adaptor.addChild(root_1, ID19_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		retval.retVal =(ID19!=null?ID19.getText():null);
                    	

                    }
                    break;
                case 2 :
                    // HintWalker.g:95:3: ^( BIND_VAR '?' )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    CommonTree root_1 = (CommonTree)adaptor.nil();_last = (CommonTree)input.LT(1);
                    BIND_VAR20=(CommonTree)match(input,BIND_VAR,FOLLOW_BIND_VAR_in_arg238); 
                    BIND_VAR20_tree = (CommonTree)adaptor.dupNode(BIND_VAR20);

                    root_1 = (CommonTree)adaptor.becomeRoot(BIND_VAR20_tree, root_1);



                    match(input, Token.DOWN, null); 
                    _last = (CommonTree)input.LT(1);
                    char_literal21=(CommonTree)match(input,22,FOLLOW_22_in_arg240); 
                    char_literal21_tree = (CommonTree)adaptor.dupNode(char_literal21);

                    adaptor.addChild(root_1, char_literal21_tree);


                    match(input, Token.UP, null); adaptor.addChild(root_0, root_1);_last = _save_last_1;
                    }


                    		retval.retVal =new BindVar(index);
                    		index++;
                    	

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
    // $ANTLR end "arg"

    // Delegated rules


 

    public static final BitSet FOLLOW_hints_in_beg55 = new BitSet(new long[]{0x0000000000000012L});
    public static final BitSet FOLLOW_HINT_in_hints70 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ID_in_hints72 = new BitSet(new long[]{0x0000000000000060L});
    public static final BitSet FOLLOW_value_in_hints74 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_EXP_LIST_in_value92 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_exp_list_in_value94 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_exp_in_value102 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_exp_in_exp_list127 = new BitSet(new long[]{0x0000000000000062L});
    public static final BitSet FOLLOW_EXP_in_exp150 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_expKey_in_exp152 = new BitSet(new long[]{0x0000000000000388L});
    public static final BitSet FOLLOW_args_in_exp154 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ID_in_expKey171 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BIND_VAR_in_expKey177 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_22_in_expKey179 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_ARGS_in_args200 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_arg_in_args203 = new BitSet(new long[]{0x0000000000000388L});
    public static final BitSet FOLLOW_arg_in_args211 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ARG_in_arg227 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_ID_in_arg229 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_BIND_VAR_in_arg238 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_22_in_arg240 = new BitSet(new long[]{0x0000000000000008L});

}