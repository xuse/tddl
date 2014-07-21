// $ANTLR 3.1.1 HintParser.g 2009-08-21 15:33:53

	package com.taobao.tddl.parser.hint;


import org.antlr.runtime.BitSet;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.ParserRuleReturnScope;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.RewriteRuleSubtreeStream;
import org.antlr.runtime.tree.RewriteRuleTokenStream;
import org.antlr.runtime.tree.TreeAdaptor;

public class HintParserParser extends Parser {
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


        public HintParserParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public HintParserParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return HintParserParser.tokenNames; }
    public String getGrammarFileName() { return "HintParser.g"; }


    public static class hints_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "hints"
    // HintParser.g:21:1: hints : DIVIDE ASTERISK ( hint )* ASTERISK DIVIDE ;
    public final HintParserParser.hints_return hints() throws RecognitionException {
        HintParserParser.hints_return retval = new HintParserParser.hints_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DIVIDE1=null;
        Token ASTERISK2=null;
        Token ASTERISK4=null;
        Token DIVIDE5=null;
        HintParserParser.hint_return hint3 = null;


        CommonTree DIVIDE1_tree=null;
        CommonTree ASTERISK2_tree=null;
        CommonTree ASTERISK4_tree=null;
        CommonTree DIVIDE5_tree=null;

        try {
            // HintParser.g:21:7: ( DIVIDE ASTERISK ( hint )* ASTERISK DIVIDE )
            // HintParser.g:22:2: DIVIDE ASTERISK ( hint )* ASTERISK DIVIDE
            {
            root_0 = (CommonTree)adaptor.nil();

            DIVIDE1=(Token)match(input,DIVIDE,FOLLOW_DIVIDE_in_hints68); 
            ASTERISK2=(Token)match(input,ASTERISK,FOLLOW_ASTERISK_in_hints71); 
            // HintParser.g:22:20: ( hint )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==ID) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // HintParser.g:22:20: hint
            	    {
            	    pushFollow(FOLLOW_hint_in_hints74);
            	    hint3=hint();

            	    state._fsp--;

            	    adaptor.addChild(root_0, hint3.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            ASTERISK4=(Token)match(input,ASTERISK,FOLLOW_ASTERISK_in_hints77); 
            DIVIDE5=(Token)match(input,DIVIDE,FOLLOW_DIVIDE_in_hints80); 

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "hints"

    public static class hint_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "hint"
    // HintParser.g:25:1: hint : ID COLON value -> ^( HINT ID value ) ;
    public final HintParserParser.hint_return hint() throws RecognitionException {
        HintParserParser.hint_return retval = new HintParserParser.hint_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID6=null;
        Token COLON7=null;
        HintParserParser.value_return value8 = null;


        CommonTree ID6_tree=null;
        CommonTree COLON7_tree=null;
        RewriteRuleTokenStream stream_COLON=new RewriteRuleTokenStream(adaptor,"token COLON");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value");
        try {
            // HintParser.g:25:6: ( ID COLON value -> ^( HINT ID value ) )
            // HintParser.g:26:2: ID COLON value
            {
            ID6=(Token)match(input,ID,FOLLOW_ID_in_hint93);  
            stream_ID.add(ID6);

            COLON7=(Token)match(input,COLON,FOLLOW_COLON_in_hint95);  
            stream_COLON.add(COLON7);

            pushFollow(FOLLOW_value_in_hint97);
            value8=value();

            state._fsp--;

            stream_value.add(value8.getTree());


            // AST REWRITE
            // elements: value, ID
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 26:17: -> ^( HINT ID value )
            {
                // HintParser.g:26:19: ^( HINT ID value )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(HINT, "HINT"), root_1);

                adaptor.addChild(root_1, stream_ID.nextNode());
                adaptor.addChild(root_1, stream_value.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "hint"

    public static class value_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "value"
    // HintParser.g:28:1: value : ( exp_list -> ^( EXP_LIST exp_list ) | exp );
    public final HintParserParser.value_return value() throws RecognitionException {
        HintParserParser.value_return retval = new HintParserParser.value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        HintParserParser.exp_list_return exp_list9 = null;

        HintParserParser.exp_return exp10 = null;


        RewriteRuleSubtreeStream stream_exp_list=new RewriteRuleSubtreeStream(adaptor,"rule exp_list");
        try {
            // HintParser.g:28:7: ( exp_list -> ^( EXP_LIST exp_list ) | exp )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==20) ) {
                alt2=1;
            }
            else if ( (LA2_0==ID||LA2_0==22) ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // HintParser.g:29:2: exp_list
                    {
                    pushFollow(FOLLOW_exp_list_in_value116);
                    exp_list9=exp_list();

                    state._fsp--;

                    stream_exp_list.add(exp_list9.getTree());


                    // AST REWRITE
                    // elements: exp_list
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 29:11: -> ^( EXP_LIST exp_list )
                    {
                        // HintParser.g:29:13: ^( EXP_LIST exp_list )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(EXP_LIST, "EXP_LIST"), root_1);

                        adaptor.addChild(root_1, stream_exp_list.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // HintParser.g:30:3: exp
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_exp_in_value127);
                    exp10=exp();

                    state._fsp--;

                    adaptor.addChild(root_0, exp10.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "value"

    public static class exp_list_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "exp_list"
    // HintParser.g:32:1: exp_list : '{' exps '}' ;
    public final HintParserParser.exp_list_return exp_list() throws RecognitionException {
        HintParserParser.exp_list_return retval = new HintParserParser.exp_list_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal11=null;
        Token char_literal13=null;
        HintParserParser.exps_return exps12 = null;


        CommonTree char_literal11_tree=null;
        CommonTree char_literal13_tree=null;

        try {
            // HintParser.g:32:9: ( '{' exps '}' )
            // HintParser.g:33:2: '{' exps '}'
            {
            root_0 = (CommonTree)adaptor.nil();

            char_literal11=(Token)match(input,20,FOLLOW_20_in_exp_list136); 
            pushFollow(FOLLOW_exps_in_exp_list139);
            exps12=exps();

            state._fsp--;

            adaptor.addChild(root_0, exps12.getTree());
            char_literal13=(Token)match(input,21,FOLLOW_21_in_exp_list141); 

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "exp_list"

    public static class exps_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "exps"
    // HintParser.g:35:1: exps : exp ( SEMICOLON exp )* ;
    public final HintParserParser.exps_return exps() throws RecognitionException {
        HintParserParser.exps_return retval = new HintParserParser.exps_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SEMICOLON15=null;
        HintParserParser.exp_return exp14 = null;

        HintParserParser.exp_return exp16 = null;


        CommonTree SEMICOLON15_tree=null;

        try {
            // HintParser.g:35:6: ( exp ( SEMICOLON exp )* )
            // HintParser.g:36:2: exp ( SEMICOLON exp )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_exp_in_exps152);
            exp14=exp();

            state._fsp--;

            adaptor.addChild(root_0, exp14.getTree());
            // HintParser.g:36:6: ( SEMICOLON exp )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==SEMICOLON) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // HintParser.g:36:7: SEMICOLON exp
            	    {
            	    SEMICOLON15=(Token)match(input,SEMICOLON,FOLLOW_SEMICOLON_in_exps155); 
            	    pushFollow(FOLLOW_exp_in_exps158);
            	    exp16=exp();

            	    state._fsp--;

            	    adaptor.addChild(root_0, exp16.getTree());

            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "exps"

    public static class exp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "exp"
    // HintParser.g:38:1: exp : expKey ( COLON )? ( args )? -> ^( EXP expKey ( args )? ) ;
    public final HintParserParser.exp_return exp() throws RecognitionException {
        HintParserParser.exp_return retval = new HintParserParser.exp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COLON18=null;
        HintParserParser.expKey_return expKey17 = null;

        HintParserParser.args_return args19 = null;


        CommonTree COLON18_tree=null;
        RewriteRuleTokenStream stream_COLON=new RewriteRuleTokenStream(adaptor,"token COLON");
        RewriteRuleSubtreeStream stream_expKey=new RewriteRuleSubtreeStream(adaptor,"rule expKey");
        RewriteRuleSubtreeStream stream_args=new RewriteRuleSubtreeStream(adaptor,"rule args");
        try {
            // HintParser.g:38:5: ( expKey ( COLON )? ( args )? -> ^( EXP expKey ( args )? ) )
            // HintParser.g:38:6: expKey ( COLON )? ( args )?
            {
            pushFollow(FOLLOW_expKey_in_exp168);
            expKey17=expKey();

            state._fsp--;

            stream_expKey.add(expKey17.getTree());
            // HintParser.g:38:13: ( COLON )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==COLON) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // HintParser.g:38:13: COLON
                    {
                    COLON18=(Token)match(input,COLON,FOLLOW_COLON_in_exp170);  
                    stream_COLON.add(COLON18);


                    }
                    break;

            }

            // HintParser.g:38:20: ( args )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( ((LA5_0>=22 && LA5_0<=23)) ) {
                alt5=1;
            }
            else if ( (LA5_0==ID) ) {
                int LA5_2 = input.LA(2);

                if ( ((LA5_2>=ASTERISK && LA5_2<=ID)||LA5_2==SEMICOLON||LA5_2==21) ) {
                    alt5=1;
                }
            }
            switch (alt5) {
                case 1 :
                    // HintParser.g:38:20: args
                    {
                    pushFollow(FOLLOW_args_in_exp173);
                    args19=args();

                    state._fsp--;

                    stream_args.add(args19.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: args, expKey
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 38:25: -> ^( EXP expKey ( args )? )
            {
                // HintParser.g:38:27: ^( EXP expKey ( args )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(EXP, "EXP"), root_1);

                adaptor.addChild(root_1, stream_expKey.nextTree());
                // HintParser.g:38:40: ( args )?
                if ( stream_args.hasNext() ) {
                    adaptor.addChild(root_1, stream_args.nextTree());

                }
                stream_args.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "exp"

    public static class expKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expKey"
    // HintParser.g:40:1: expKey : ( ID | '?' -> ^( BIND_VAR '?' ) );
    public final HintParserParser.expKey_return expKey() throws RecognitionException {
        HintParserParser.expKey_return retval = new HintParserParser.expKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID20=null;
        Token char_literal21=null;

        CommonTree ID20_tree=null;
        CommonTree char_literal21_tree=null;
        RewriteRuleTokenStream stream_22=new RewriteRuleTokenStream(adaptor,"token 22");

        try {
            // HintParser.g:40:7: ( ID | '?' -> ^( BIND_VAR '?' ) )
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==ID) ) {
                alt6=1;
            }
            else if ( (LA6_0==22) ) {
                alt6=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;
            }
            switch (alt6) {
                case 1 :
                    // HintParser.g:41:2: ID
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    ID20=(Token)match(input,ID,FOLLOW_ID_in_expKey192); 
                    ID20_tree = (CommonTree)adaptor.create(ID20);
                    adaptor.addChild(root_0, ID20_tree);


                    }
                    break;
                case 2 :
                    // HintParser.g:42:3: '?'
                    {
                    char_literal21=(Token)match(input,22,FOLLOW_22_in_expKey196);  
                    stream_22.add(char_literal21);



                    // AST REWRITE
                    // elements: 22
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 42:6: -> ^( BIND_VAR '?' )
                    {
                        // HintParser.g:42:8: ^( BIND_VAR '?' )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(BIND_VAR, "BIND_VAR"), root_1);

                        adaptor.addChild(root_1, stream_22.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expKey"

    public static class args_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "args"
    // HintParser.g:44:1: args : ( '[' arg ( COMMA arg )* ']' -> ^( ARGS ( arg )* ) | arg );
    public final HintParserParser.args_return args() throws RecognitionException {
        HintParserParser.args_return retval = new HintParserParser.args_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal22=null;
        Token COMMA24=null;
        Token char_literal26=null;
        HintParserParser.arg_return arg23 = null;

        HintParserParser.arg_return arg25 = null;

        HintParserParser.arg_return arg27 = null;


        CommonTree char_literal22_tree=null;
        CommonTree COMMA24_tree=null;
        CommonTree char_literal26_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_23=new RewriteRuleTokenStream(adaptor,"token 23");
        RewriteRuleTokenStream stream_24=new RewriteRuleTokenStream(adaptor,"token 24");
        RewriteRuleSubtreeStream stream_arg=new RewriteRuleSubtreeStream(adaptor,"rule arg");
        try {
            // HintParser.g:44:5: ( '[' arg ( COMMA arg )* ']' -> ^( ARGS ( arg )* ) | arg )
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==23) ) {
                alt8=1;
            }
            else if ( (LA8_0==ID||LA8_0==22) ) {
                alt8=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 8, 0, input);

                throw nvae;
            }
            switch (alt8) {
                case 1 :
                    // HintParser.g:45:2: '[' arg ( COMMA arg )* ']'
                    {
                    char_literal22=(Token)match(input,23,FOLLOW_23_in_args211);  
                    stream_23.add(char_literal22);

                    pushFollow(FOLLOW_arg_in_args213);
                    arg23=arg();

                    state._fsp--;

                    stream_arg.add(arg23.getTree());
                    // HintParser.g:45:10: ( COMMA arg )*
                    loop7:
                    do {
                        int alt7=2;
                        int LA7_0 = input.LA(1);

                        if ( (LA7_0==COMMA) ) {
                            alt7=1;
                        }


                        switch (alt7) {
                    	case 1 :
                    	    // HintParser.g:45:11: COMMA arg
                    	    {
                    	    COMMA24=(Token)match(input,COMMA,FOLLOW_COMMA_in_args216);  
                    	    stream_COMMA.add(COMMA24);

                    	    pushFollow(FOLLOW_arg_in_args218);
                    	    arg25=arg();

                    	    state._fsp--;

                    	    stream_arg.add(arg25.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop7;
                        }
                    } while (true);

                    char_literal26=(Token)match(input,24,FOLLOW_24_in_args222);  
                    stream_24.add(char_literal26);



                    // AST REWRITE
                    // elements: arg
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 45:28: -> ^( ARGS ( arg )* )
                    {
                        // HintParser.g:45:30: ^( ARGS ( arg )* )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ARGS, "ARGS"), root_1);

                        // HintParser.g:45:37: ( arg )*
                        while ( stream_arg.hasNext() ) {
                            adaptor.addChild(root_1, stream_arg.nextTree());

                        }
                        stream_arg.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // HintParser.g:46:3: arg
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_arg_in_args235);
                    arg27=arg();

                    state._fsp--;

                    adaptor.addChild(root_0, arg27.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "args"

    public static class arg_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arg"
    // HintParser.g:49:1: arg : ( ID -> ^( ARG ID ) | '?' -> ^( BIND_VAR '?' ) );
    public final HintParserParser.arg_return arg() throws RecognitionException {
        HintParserParser.arg_return retval = new HintParserParser.arg_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID28=null;
        Token char_literal29=null;

        CommonTree ID28_tree=null;
        CommonTree char_literal29_tree=null;
        RewriteRuleTokenStream stream_22=new RewriteRuleTokenStream(adaptor,"token 22");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            // HintParser.g:49:6: ( ID -> ^( ARG ID ) | '?' -> ^( BIND_VAR '?' ) )
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0==ID) ) {
                alt9=1;
            }
            else if ( (LA9_0==22) ) {
                alt9=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 9, 0, input);

                throw nvae;
            }
            switch (alt9) {
                case 1 :
                    // HintParser.g:50:2: ID
                    {
                    ID28=(Token)match(input,ID,FOLLOW_ID_in_arg249);  
                    stream_ID.add(ID28);



                    // AST REWRITE
                    // elements: ID
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 50:5: -> ^( ARG ID )
                    {
                        // HintParser.g:50:7: ^( ARG ID )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ARG, "ARG"), root_1);

                        adaptor.addChild(root_1, stream_ID.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // HintParser.g:51:3: '?'
                    {
                    char_literal29=(Token)match(input,22,FOLLOW_22_in_arg260);  
                    stream_22.add(char_literal29);



                    // AST REWRITE
                    // elements: 22
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 51:7: -> ^( BIND_VAR '?' )
                    {
                        // HintParser.g:51:9: ^( BIND_VAR '?' )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(BIND_VAR, "BIND_VAR"), root_1);

                        adaptor.addChild(root_1, stream_22.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "arg"

    // Delegated rules


 

    public static final BitSet FOLLOW_DIVIDE_in_hints68 = new BitSet(new long[]{0x0000000000000800L});
    public static final BitSet FOLLOW_ASTERISK_in_hints71 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_hint_in_hints74 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_ASTERISK_in_hints77 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_DIVIDE_in_hints80 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_hint93 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_COLON_in_hint95 = new BitSet(new long[]{0x0000000000501000L});
    public static final BitSet FOLLOW_value_in_hint97 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_exp_list_in_value116 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_exp_in_value127 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_20_in_exp_list136 = new BitSet(new long[]{0x0000000000501000L});
    public static final BitSet FOLLOW_exps_in_exp_list139 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_21_in_exp_list141 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_exp_in_exps152 = new BitSet(new long[]{0x0000000000004002L});
    public static final BitSet FOLLOW_SEMICOLON_in_exps155 = new BitSet(new long[]{0x0000000000501000L});
    public static final BitSet FOLLOW_exp_in_exps158 = new BitSet(new long[]{0x0000000000004002L});
    public static final BitSet FOLLOW_expKey_in_exp168 = new BitSet(new long[]{0x0000000000C03002L});
    public static final BitSet FOLLOW_COLON_in_exp170 = new BitSet(new long[]{0x0000000000C01002L});
    public static final BitSet FOLLOW_args_in_exp173 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_expKey192 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_22_in_expKey196 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_23_in_args211 = new BitSet(new long[]{0x0000000000C01000L});
    public static final BitSet FOLLOW_arg_in_args213 = new BitSet(new long[]{0x0000000001008000L});
    public static final BitSet FOLLOW_COMMA_in_args216 = new BitSet(new long[]{0x0000000000C01000L});
    public static final BitSet FOLLOW_arg_in_args218 = new BitSet(new long[]{0x0000000001008000L});
    public static final BitSet FOLLOW_24_in_args222 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arg_in_args235 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_arg249 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_22_in_arg260 = new BitSet(new long[]{0x0000000000000002L});

}
