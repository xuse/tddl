// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g 2011-08-26 12:28:44

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

public class MySQLDateParserParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "TIMESTRING", "DATE", "QUOTA", "TIME", "NS", "SPLIT", "WS", "ID", "P", "ARROW", "DOUBLEVERTBAR", "RPAREN", "LPAREN", "LTH", "NOT_EQ", "LEQ", "GEQ", "GTH", "'\\''"
    };
    public static final int NS=8;
    public static final int T__22=22;
    public static final int SPLIT=9;
    public static final int GTH=21;
    public static final int P=12;
    public static final int DOUBLEVERTBAR=14;
    public static final int ID=11;
    public static final int EOF=-1;
    public static final int LPAREN=16;
    public static final int NOT_EQ=18;
    public static final int TIMESTRING=4;
    public static final int LTH=17;
    public static final int TIME=7;
    public static final int RPAREN=15;
    public static final int WS=10;
    public static final int QUOTA=6;
    public static final int ARROW=13;
    public static final int GEQ=20;
    public static final int DATE=5;
    public static final int LEQ=19;

    // delegates
    // delegators


        public MySQLDateParserParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public MySQLDateParserParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return MySQLDateParserParser.tokenNames; }
    public String getGrammarFileName() { return "D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g"; }


    public static class quoted_string_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "quoted_string"
    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:23:1: quoted_string : time_quoted ;
    public final MySQLDateParserParser.quoted_string_return quoted_string() throws RecognitionException {
        MySQLDateParserParser.quoted_string_return retval = new MySQLDateParserParser.quoted_string_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLDateParserParser.time_quoted_return time_quoted1 = null;



        try {
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:24:2: ( time_quoted )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:24:4: time_quoted
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_time_quoted_in_quoted_string61);
            time_quoted1=time_quoted();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, time_quoted1.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
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
    // $ANTLR end "quoted_string"

    public static class time_quoted_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "time_quoted"
    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:26:1: time_quoted : ( ( '\\'' date_string ( time_string )? '\\'' )=> '\\'' datetime_string '\\'' -> datetime_string | quo );
    public final MySQLDateParserParser.time_quoted_return time_quoted() throws RecognitionException {
        MySQLDateParserParser.time_quoted_return retval = new MySQLDateParserParser.time_quoted_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal2=null;
        Token char_literal4=null;
        MySQLDateParserParser.datetime_string_return datetime_string3 = null;

        MySQLDateParserParser.quo_return quo5 = null;


        CommonTree char_literal2_tree=null;
        CommonTree char_literal4_tree=null;
        RewriteRuleTokenStream stream_22=new RewriteRuleTokenStream(adaptor,"token 22");
        RewriteRuleSubtreeStream stream_datetime_string=new RewriteRuleSubtreeStream(adaptor,"rule datetime_string");
        try {
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:27:2: ( ( '\\'' date_string ( time_string )? '\\'' )=> '\\'' datetime_string '\\'' -> datetime_string | quo )
            int alt1=2;
            alt1 = dfa1.predict(input);
            switch (alt1) {
                case 1 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:27:3: ( '\\'' date_string ( time_string )? '\\'' )=> '\\'' datetime_string '\\''
                    {
                    char_literal2=(Token)match(input,22,FOLLOW_22_in_time_quoted84); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_22.add(char_literal2);

                    pushFollow(FOLLOW_datetime_string_in_time_quoted86);
                    datetime_string3=datetime_string();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_datetime_string.add(datetime_string3.getTree());
                    char_literal4=(Token)match(input,22,FOLLOW_22_in_time_quoted89); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_22.add(char_literal4);



                    // AST REWRITE
                    // elements: datetime_string
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 27:69: -> datetime_string
                    {
                        adaptor.addChild(root_0, stream_datetime_string.nextTree());

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:28:3: quo
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_quo_in_time_quoted95);
                    quo5=quo();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, quo5.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
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
    // $ANTLR end "time_quoted"

    public static class datetime_string_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "datetime_string"
    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:30:1: datetime_string : ( date_string )? ( time_string )? -> ^( TIMESTRING date_string ( time_string )? ) ;
    public final MySQLDateParserParser.datetime_string_return datetime_string() throws RecognitionException {
        MySQLDateParserParser.datetime_string_return retval = new MySQLDateParserParser.datetime_string_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        MySQLDateParserParser.date_string_return date_string6 = null;

        MySQLDateParserParser.time_string_return time_string7 = null;


        RewriteRuleSubtreeStream stream_date_string=new RewriteRuleSubtreeStream(adaptor,"rule date_string");
        RewriteRuleSubtreeStream stream_time_string=new RewriteRuleSubtreeStream(adaptor,"rule time_string");
        try {
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:31:2: ( ( date_string )? ( time_string )? -> ^( TIMESTRING date_string ( time_string )? ) )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:31:4: ( date_string )? ( time_string )?
            {
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:31:4: ( date_string )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==NS) ) {
                int LA2_1 = input.LA(2);

                if ( (LA2_1==SPLIT) ) {
                    int LA2_3 = input.LA(3);

                    if ( (LA2_3==NS) ) {
                        int LA2_4 = input.LA(4);

                        if ( (LA2_4==SPLIT) ) {
                            int LA2_5 = input.LA(5);

                            if ( (LA2_5==NS) ) {
                                alt2=1;
                            }
                        }
                    }
                }
            }
            switch (alt2) {
                case 1 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:31:4: date_string
                    {
                    pushFollow(FOLLOW_date_string_in_datetime_string105);
                    date_string6=date_string();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_date_string.add(date_string6.getTree());

                    }
                    break;

            }

            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:31:18: ( time_string )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==NS) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:31:18: time_string
                    {
                    pushFollow(FOLLOW_time_string_in_datetime_string109);
                    time_string7=time_string();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_time_string.add(time_string7.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: date_string, time_string
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 31:31: -> ^( TIMESTRING date_string ( time_string )? )
            {
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:31:34: ^( TIMESTRING date_string ( time_string )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TIMESTRING, "TIMESTRING"), root_1);

                adaptor.addChild(root_1, stream_date_string.nextTree());
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:31:59: ( time_string )?
                if ( stream_time_string.hasNext() ) {
                    adaptor.addChild(root_1, stream_time_string.nextTree());

                }
                stream_time_string.reset();

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
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "datetime_string"

    public static class date_string_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "date_string"
    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:33:1: date_string : NS SPLIT NS SPLIT NS -> ( ^( DATE NS ) )+ ;
    public final MySQLDateParserParser.date_string_return date_string() throws RecognitionException {
        MySQLDateParserParser.date_string_return retval = new MySQLDateParserParser.date_string_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token NS8=null;
        Token SPLIT9=null;
        Token NS10=null;
        Token SPLIT11=null;
        Token NS12=null;

        CommonTree NS8_tree=null;
        CommonTree SPLIT9_tree=null;
        CommonTree NS10_tree=null;
        CommonTree SPLIT11_tree=null;
        CommonTree NS12_tree=null;
        RewriteRuleTokenStream stream_NS=new RewriteRuleTokenStream(adaptor,"token NS");
        RewriteRuleTokenStream stream_SPLIT=new RewriteRuleTokenStream(adaptor,"token SPLIT");

        try {
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:34:2: ( NS SPLIT NS SPLIT NS -> ( ^( DATE NS ) )+ )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:34:4: NS SPLIT NS SPLIT NS
            {
            NS8=(Token)match(input,NS,FOLLOW_NS_in_date_string131); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_NS.add(NS8);

            SPLIT9=(Token)match(input,SPLIT,FOLLOW_SPLIT_in_date_string133); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_SPLIT.add(SPLIT9);

            NS10=(Token)match(input,NS,FOLLOW_NS_in_date_string135); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_NS.add(NS10);

            SPLIT11=(Token)match(input,SPLIT,FOLLOW_SPLIT_in_date_string137); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_SPLIT.add(SPLIT11);

            NS12=(Token)match(input,NS,FOLLOW_NS_in_date_string139); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_NS.add(NS12);



            // AST REWRITE
            // elements: NS
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 34:24: -> ( ^( DATE NS ) )+
            {
                if ( !(stream_NS.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_NS.hasNext() ) {
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:34:26: ^( DATE NS )
                    {
                    CommonTree root_1 = (CommonTree)adaptor.nil();
                    root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(DATE, "DATE"), root_1);

                    adaptor.addChild(root_1, stream_NS.nextNode());

                    adaptor.addChild(root_0, root_1);
                    }

                }
                stream_NS.reset();

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
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
    // $ANTLR end "date_string"

    public static class time_string_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "time_string"
    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:45:1: time_string : NS SPLIT NS SPLIT NS -> ( ^( TIME NS ) )+ ;
    public final MySQLDateParserParser.time_string_return time_string() throws RecognitionException {
        MySQLDateParserParser.time_string_return retval = new MySQLDateParserParser.time_string_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token NS13=null;
        Token SPLIT14=null;
        Token NS15=null;
        Token SPLIT16=null;
        Token NS17=null;

        CommonTree NS13_tree=null;
        CommonTree SPLIT14_tree=null;
        CommonTree NS15_tree=null;
        CommonTree SPLIT16_tree=null;
        CommonTree NS17_tree=null;
        RewriteRuleTokenStream stream_NS=new RewriteRuleTokenStream(adaptor,"token NS");
        RewriteRuleTokenStream stream_SPLIT=new RewriteRuleTokenStream(adaptor,"token SPLIT");

        try {
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:46:2: ( NS SPLIT NS SPLIT NS -> ( ^( TIME NS ) )+ )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:46:4: NS SPLIT NS SPLIT NS
            {
            NS13=(Token)match(input,NS,FOLLOW_NS_in_time_string189); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_NS.add(NS13);

            SPLIT14=(Token)match(input,SPLIT,FOLLOW_SPLIT_in_time_string191); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_SPLIT.add(SPLIT14);

            NS15=(Token)match(input,NS,FOLLOW_NS_in_time_string193); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_NS.add(NS15);

            SPLIT16=(Token)match(input,SPLIT,FOLLOW_SPLIT_in_time_string195); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_SPLIT.add(SPLIT16);

            NS17=(Token)match(input,NS,FOLLOW_NS_in_time_string197); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_NS.add(NS17);



            // AST REWRITE
            // elements: NS
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 46:24: -> ( ^( TIME NS ) )+
            {
                if ( !(stream_NS.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_NS.hasNext() ) {
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:46:26: ^( TIME NS )
                    {
                    CommonTree root_1 = (CommonTree)adaptor.nil();
                    root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(TIME, "TIME"), root_1);

                    adaptor.addChild(root_1, stream_NS.nextNode());

                    adaptor.addChild(root_0, root_1);
                    }

                }
                stream_NS.reset();

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
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
    // $ANTLR end "time_string"

    public static class quo_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "quo"
    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:96:1: quo : '\\'' ( . )* '\\'' -> ^( QUOTA ID ) ;
    public final MySQLDateParserParser.quo_return quo() throws RecognitionException {
        MySQLDateParserParser.quo_return retval = new MySQLDateParserParser.quo_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal18=null;
        Token wildcard19=null;
        Token char_literal20=null;

        CommonTree char_literal18_tree=null;
        CommonTree wildcard19_tree=null;
        CommonTree char_literal20_tree=null;
        RewriteRuleTokenStream stream_22=new RewriteRuleTokenStream(adaptor,"token 22");

        try {
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:96:5: ( '\\'' ( . )* '\\'' -> ^( QUOTA ID ) )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:96:6: '\\'' ( . )* '\\''
            {
            char_literal18=(Token)match(input,22,FOLLOW_22_in_quo499); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_22.add(char_literal18);

            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:96:11: ( . )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==22) ) {
                    alt4=2;
                }
                else if ( ((LA4_0>=TIMESTRING && LA4_0<=GTH)) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:96:11: .
            	    {
            	    wildcard19=(Token)input.LT(1);
            	    matchAny(input); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    wildcard19_tree = (CommonTree)adaptor.create(wildcard19);
            	    adaptor.addChild(root_0, wildcard19_tree);
            	    }

            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);

            char_literal20=(Token)match(input,22,FOLLOW_22_in_quo504); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_22.add(char_literal20);



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
            // 96:18: -> ^( QUOTA ID )
            {
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:96:20: ^( QUOTA ID )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(QUOTA, "QUOTA"), root_1);

                adaptor.addChild(root_1, (CommonTree)adaptor.create(ID, "ID"));

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
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "quo"

    // $ANTLR start synpred1_MySQLDateParser
    public final void synpred1_MySQLDateParser_fragment() throws RecognitionException {   
        // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:27:3: ( '\\'' date_string ( time_string )? '\\'' )
        // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:27:4: '\\'' date_string ( time_string )? '\\''
        {
        match(input,22,FOLLOW_22_in_synpred1_MySQLDateParser72); if (state.failed) return ;
        pushFollow(FOLLOW_date_string_in_synpred1_MySQLDateParser74);
        date_string();

        state._fsp--;
        if (state.failed) return ;
        // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:27:21: ( time_string )?
        int alt5=2;
        int LA5_0 = input.LA(1);

        if ( (LA5_0==NS) ) {
            alt5=1;
        }
        switch (alt5) {
            case 1 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:27:21: time_string
                {
                pushFollow(FOLLOW_time_string_in_synpred1_MySQLDateParser76);
                time_string();

                state._fsp--;
                if (state.failed) return ;

                }
                break;

        }

        match(input,22,FOLLOW_22_in_synpred1_MySQLDateParser79); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred1_MySQLDateParser

    // Delegated rules

    public final boolean synpred1_MySQLDateParser() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred1_MySQLDateParser_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA1 dfa1 = new DFA1(this);
    static final String DFA1_eotS =
        "\17\uffff";
    static final String DFA1_eofS =
        "\17\uffff";
    static final String DFA1_minS =
        "\1\26\2\4\1\0\1\uffff\1\4\1\uffff\10\4";
    static final String DFA1_maxS =
        "\3\26\1\0\1\uffff\1\26\1\uffff\10\26";
    static final String DFA1_acceptS =
        "\4\uffff\1\2\1\uffff\1\1\10\uffff";
    static final String DFA1_specialS =
        "\3\uffff\1\0\13\uffff}>";
    static final String[] DFA1_transitionS = {
            "\1\1",
            "\4\4\1\2\15\4\1\3",
            "\5\4\1\5\15\4",
            "\1\uffff",
            "",
            "\4\4\1\7\16\4",
            "",
            "\5\4\1\10\15\4",
            "\4\4\1\11\16\4",
            "\4\4\1\12\15\4\1\3",
            "\5\4\1\13\15\4",
            "\4\4\1\14\16\4",
            "\5\4\1\15\15\4",
            "\4\4\1\16\16\4",
            "\22\4\1\3"
    };

    static final short[] DFA1_eot = DFA.unpackEncodedString(DFA1_eotS);
    static final short[] DFA1_eof = DFA.unpackEncodedString(DFA1_eofS);
    static final char[] DFA1_min = DFA.unpackEncodedStringToUnsignedChars(DFA1_minS);
    static final char[] DFA1_max = DFA.unpackEncodedStringToUnsignedChars(DFA1_maxS);
    static final short[] DFA1_accept = DFA.unpackEncodedString(DFA1_acceptS);
    static final short[] DFA1_special = DFA.unpackEncodedString(DFA1_specialS);
    static final short[][] DFA1_transition;

    static {
        int numStates = DFA1_transitionS.length;
        DFA1_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA1_transition[i] = DFA.unpackEncodedString(DFA1_transitionS[i]);
        }
    }

    class DFA1 extends DFA {

        public DFA1(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 1;
            this.eot = DFA1_eot;
            this.eof = DFA1_eof;
            this.min = DFA1_min;
            this.max = DFA1_max;
            this.accept = DFA1_accept;
            this.special = DFA1_special;
            this.transition = DFA1_transition;
        }
        public String getDescription() {
            return "26:1: time_quoted : ( ( '\\'' date_string ( time_string )? '\\'' )=> '\\'' datetime_string '\\'' -> datetime_string | quo );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA1_3 = input.LA(1);

                         
                        int index1_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred1_MySQLDateParser()) ) {s = 6;}

                        else if ( (true) ) {s = 4;}

                         
                        input.seek(index1_3);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 1, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_time_quoted_in_quoted_string61 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_22_in_time_quoted84 = new BitSet(new long[]{0x0000000000400100L});
    public static final BitSet FOLLOW_datetime_string_in_time_quoted86 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_22_in_time_quoted89 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_quo_in_time_quoted95 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_date_string_in_datetime_string105 = new BitSet(new long[]{0x0000000000000102L});
    public static final BitSet FOLLOW_time_string_in_datetime_string109 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NS_in_date_string131 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_SPLIT_in_date_string133 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_NS_in_date_string135 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_SPLIT_in_date_string137 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_NS_in_date_string139 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NS_in_time_string189 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_SPLIT_in_time_string191 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_NS_in_time_string193 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_SPLIT_in_time_string195 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_NS_in_time_string197 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_22_in_quo499 = new BitSet(new long[]{0x00000000007FFFF0L});
    public static final BitSet FOLLOW_22_in_quo504 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_22_in_synpred1_MySQLDateParser72 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_date_string_in_synpred1_MySQLDateParser74 = new BitSet(new long[]{0x0000000000400100L});
    public static final BitSet FOLLOW_time_string_in_synpred1_MySQLDateParser76 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_22_in_synpred1_MySQLDateParser79 = new BitSet(new long[]{0x0000000000000002L});

}