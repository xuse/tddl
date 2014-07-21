// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g 2011-08-26 12:28:44
 package  com.taobao.tddl.parser.mysql; 

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class MySQLDateParserLexer extends Lexer {
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

    public MySQLDateParserLexer() {;} 
    public MySQLDateParserLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public MySQLDateParserLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g"; }

    // $ANTLR start "T__22"
    public final void mT__22() throws RecognitionException {
        try {
            int _type = T__22;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:9:7: ( '\\'' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:9:9: '\\''
            {
            match('\''); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__22"

    // $ANTLR start "SPLIT"
    public final void mSPLIT() throws RecognitionException {
        try {
            int _type = SPLIT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:37:2: ( '-' | '*' | '.' | '+' | '/' | '@' | ':' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:
            {
            if ( (input.LA(1)>='*' && input.LA(1)<='+')||(input.LA(1)>='-' && input.LA(1)<='/')||input.LA(1)==':'||input.LA(1)=='@' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SPLIT"

    // $ANTLR start "NS"
    public final void mNS() throws RecognitionException {
        try {
            int _type = NS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:49:5: ( '0' .. '9' ( '0' .. '9' )* )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:49:7: '0' .. '9' ( '0' .. '9' )*
            {
            matchRange('0','9'); 
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:49:18: ( '0' .. '9' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:49:20: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "NS"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:52:5: ( ( ' ' | '\\t' | '\\r' '\\n' | '\\n' | '\\r' ) )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:52:9: ( ' ' | '\\t' | '\\r' '\\n' | '\\n' | '\\r' )
            {
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:52:9: ( ' ' | '\\t' | '\\r' '\\n' | '\\n' | '\\r' )
            int alt2=5;
            switch ( input.LA(1) ) {
            case ' ':
                {
                alt2=1;
                }
                break;
            case '\t':
                {
                alt2=2;
                }
                break;
            case '\r':
                {
                int LA2_3 = input.LA(2);

                if ( (LA2_3=='\n') ) {
                    alt2=3;
                }
                else {
                    alt2=5;}
                }
                break;
            case '\n':
                {
                alt2=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }

            switch (alt2) {
                case 1 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:52:13: ' '
                    {
                    match(' '); 

                    }
                    break;
                case 2 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:53:7: '\\t'
                    {
                    match('\t'); 

                    }
                    break;
                case 3 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:54:7: '\\r' '\\n'
                    {
                    match('\r'); 
                    match('\n'); 

                    }
                    break;
                case 4 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:55:7: '\\n'
                    {
                    match('\n'); 

                    }
                    break;
                case 5 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:56:7: '\\r'
                    {
                    match('\r'); 

                    }
                    break;

            }

            skip();

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "ID"
    public final void mID() throws RecognitionException {
        try {
            int _type = ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:61:5: ( ( 'A' .. 'Z' | 'a' .. 'z' ) ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )* )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:61:7: ( 'A' .. 'Z' | 'a' .. 'z' ) ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )*
            {
            if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:61:29: ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( ((LA3_0>='#' && LA3_0<='$')||(LA3_0>='0' && LA3_0<='9')||(LA3_0>='A' && LA3_0<='Z')||LA3_0=='_'||(LA3_0>='a' && LA3_0<='z')) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:
            	    {
            	    if ( (input.LA(1)>='#' && input.LA(1)<='$')||(input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ID"

    // $ANTLR start "P"
    public final void mP() throws RecognitionException {
        try {
            int _type = P;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:65:3: ( '%' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:65:5: '%'
            {
            match('%'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "P"

    // $ANTLR start "ARROW"
    public final void mARROW() throws RecognitionException {
        try {
            int _type = ARROW;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:68:2: ( '=>' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:68:4: '=>'
            {
            match("=>"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ARROW"

    // $ANTLR start "DOUBLEVERTBAR"
    public final void mDOUBLEVERTBAR() throws RecognitionException {
        try {
            int _type = DOUBLEVERTBAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:71:2: ( '||' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:71:4: '||'
            {
            match("||"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DOUBLEVERTBAR"

    // $ANTLR start "RPAREN"
    public final void mRPAREN() throws RecognitionException {
        try {
            int _type = RPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:76:2: ( ')' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:76:4: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RPAREN"

    // $ANTLR start "LPAREN"
    public final void mLPAREN() throws RecognitionException {
        try {
            int _type = LPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:79:2: ( '(' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:79:4: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LPAREN"

    // $ANTLR start "LTH"
    public final void mLTH() throws RecognitionException {
        try {
            int _type = LTH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:82:2: ( '<' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:82:4: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LTH"

    // $ANTLR start "NOT_EQ"
    public final void mNOT_EQ() throws RecognitionException {
        try {
            int _type = NOT_EQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:85:2: ( '<>' | '!=' | '^=' )
            int alt4=3;
            switch ( input.LA(1) ) {
            case '<':
                {
                alt4=1;
                }
                break;
            case '!':
                {
                alt4=2;
                }
                break;
            case '^':
                {
                alt4=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;
            }

            switch (alt4) {
                case 1 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:85:4: '<>'
                    {
                    match("<>"); 


                    }
                    break;
                case 2 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:85:11: '!='
                    {
                    match("!="); 


                    }
                    break;
                case 3 :
                    // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:85:18: '^='
                    {
                    match("^="); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "NOT_EQ"

    // $ANTLR start "LEQ"
    public final void mLEQ() throws RecognitionException {
        try {
            int _type = LEQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:88:2: ( '<=' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:88:4: '<='
            {
            match("<="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LEQ"

    // $ANTLR start "GEQ"
    public final void mGEQ() throws RecognitionException {
        try {
            int _type = GEQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:91:2: ( '>=' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:91:4: '>='
            {
            match(">="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "GEQ"

    // $ANTLR start "GTH"
    public final void mGTH() throws RecognitionException {
        try {
            int _type = GTH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:94:2: ( '>' )
            // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:94:4: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "GTH"

    public void mTokens() throws RecognitionException {
        // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:8: ( T__22 | SPLIT | NS | WS | ID | P | ARROW | DOUBLEVERTBAR | RPAREN | LPAREN | LTH | NOT_EQ | LEQ | GEQ | GTH )
        int alt5=15;
        alt5 = dfa5.predict(input);
        switch (alt5) {
            case 1 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:10: T__22
                {
                mT__22(); 

                }
                break;
            case 2 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:16: SPLIT
                {
                mSPLIT(); 

                }
                break;
            case 3 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:22: NS
                {
                mNS(); 

                }
                break;
            case 4 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:25: WS
                {
                mWS(); 

                }
                break;
            case 5 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:28: ID
                {
                mID(); 

                }
                break;
            case 6 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:31: P
                {
                mP(); 

                }
                break;
            case 7 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:33: ARROW
                {
                mARROW(); 

                }
                break;
            case 8 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:39: DOUBLEVERTBAR
                {
                mDOUBLEVERTBAR(); 

                }
                break;
            case 9 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:53: RPAREN
                {
                mRPAREN(); 

                }
                break;
            case 10 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:60: LPAREN
                {
                mLPAREN(); 

                }
                break;
            case 11 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:67: LTH
                {
                mLTH(); 

                }
                break;
            case 12 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:71: NOT_EQ
                {
                mNOT_EQ(); 

                }
                break;
            case 13 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:78: LEQ
                {
                mLEQ(); 

                }
                break;
            case 14 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:82: GEQ
                {
                mGEQ(); 

                }
                break;
            case 15 :
                // D:\\source\\merge-bak\\tddl-trunk-merge\\tddl-parser\\src\\main\\java\\com\\taobao\\tddl\\parser\\mysql\\MySQLDateParser.g:1:86: GTH
                {
                mGTH(); 

                }
                break;

        }

    }


    protected DFA5 dfa5 = new DFA5(this);
    static final String DFA5_eotS =
        "\13\uffff\1\17\1\uffff\1\21\4\uffff";
    static final String DFA5_eofS =
        "\22\uffff";
    static final String DFA5_minS =
        "\1\11\12\uffff\1\75\1\uffff\1\75\4\uffff";
    static final String DFA5_maxS =
        "\1\174\12\uffff\1\76\1\uffff\1\75\4\uffff";
    static final String DFA5_acceptS =
        "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\uffff\1\14"+
        "\1\uffff\1\15\1\13\1\16\1\17";
    static final String DFA5_specialS =
        "\22\uffff}>";
    static final String[] DFA5_transitionS = {
            "\2\4\2\uffff\1\4\22\uffff\1\4\1\14\3\uffff\1\6\1\uffff\1\1"+
            "\1\12\1\11\2\2\1\uffff\3\2\12\3\1\2\1\uffff\1\13\1\7\1\15\1"+
            "\uffff\1\2\32\5\3\uffff\1\14\2\uffff\32\5\1\uffff\1\10",
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
            "\1\16\1\14",
            "",
            "\1\20",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA5_eot = DFA.unpackEncodedString(DFA5_eotS);
    static final short[] DFA5_eof = DFA.unpackEncodedString(DFA5_eofS);
    static final char[] DFA5_min = DFA.unpackEncodedStringToUnsignedChars(DFA5_minS);
    static final char[] DFA5_max = DFA.unpackEncodedStringToUnsignedChars(DFA5_maxS);
    static final short[] DFA5_accept = DFA.unpackEncodedString(DFA5_acceptS);
    static final short[] DFA5_special = DFA.unpackEncodedString(DFA5_specialS);
    static final short[][] DFA5_transition;

    static {
        int numStates = DFA5_transitionS.length;
        DFA5_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA5_transition[i] = DFA.unpackEncodedString(DFA5_transitionS[i]);
        }
    }

    class DFA5 extends DFA {

        public DFA5(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 5;
            this.eot = DFA5_eot;
            this.eof = DFA5_eof;
            this.min = DFA5_min;
            this.max = DFA5_max;
            this.accept = DFA5_accept;
            this.special = DFA5_special;
            this.transition = DFA5_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__22 | SPLIT | NS | WS | ID | P | ARROW | DOUBLEVERTBAR | RPAREN | LPAREN | LTH | NOT_EQ | LEQ | GEQ | GTH );";
        }
    }
 

}
