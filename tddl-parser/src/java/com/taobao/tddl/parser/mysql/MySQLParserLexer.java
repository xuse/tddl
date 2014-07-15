// $ANTLR 3.2 Sep 23, 2009 12:02:23 D:\\tools\\antlr\\test\\MySQLParser.g 2011-09-13 15:44:51
 package  com.taobao.tddl.parser.mysql; 

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class MySQLParserLexer extends Lexer {
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

    public MySQLParserLexer() {;} 
    public MySQLParserLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public MySQLParserLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "D:\\tools\\antlr\\test\\MySQLParser.g"; }

    // $ANTLR start "T__99"
    public final void mT__99() throws RecognitionException {
        try {
            int _type = T__99;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:9:7: ( 'SET' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:9:9: 'SET'
            {
            match("SET"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__99"

    // $ANTLR start "T__100"
    public final void mT__100() throws RecognitionException {
        try {
            int _type = T__100;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:10:8: ( 'INSERT' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:10:10: 'INSERT'
            {
            match("INSERT"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__100"

    // $ANTLR start "T__101"
    public final void mT__101() throws RecognitionException {
        try {
            int _type = T__101;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:11:8: ( 'INTO' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:11:10: 'INTO'
            {
            match("INTO"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__101"

    // $ANTLR start "T__102"
    public final void mT__102() throws RecognitionException {
        try {
            int _type = T__102;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:12:8: ( 'VALUES' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:12:10: 'VALUES'
            {
            match("VALUES"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__102"

    // $ANTLR start "T__103"
    public final void mT__103() throws RecognitionException {
        try {
            int _type = T__103;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:13:8: ( 'REPLACE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:13:10: 'REPLACE'
            {
            match("REPLACE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__103"

    // $ANTLR start "T__104"
    public final void mT__104() throws RecognitionException {
        try {
            int _type = T__104;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:14:8: ( 'GROUP' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:14:10: 'GROUP'
            {
            match("GROUP"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__104"

    // $ANTLR start "T__105"
    public final void mT__105() throws RecognitionException {
        try {
            int _type = T__105;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:15:8: ( 'BY' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:15:10: 'BY'
            {
            match("BY"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__105"

    // $ANTLR start "T__106"
    public final void mT__106() throws RecognitionException {
        try {
            int _type = T__106;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:16:8: ( 'HAVING' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:16:10: 'HAVING'
            {
            match("HAVING"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__106"

    // $ANTLR start "T__107"
    public final void mT__107() throws RecognitionException {
        try {
            int _type = T__107;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:17:8: ( 'ORDER' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:17:10: 'ORDER'
            {
            match("ORDER"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__107"

    // $ANTLR start "T__108"
    public final void mT__108() throws RecognitionException {
        try {
            int _type = T__108;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:18:8: ( 'SELECT' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:18:10: 'SELECT'
            {
            match("SELECT"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__108"

    // $ANTLR start "T__109"
    public final void mT__109() throws RecognitionException {
        try {
            int _type = T__109;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:19:8: ( 'DISTINCT' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:19:10: 'DISTINCT'
            {
            match("DISTINCT"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__109"

    // $ANTLR start "T__110"
    public final void mT__110() throws RecognitionException {
        try {
            int _type = T__110;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:20:8: ( 'WHERE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:20:10: 'WHERE'
            {
            match("WHERE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__110"

    // $ANTLR start "T__111"
    public final void mT__111() throws RecognitionException {
        try {
            int _type = T__111;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:21:8: ( 'NOT' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:21:10: 'NOT'
            {
            match("NOT"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__111"

    // $ANTLR start "T__112"
    public final void mT__112() throws RecognitionException {
        try {
            int _type = T__112;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:22:8: ( 'OR' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:22:10: 'OR'
            {
            match("OR"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__112"

    // $ANTLR start "T__113"
    public final void mT__113() throws RecognitionException {
        try {
            int _type = T__113;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:23:8: ( 'AND' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:23:10: 'AND'
            {
            match("AND"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__113"

    // $ANTLR start "T__114"
    public final void mT__114() throws RecognitionException {
        try {
            int _type = T__114;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:24:8: ( 'BETWEEN' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:24:10: 'BETWEEN'
            {
            match("BETWEEN"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__114"

    // $ANTLR start "T__115"
    public final void mT__115() throws RecognitionException {
        try {
            int _type = T__115;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:25:8: ( 'IS' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:25:10: 'IS'
            {
            match("IS"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__115"

    // $ANTLR start "T__116"
    public final void mT__116() throws RecognitionException {
        try {
            int _type = T__116;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:26:8: ( 'NAN' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:26:10: 'NAN'
            {
            match("NAN"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__116"

    // $ANTLR start "T__117"
    public final void mT__117() throws RecognitionException {
        try {
            int _type = T__117;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:27:8: ( 'INFINITE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:27:10: 'INFINITE'
            {
            match("INFINITE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__117"

    // $ANTLR start "T__118"
    public final void mT__118() throws RecognitionException {
        try {
            int _type = T__118;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:28:8: ( 'NULL' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:28:10: 'NULL'
            {
            match("NULL"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__118"

    // $ANTLR start "T__119"
    public final void mT__119() throws RecognitionException {
        try {
            int _type = T__119;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:29:8: ( 'IN' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:29:10: 'IN'
            {
            match("IN"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__119"

    // $ANTLR start "T__120"
    public final void mT__120() throws RecognitionException {
        try {
            int _type = T__120;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:30:8: ( 'LIKE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:30:10: 'LIKE'
            {
            match("LIKE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__120"

    // $ANTLR start "T__121"
    public final void mT__121() throws RecognitionException {
        try {
            int _type = T__121;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:31:8: ( 'ROWNUM' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:31:10: 'ROWNUM'
            {
            match("ROWNUM"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__121"

    // $ANTLR start "T__122"
    public final void mT__122() throws RecognitionException {
        try {
            int _type = T__122;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:32:8: ( '?' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:32:10: '?'
            {
            match('?'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__122"

    // $ANTLR start "T__123"
    public final void mT__123() throws RecognitionException {
        try {
            int _type = T__123;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:33:8: ( 'FROM' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:33:10: 'FROM'
            {
            match("FROM"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__123"

    // $ANTLR start "T__124"
    public final void mT__124() throws RecognitionException {
        try {
            int _type = T__124;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:34:8: ( 'JOIN' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:34:10: 'JOIN'
            {
            match("JOIN"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__124"

    // $ANTLR start "T__125"
    public final void mT__125() throws RecognitionException {
        try {
            int _type = T__125;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:35:8: ( 'ON' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:35:10: 'ON'
            {
            match("ON"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__125"

    // $ANTLR start "T__126"
    public final void mT__126() throws RecognitionException {
        try {
            int _type = T__126;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:36:8: ( 'INNER' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:36:10: 'INNER'
            {
            match("INNER"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__126"

    // $ANTLR start "T__127"
    public final void mT__127() throws RecognitionException {
        try {
            int _type = T__127;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:37:8: ( 'LEFT' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:37:10: 'LEFT'
            {
            match("LEFT"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__127"

    // $ANTLR start "T__128"
    public final void mT__128() throws RecognitionException {
        try {
            int _type = T__128;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:38:8: ( 'RIGHT' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:38:10: 'RIGHT'
            {
            match("RIGHT"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__128"

    // $ANTLR start "T__129"
    public final void mT__129() throws RecognitionException {
        try {
            int _type = T__129;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:39:8: ( 'FULL' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:39:10: 'FULL'
            {
            match("FULL"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__129"

    // $ANTLR start "T__130"
    public final void mT__130() throws RecognitionException {
        try {
            int _type = T__130;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:40:8: ( 'UNION' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:40:10: 'UNION'
            {
            match("UNION"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__130"

    // $ANTLR start "T__131"
    public final void mT__131() throws RecognitionException {
        try {
            int _type = T__131;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:41:8: ( 'CROSS' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:41:10: 'CROSS'
            {
            match("CROSS"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__131"

    // $ANTLR start "T__132"
    public final void mT__132() throws RecognitionException {
        try {
            int _type = T__132;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:42:8: ( 'OUTER' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:42:10: 'OUTER'
            {
            match("OUTER"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__132"

    // $ANTLR start "T__133"
    public final void mT__133() throws RecognitionException {
        try {
            int _type = T__133;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:43:8: ( 'TRUE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:43:10: 'TRUE'
            {
            match("TRUE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__133"

    // $ANTLR start "T__134"
    public final void mT__134() throws RecognitionException {
        try {
            int _type = T__134;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:44:8: ( 'FALSE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:44:10: 'FALSE'
            {
            match("FALSE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__134"

    // $ANTLR start "T__135"
    public final void mT__135() throws RecognitionException {
        try {
            int _type = T__135;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:45:8: ( 'AS' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:45:10: 'AS'
            {
            match("AS"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__135"

    // $ANTLR start "T__136"
    public final void mT__136() throws RecognitionException {
        try {
            int _type = T__136;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:46:8: ( 'FORCE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:46:10: 'FORCE'
            {
            match("FORCE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__136"

    // $ANTLR start "T__137"
    public final void mT__137() throws RecognitionException {
        try {
            int _type = T__137;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:47:8: ( 'INDEX' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:47:10: 'INDEX'
            {
            match("INDEX"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__137"

    // $ANTLR start "T__138"
    public final void mT__138() throws RecognitionException {
        try {
            int _type = T__138;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:48:8: ( 'IGNORE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:48:10: 'IGNORE'
            {
            match("IGNORE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__138"

    // $ANTLR start "T__139"
    public final void mT__139() throws RecognitionException {
        try {
            int _type = T__139;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:49:8: ( 'DELETE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:49:10: 'DELETE'
            {
            match("DELETE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__139"

    // $ANTLR start "T__140"
    public final void mT__140() throws RecognitionException {
        try {
            int _type = T__140;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:50:8: ( 'UPDATE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:50:10: 'UPDATE'
            {
            match("UPDATE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__140"

    // $ANTLR start "T__141"
    public final void mT__141() throws RecognitionException {
        try {
            int _type = T__141;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:51:8: ( 'LIMIT' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:51:10: 'LIMIT'
            {
            match("LIMIT"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__141"

    // $ANTLR start "T__142"
    public final void mT__142() throws RecognitionException {
        try {
            int _type = T__142;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:52:8: ( 'FOR' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:52:10: 'FOR'
            {
            match("FOR"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__142"

    // $ANTLR start "T__143"
    public final void mT__143() throws RecognitionException {
        try {
            int _type = T__143;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:53:8: ( 'LOCK' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:53:10: 'LOCK'
            {
            match("LOCK"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__143"

    // $ANTLR start "T__144"
    public final void mT__144() throws RecognitionException {
        try {
            int _type = T__144;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:54:8: ( 'SHARE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:54:10: 'SHARE'
            {
            match("SHARE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__144"

    // $ANTLR start "T__145"
    public final void mT__145() throws RecognitionException {
        try {
            int _type = T__145;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:55:8: ( 'MODE' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:55:10: 'MODE'
            {
            match("MODE"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__145"

    // $ANTLR start "INTERVAL"
    public final void mINTERVAL() throws RecognitionException {
        try {
            int _type = INTERVAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:340:9: ( 'INTERVAL' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:340:10: 'INTERVAL'
            {
            match("INTERVAL"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "INTERVAL"

    // $ANTLR start "N"
    public final void mN() throws RecognitionException {
        try {
            int _type = N;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:484:5: ( '0' .. '9' ( '0' .. '9' )* )
            // D:\\tools\\antlr\\test\\MySQLParser.g:484:7: '0' .. '9' ( '0' .. '9' )*
            {
            matchRange('0','9'); 
            // D:\\tools\\antlr\\test\\MySQLParser.g:484:18: ( '0' .. '9' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:484:20: '0' .. '9'
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
    // $ANTLR end "N"

    // $ANTLR start "ASC"
    public final void mASC() throws RecognitionException {
        try {
            int _type = ASC;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:487:4: ( 'ASC' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:487:5: 'ASC'
            {
            match("ASC"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ASC"

    // $ANTLR start "DESC"
    public final void mDESC() throws RecognitionException {
        try {
            int _type = DESC;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:488:5: ( 'DESC' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:488:6: 'DESC'
            {
            match("DESC"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DESC"

    // $ANTLR start "EXPONENT"
    public final void mEXPONENT() throws RecognitionException {
        try {
            int _type = EXPONENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:490:2: ( '**' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:490:4: '**'
            {
            match("**"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "EXPONENT"

    // $ANTLR start "ID"
    public final void mID() throws RecognitionException {
        try {
            int _type = ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:493:5: ( ( 'A' .. 'Z' | 'a' .. 'z' ) ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )* | '`' ( 'A' .. 'Z' | 'a' .. 'z' ) ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )* '`' )
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( ((LA4_0>='A' && LA4_0<='Z')||(LA4_0>='a' && LA4_0<='z')) ) {
                alt4=1;
            }
            else if ( (LA4_0=='`') ) {
                alt4=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;
            }
            switch (alt4) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:493:7: ( 'A' .. 'Z' | 'a' .. 'z' ) ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )*
                    {
                    if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}

                    // D:\\tools\\antlr\\test\\MySQLParser.g:493:29: ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )*
                    loop2:
                    do {
                        int alt2=2;
                        int LA2_0 = input.LA(1);

                        if ( ((LA2_0>='#' && LA2_0<='$')||(LA2_0>='0' && LA2_0<='9')||(LA2_0>='A' && LA2_0<='Z')||LA2_0=='_'||(LA2_0>='a' && LA2_0<='z')) ) {
                            alt2=1;
                        }


                        switch (alt2) {
                    	case 1 :
                    	    // D:\\tools\\antlr\\test\\MySQLParser.g:
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
                    	    break loop2;
                        }
                    } while (true);


                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:494:7: '`' ( 'A' .. 'Z' | 'a' .. 'z' ) ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )* '`'
                    {
                    match('`'); 
                    if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}

                    // D:\\tools\\antlr\\test\\MySQLParser.g:494:32: ( 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '_' | '$' | '#' )*
                    loop3:
                    do {
                        int alt3=2;
                        int LA3_0 = input.LA(1);

                        if ( ((LA3_0>='#' && LA3_0<='$')||(LA3_0>='0' && LA3_0<='9')||(LA3_0>='A' && LA3_0<='Z')||LA3_0=='_'||(LA3_0>='a' && LA3_0<='z')) ) {
                            alt3=1;
                        }


                        switch (alt3) {
                    	case 1 :
                    	    // D:\\tools\\antlr\\test\\MySQLParser.g:
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

                    match('`'); 

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ID"

    // $ANTLR start "PLUS"
    public final void mPLUS() throws RecognitionException {
        try {
            int _type = PLUS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:497:2: ( '+' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:497:4: '+'
            {
            match('+'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "PLUS"

    // $ANTLR start "NUMBER"
    public final void mNUMBER() throws RecognitionException {
        try {
            int _type = NUMBER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:501:2: ( ( ( N '.' N ) | ( '.' N ) ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:502:2: ( ( N '.' N ) | ( '.' N ) )
            {
            // D:\\tools\\antlr\\test\\MySQLParser.g:502:2: ( ( N '.' N ) | ( '.' N ) )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( ((LA5_0>='0' && LA5_0<='9')) ) {
                alt5=1;
            }
            else if ( (LA5_0=='.') ) {
                alt5=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;
            }
            switch (alt5) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:502:3: ( N '.' N )
                    {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:502:3: ( N '.' N )
                    // D:\\tools\\antlr\\test\\MySQLParser.g:502:5: N '.' N
                    {
                    mN(); 
                    match('.'); 
                    mN(); 

                    }


                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:503:3: ( '.' N )
                    {
                    // D:\\tools\\antlr\\test\\MySQLParser.g:503:3: ( '.' N )
                    // D:\\tools\\antlr\\test\\MySQLParser.g:503:4: '.' N
                    {
                    match('.'); 
                    mN(); 

                    }


                    }
                    break;

            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "NUMBER"

    // $ANTLR start "MINUS"
    public final void mMINUS() throws RecognitionException {
        try {
            int _type = MINUS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:509:2: ( '-' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:509:4: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "MINUS"

    // $ANTLR start "DOT"
    public final void mDOT() throws RecognitionException {
        try {
            int _type = DOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:512:2: ( POINT )
            // D:\\tools\\antlr\\test\\MySQLParser.g:512:4: POINT
            {
            mPOINT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DOT"

    // $ANTLR start "COMMA"
    public final void mCOMMA() throws RecognitionException {
        try {
            int _type = COMMA;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:515:2: ( ',' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:515:4: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COMMA"

    // $ANTLR start "EQ"
    public final void mEQ() throws RecognitionException {
        try {
            int _type = EQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:518:2: ( '=' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:518:4: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "EQ"

    // $ANTLR start "DIVIDE"
    public final void mDIVIDE() throws RecognitionException {
        try {
            int _type = DIVIDE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:522:2: ( '/' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:522:4: '/'
            {
            match('/'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DIVIDE"

    // $ANTLR start "ASTERISK"
    public final void mASTERISK() throws RecognitionException {
        try {
            int _type = ASTERISK;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:525:2: ( '*' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:525:4: '*'
            {
            match('*'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ASTERISK"

    // $ANTLR start "ARROW"
    public final void mARROW() throws RecognitionException {
        try {
            int _type = ARROW;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:528:2: ( '=>' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:528:4: '=>'
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
            // D:\\tools\\antlr\\test\\MySQLParser.g:531:2: ( '||' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:531:4: '||'
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

    // $ANTLR start "POINT"
    public final void mPOINT() throws RecognitionException {
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:536:2: ( '.' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:536:4: '.'
            {
            match('.'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "POINT"

    // $ANTLR start "RPAREN"
    public final void mRPAREN() throws RecognitionException {
        try {
            int _type = RPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:539:2: ( ')' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:539:4: ')'
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
            // D:\\tools\\antlr\\test\\MySQLParser.g:542:2: ( '(' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:542:4: '('
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
            // D:\\tools\\antlr\\test\\MySQLParser.g:545:2: ( '<' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:545:4: '<'
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
            // D:\\tools\\antlr\\test\\MySQLParser.g:548:2: ( '<>' | '!=' | '^=' )
            int alt6=3;
            switch ( input.LA(1) ) {
            case '<':
                {
                alt6=1;
                }
                break;
            case '!':
                {
                alt6=2;
                }
                break;
            case '^':
                {
                alt6=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;
            }

            switch (alt6) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:548:4: '<>'
                    {
                    match("<>"); 


                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:548:11: '!='
                    {
                    match("!="); 


                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:548:18: '^='
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
            // D:\\tools\\antlr\\test\\MySQLParser.g:551:2: ( '<=' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:551:4: '<='
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
            // D:\\tools\\antlr\\test\\MySQLParser.g:554:2: ( '>=' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:554:4: '>='
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
            // D:\\tools\\antlr\\test\\MySQLParser.g:557:2: ( '>' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:557:4: '>'
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

    // $ANTLR start "QUOTED_STRING"
    public final void mQUOTED_STRING() throws RecognitionException {
        try {
            int _type = QUOTED_STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:565:2: ( '\\'' (~ '\\'' | '\\'\\'' )* '\\'' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:565:3: '\\'' (~ '\\'' | '\\'\\'' )* '\\''
            {
            match('\''); 
            // D:\\tools\\antlr\\test\\MySQLParser.g:565:8: (~ '\\'' | '\\'\\'' )*
            loop7:
            do {
                int alt7=3;
                int LA7_0 = input.LA(1);

                if ( (LA7_0=='\'') ) {
                    int LA7_1 = input.LA(2);

                    if ( (LA7_1=='\'') ) {
                        alt7=2;
                    }


                }
                else if ( ((LA7_0>='\u0000' && LA7_0<='&')||(LA7_0>='(' && LA7_0<='\uFFFF')) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:565:9: ~ '\\''
            	    {
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='&')||(input.LA(1)>='(' && input.LA(1)<='\uFFFF') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;
            	case 2 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:565:15: '\\'\\''
            	    {
            	    match("''"); 


            	    }
            	    break;

            	default :
            	    break loop7;
                }
            } while (true);

            match('\''); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "QUOTED_STRING"

    // $ANTLR start "DOUBLEQUOTED_STRING"
    public final void mDOUBLEQUOTED_STRING() throws RecognitionException {
        try {
            // D:\\tools\\antlr\\test\\MySQLParser.g:570:2: ( '\"' (~ ( '\"' ) )* '\"' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:570:4: '\"' (~ ( '\"' ) )* '\"'
            {
            match('\"'); 
            // D:\\tools\\antlr\\test\\MySQLParser.g:570:8: (~ ( '\"' ) )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( ((LA8_0>='\u0000' && LA8_0<='!')||(LA8_0>='#' && LA8_0<='\uFFFF')) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // D:\\tools\\antlr\\test\\MySQLParser.g:570:10: ~ ( '\"' )
            	    {
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='!')||(input.LA(1)>='#' && input.LA(1)<='\uFFFF') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);

            match('\"'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "DOUBLEQUOTED_STRING"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:573:5: ( ( ' ' | '\\t' | '\\r' '\\n' | '\\n' | '\\r' ) )
            // D:\\tools\\antlr\\test\\MySQLParser.g:573:9: ( ' ' | '\\t' | '\\r' '\\n' | '\\n' | '\\r' )
            {
            // D:\\tools\\antlr\\test\\MySQLParser.g:573:9: ( ' ' | '\\t' | '\\r' '\\n' | '\\n' | '\\r' )
            int alt9=5;
            switch ( input.LA(1) ) {
            case ' ':
                {
                alt9=1;
                }
                break;
            case '\t':
                {
                alt9=2;
                }
                break;
            case '\r':
                {
                int LA9_3 = input.LA(2);

                if ( (LA9_3=='\n') ) {
                    alt9=3;
                }
                else {
                    alt9=5;}
                }
                break;
            case '\n':
                {
                alt9=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 9, 0, input);

                throw nvae;
            }

            switch (alt9) {
                case 1 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:573:13: ' '
                    {
                    match(' '); 

                    }
                    break;
                case 2 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:574:7: '\\t'
                    {
                    match('\t'); 

                    }
                    break;
                case 3 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:575:7: '\\r' '\\n'
                    {
                    match('\r'); 
                    match('\n'); 

                    }
                    break;
                case 4 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:576:7: '\\n'
                    {
                    match('\n'); 

                    }
                    break;
                case 5 :
                    // D:\\tools\\antlr\\test\\MySQLParser.g:577:7: '\\r'
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

    // $ANTLR start "BITOR"
    public final void mBITOR() throws RecognitionException {
        try {
            int _type = BITOR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:583:2: ( '|' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:583:3: '|'
            {
            match('|'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITOR"

    // $ANTLR start "BITAND"
    public final void mBITAND() throws RecognitionException {
        try {
            int _type = BITAND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:586:2: ( '&' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:586:3: '&'
            {
            match('&'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITAND"

    // $ANTLR start "BITXOR"
    public final void mBITXOR() throws RecognitionException {
        try {
            int _type = BITXOR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:589:2: ( '^' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:589:3: '^'
            {
            match('^'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITXOR"

    // $ANTLR start "SHIFTLEFT"
    public final void mSHIFTLEFT() throws RecognitionException {
        try {
            int _type = SHIFTLEFT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:592:2: ( '<<' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:592:3: '<<'
            {
            match("<<"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SHIFTLEFT"

    // $ANTLR start "SHIFTRIGHT"
    public final void mSHIFTRIGHT() throws RecognitionException {
        try {
            int _type = SHIFTRIGHT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\tools\\antlr\\test\\MySQLParser.g:595:2: ( '>>' )
            // D:\\tools\\antlr\\test\\MySQLParser.g:595:3: '>>'
            {
            match(">>"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SHIFTRIGHT"

    public void mTokens() throws RecognitionException {
        // D:\\tools\\antlr\\test\\MySQLParser.g:1:8: ( T__99 | T__100 | T__101 | T__102 | T__103 | T__104 | T__105 | T__106 | T__107 | T__108 | T__109 | T__110 | T__111 | T__112 | T__113 | T__114 | T__115 | T__116 | T__117 | T__118 | T__119 | T__120 | T__121 | T__122 | T__123 | T__124 | T__125 | T__126 | T__127 | T__128 | T__129 | T__130 | T__131 | T__132 | T__133 | T__134 | T__135 | T__136 | T__137 | T__138 | T__139 | T__140 | T__141 | T__142 | T__143 | T__144 | T__145 | INTERVAL | N | ASC | DESC | EXPONENT | ID | PLUS | NUMBER | MINUS | DOT | COMMA | EQ | DIVIDE | ASTERISK | ARROW | DOUBLEVERTBAR | RPAREN | LPAREN | LTH | NOT_EQ | LEQ | GEQ | GTH | QUOTED_STRING | WS | BITOR | BITAND | BITXOR | SHIFTLEFT | SHIFTRIGHT )
        int alt10=77;
        alt10 = dfa10.predict(input);
        switch (alt10) {
            case 1 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:10: T__99
                {
                mT__99(); 

                }
                break;
            case 2 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:16: T__100
                {
                mT__100(); 

                }
                break;
            case 3 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:23: T__101
                {
                mT__101(); 

                }
                break;
            case 4 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:30: T__102
                {
                mT__102(); 

                }
                break;
            case 5 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:37: T__103
                {
                mT__103(); 

                }
                break;
            case 6 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:44: T__104
                {
                mT__104(); 

                }
                break;
            case 7 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:51: T__105
                {
                mT__105(); 

                }
                break;
            case 8 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:58: T__106
                {
                mT__106(); 

                }
                break;
            case 9 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:65: T__107
                {
                mT__107(); 

                }
                break;
            case 10 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:72: T__108
                {
                mT__108(); 

                }
                break;
            case 11 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:79: T__109
                {
                mT__109(); 

                }
                break;
            case 12 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:86: T__110
                {
                mT__110(); 

                }
                break;
            case 13 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:93: T__111
                {
                mT__111(); 

                }
                break;
            case 14 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:100: T__112
                {
                mT__112(); 

                }
                break;
            case 15 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:107: T__113
                {
                mT__113(); 

                }
                break;
            case 16 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:114: T__114
                {
                mT__114(); 

                }
                break;
            case 17 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:121: T__115
                {
                mT__115(); 

                }
                break;
            case 18 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:128: T__116
                {
                mT__116(); 

                }
                break;
            case 19 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:135: T__117
                {
                mT__117(); 

                }
                break;
            case 20 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:142: T__118
                {
                mT__118(); 

                }
                break;
            case 21 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:149: T__119
                {
                mT__119(); 

                }
                break;
            case 22 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:156: T__120
                {
                mT__120(); 

                }
                break;
            case 23 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:163: T__121
                {
                mT__121(); 

                }
                break;
            case 24 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:170: T__122
                {
                mT__122(); 

                }
                break;
            case 25 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:177: T__123
                {
                mT__123(); 

                }
                break;
            case 26 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:184: T__124
                {
                mT__124(); 

                }
                break;
            case 27 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:191: T__125
                {
                mT__125(); 

                }
                break;
            case 28 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:198: T__126
                {
                mT__126(); 

                }
                break;
            case 29 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:205: T__127
                {
                mT__127(); 

                }
                break;
            case 30 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:212: T__128
                {
                mT__128(); 

                }
                break;
            case 31 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:219: T__129
                {
                mT__129(); 

                }
                break;
            case 32 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:226: T__130
                {
                mT__130(); 

                }
                break;
            case 33 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:233: T__131
                {
                mT__131(); 

                }
                break;
            case 34 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:240: T__132
                {
                mT__132(); 

                }
                break;
            case 35 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:247: T__133
                {
                mT__133(); 

                }
                break;
            case 36 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:254: T__134
                {
                mT__134(); 

                }
                break;
            case 37 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:261: T__135
                {
                mT__135(); 

                }
                break;
            case 38 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:268: T__136
                {
                mT__136(); 

                }
                break;
            case 39 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:275: T__137
                {
                mT__137(); 

                }
                break;
            case 40 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:282: T__138
                {
                mT__138(); 

                }
                break;
            case 41 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:289: T__139
                {
                mT__139(); 

                }
                break;
            case 42 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:296: T__140
                {
                mT__140(); 

                }
                break;
            case 43 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:303: T__141
                {
                mT__141(); 

                }
                break;
            case 44 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:310: T__142
                {
                mT__142(); 

                }
                break;
            case 45 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:317: T__143
                {
                mT__143(); 

                }
                break;
            case 46 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:324: T__144
                {
                mT__144(); 

                }
                break;
            case 47 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:331: T__145
                {
                mT__145(); 

                }
                break;
            case 48 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:338: INTERVAL
                {
                mINTERVAL(); 

                }
                break;
            case 49 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:347: N
                {
                mN(); 

                }
                break;
            case 50 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:349: ASC
                {
                mASC(); 

                }
                break;
            case 51 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:353: DESC
                {
                mDESC(); 

                }
                break;
            case 52 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:358: EXPONENT
                {
                mEXPONENT(); 

                }
                break;
            case 53 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:367: ID
                {
                mID(); 

                }
                break;
            case 54 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:370: PLUS
                {
                mPLUS(); 

                }
                break;
            case 55 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:375: NUMBER
                {
                mNUMBER(); 

                }
                break;
            case 56 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:382: MINUS
                {
                mMINUS(); 

                }
                break;
            case 57 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:388: DOT
                {
                mDOT(); 

                }
                break;
            case 58 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:392: COMMA
                {
                mCOMMA(); 

                }
                break;
            case 59 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:398: EQ
                {
                mEQ(); 

                }
                break;
            case 60 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:401: DIVIDE
                {
                mDIVIDE(); 

                }
                break;
            case 61 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:408: ASTERISK
                {
                mASTERISK(); 

                }
                break;
            case 62 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:417: ARROW
                {
                mARROW(); 

                }
                break;
            case 63 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:423: DOUBLEVERTBAR
                {
                mDOUBLEVERTBAR(); 

                }
                break;
            case 64 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:437: RPAREN
                {
                mRPAREN(); 

                }
                break;
            case 65 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:444: LPAREN
                {
                mLPAREN(); 

                }
                break;
            case 66 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:451: LTH
                {
                mLTH(); 

                }
                break;
            case 67 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:455: NOT_EQ
                {
                mNOT_EQ(); 

                }
                break;
            case 68 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:462: LEQ
                {
                mLEQ(); 

                }
                break;
            case 69 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:466: GEQ
                {
                mGEQ(); 

                }
                break;
            case 70 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:470: GTH
                {
                mGTH(); 

                }
                break;
            case 71 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:474: QUOTED_STRING
                {
                mQUOTED_STRING(); 

                }
                break;
            case 72 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:488: WS
                {
                mWS(); 

                }
                break;
            case 73 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:491: BITOR
                {
                mBITOR(); 

                }
                break;
            case 74 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:497: BITAND
                {
                mBITAND(); 

                }
                break;
            case 75 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:504: BITXOR
                {
                mBITXOR(); 

                }
                break;
            case 76 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:511: SHIFTLEFT
                {
                mSHIFTLEFT(); 

                }
                break;
            case 77 :
                // D:\\tools\\antlr\\test\\MySQLParser.g:1:521: SHIFTRIGHT
                {
                mSHIFTRIGHT(); 

                }
                break;

        }

    }


    protected DFA10 dfa10 = new DFA10(this);
    static final String DFA10_eotS =
        "\1\uffff\15\27\1\uffff\6\27\1\116\1\121\2\uffff\1\122\2\uffff\1"+
        "\124\1\uffff\1\126\2\uffff\1\131\1\uffff\1\132\1\135\3\uffff\2\27"+
        "\1\146\1\147\6\27\1\156\2\27\1\162\1\163\10\27\1\176\15\27\1\116"+
        "\20\uffff\1\u008d\7\27\2\uffff\6\27\1\uffff\3\27\2\uffff\5\27\1"+
        "\u00a4\1\u00a5\1\27\1\u00a7\1\u00a8\1\uffff\7\27\1\u00b1\6\27\1"+
        "\uffff\3\27\1\u00bb\20\27\1\u00cc\1\27\2\uffff\1\u00ce\2\uffff\1"+
        "\u00cf\1\27\1\u00d1\1\u00d2\1\u00d3\1\u00d4\2\27\1\uffff\1\u00d7"+
        "\3\27\1\u00db\1\u00dc\1\27\1\u00de\1\27\1\uffff\2\27\1\u00e2\1\u00e3"+
        "\4\27\1\u00e8\1\u00e9\2\27\1\u00ec\1\u00ed\2\27\1\uffff\1\u00f0"+
        "\2\uffff\1\u00f1\4\uffff\1\u00f2\1\u00f3\1\uffff\1\u00f4\1\27\1"+
        "\u00f6\2\uffff\1\u00f7\1\uffff\1\u00f8\2\27\2\uffff\1\u00fb\1\u00fc"+
        "\1\27\1\u00fe\2\uffff\1\27\1\u0100\2\uffff\1\27\1\u0102\5\uffff"+
        "\1\u0103\3\uffff\2\27\2\uffff\1\u0106\1\uffff\1\u0107\1\uffff\1"+
        "\27\2\uffff\1\u0109\1\u010a\2\uffff\1\u010b\3\uffff";
    static final String DFA10_eofS =
        "\u010c\uffff";
    static final String DFA10_minS =
        "\1\11\1\105\1\107\1\101\1\105\1\122\1\105\1\101\1\116\1\105\1\110"+
        "\1\101\1\116\1\105\1\uffff\1\101\1\117\1\116\2\122\1\117\1\56\1"+
        "\52\2\uffff\1\60\2\uffff\1\76\1\uffff\1\174\2\uffff\1\74\1\uffff"+
        "\2\75\3\uffff\1\114\1\101\2\43\1\116\1\114\1\120\1\127\1\107\1\117"+
        "\1\43\1\124\1\126\2\43\1\124\1\123\1\114\1\105\1\124\1\116\1\114"+
        "\1\104\1\43\1\113\1\106\1\103\1\117\2\114\1\122\2\111\1\104\1\117"+
        "\1\125\1\104\1\56\20\uffff\1\43\1\105\1\122\2\105\1\111\2\105\2"+
        "\uffff\1\117\1\125\1\114\1\116\1\110\1\125\1\uffff\1\127\1\111\1"+
        "\105\2\uffff\1\105\1\124\1\105\1\103\1\122\2\43\1\114\2\43\1\uffff"+
        "\1\105\1\111\1\124\1\113\1\115\1\114\1\123\1\43\1\116\1\117\1\101"+
        "\1\123\2\105\1\uffff\1\103\1\105\1\122\1\43\1\122\1\116\1\122\1"+
        "\130\1\122\1\105\1\101\1\125\1\124\1\120\1\105\1\116\2\122\1\111"+
        "\1\124\1\43\1\105\2\uffff\1\43\2\uffff\1\43\1\124\4\43\2\105\1\uffff"+
        "\1\43\1\116\1\124\1\123\2\43\1\124\1\43\1\124\1\uffff\1\126\1\111"+
        "\2\43\1\105\1\123\1\103\1\115\2\43\1\105\1\107\2\43\1\116\1\105"+
        "\1\uffff\1\43\2\uffff\1\43\4\uffff\2\43\1\uffff\1\43\1\105\1\43"+
        "\2\uffff\1\43\1\uffff\1\43\1\101\1\124\2\uffff\2\43\1\105\1\43\2"+
        "\uffff\1\116\1\43\2\uffff\1\103\1\43\5\uffff\1\43\3\uffff\1\114"+
        "\1\105\2\uffff\1\43\1\uffff\1\43\1\uffff\1\124\2\uffff\2\43\2\uffff"+
        "\1\43\3\uffff";
    static final String DFA10_maxS =
        "\1\174\1\110\1\123\1\101\1\117\1\122\1\131\1\101\1\125\1\111\1"+
        "\110\1\125\1\123\1\117\1\uffff\1\125\1\117\1\120\2\122\1\117\1\71"+
        "\1\52\2\uffff\1\71\2\uffff\1\76\1\uffff\1\174\2\uffff\1\76\1\uffff"+
        "\1\75\1\76\3\uffff\1\124\1\101\2\172\1\116\1\114\1\120\1\127\1\107"+
        "\1\117\1\172\1\124\1\126\2\172\1\124\2\123\1\105\1\124\1\116\1\114"+
        "\1\104\1\172\1\115\1\106\1\103\1\117\2\114\1\122\2\111\1\104\1\117"+
        "\1\125\1\104\1\71\20\uffff\1\172\1\105\1\122\1\105\1\117\1\111\2"+
        "\105\2\uffff\1\117\1\125\1\114\1\116\1\110\1\125\1\uffff\1\127\1"+
        "\111\1\105\2\uffff\1\105\1\124\1\105\1\103\1\122\2\172\1\114\2\172"+
        "\1\uffff\1\105\1\111\1\124\1\113\1\115\1\114\1\123\1\172\1\116\1"+
        "\117\1\101\1\123\2\105\1\uffff\1\103\1\105\1\122\1\172\1\122\1\116"+
        "\1\122\1\130\1\122\1\105\1\101\1\125\1\124\1\120\1\105\1\116\2\122"+
        "\1\111\1\124\1\172\1\105\2\uffff\1\172\2\uffff\1\172\1\124\4\172"+
        "\2\105\1\uffff\1\172\1\116\1\124\1\123\2\172\1\124\1\172\1\124\1"+
        "\uffff\1\126\1\111\2\172\1\105\1\123\1\103\1\115\2\172\1\105\1\107"+
        "\2\172\1\116\1\105\1\uffff\1\172\2\uffff\1\172\4\uffff\2\172\1\uffff"+
        "\1\172\1\105\1\172\2\uffff\1\172\1\uffff\1\172\1\101\1\124\2\uffff"+
        "\2\172\1\105\1\172\2\uffff\1\116\1\172\2\uffff\1\103\1\172\5\uffff"+
        "\1\172\3\uffff\1\114\1\105\2\uffff\1\172\1\uffff\1\172\1\uffff\1"+
        "\124\2\uffff\2\172\2\uffff\1\172\3\uffff";
    static final String DFA10_acceptS =
        "\16\uffff\1\30\10\uffff\1\65\1\66\1\uffff\1\70\1\72\1\uffff\1\74"+
        "\1\uffff\1\100\1\101\1\uffff\1\103\2\uffff\1\107\1\110\1\112\46"+
        "\uffff\1\61\1\67\1\64\1\75\1\71\1\76\1\73\1\77\1\111\1\104\1\114"+
        "\1\102\1\113\1\105\1\115\1\106\10\uffff\1\25\1\21\6\uffff\1\7\3"+
        "\uffff\1\16\1\33\12\uffff\1\45\16\uffff\1\1\26\uffff\1\15\1\22\1"+
        "\uffff\1\17\1\62\10\uffff\1\54\11\uffff\1\3\20\uffff\1\63\1\uffff"+
        "\1\24\1\26\1\uffff\1\35\1\55\1\31\1\37\2\uffff\1\32\3\uffff\1\43"+
        "\1\57\1\uffff\1\56\3\uffff\1\34\1\47\4\uffff\1\36\1\6\2\uffff\1"+
        "\11\1\42\2\uffff\1\14\1\53\1\44\1\46\1\40\1\uffff\1\41\1\12\1\2"+
        "\2\uffff\1\50\1\4\1\uffff\1\27\1\uffff\1\10\1\uffff\1\51\1\52\2"+
        "\uffff\1\5\1\20\1\uffff\1\60\1\23\1\13";
    static final String DFA10_specialS =
        "\u010c\uffff}>";
    static final String[] DFA10_transitionS = {
            "\2\46\2\uffff\1\46\22\uffff\1\46\1\42\4\uffff\1\47\1\45\1\40"+
            "\1\37\1\26\1\30\1\33\1\32\1\31\1\35\12\25\2\uffff\1\41\1\34"+
            "\1\44\1\16\1\uffff\1\14\1\6\1\22\1\11\1\27\1\17\1\5\1\7\1\2"+
            "\1\20\1\27\1\15\1\24\1\13\1\10\2\27\1\4\1\1\1\23\1\21\1\3\1"+
            "\12\3\27\3\uffff\1\43\1\uffff\33\27\1\uffff\1\36",
            "\1\50\2\uffff\1\51",
            "\1\54\6\uffff\1\52\4\uffff\1\53",
            "\1\55",
            "\1\56\3\uffff\1\60\5\uffff\1\57",
            "\1\61",
            "\1\63\23\uffff\1\62",
            "\1\64",
            "\1\66\3\uffff\1\65\2\uffff\1\67",
            "\1\71\3\uffff\1\70",
            "\1\72",
            "\1\74\15\uffff\1\73\5\uffff\1\75",
            "\1\76\4\uffff\1\77",
            "\1\101\3\uffff\1\100\5\uffff\1\102",
            "",
            "\1\105\15\uffff\1\106\2\uffff\1\103\2\uffff\1\104",
            "\1\107",
            "\1\110\1\uffff\1\111",
            "\1\112",
            "\1\113",
            "\1\114",
            "\1\117\1\uffff\12\115",
            "\1\120",
            "",
            "",
            "\12\117",
            "",
            "",
            "\1\123",
            "",
            "\1\125",
            "",
            "",
            "\1\130\1\127\1\42",
            "",
            "\1\42",
            "\1\133\1\134",
            "",
            "",
            "",
            "\1\137\7\uffff\1\136",
            "\1\140",
            "\2\27\13\uffff\12\27\7\uffff\3\27\1\145\1\27\1\143\7\27\1"+
            "\144\4\27\1\141\1\142\6\27\4\uffff\1\27\1\uffff\32\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\150",
            "\1\151",
            "\1\152",
            "\1\153",
            "\1\154",
            "\1\155",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\157",
            "\1\160",
            "\2\27\13\uffff\12\27\7\uffff\3\27\1\161\26\27\4\uffff\1\27"+
            "\1\uffff\32\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\164",
            "\1\165",
            "\1\166\6\uffff\1\167",
            "\1\170",
            "\1\171",
            "\1\172",
            "\1\173",
            "\1\174",
            "\2\27\13\uffff\12\27\7\uffff\2\27\1\175\27\27\4\uffff\1\27"+
            "\1\uffff\32\27",
            "\1\177\1\uffff\1\u0080",
            "\1\u0081",
            "\1\u0082",
            "\1\u0083",
            "\1\u0084",
            "\1\u0085",
            "\1\u0086",
            "\1\u0087",
            "\1\u0088",
            "\1\u0089",
            "\1\u008a",
            "\1\u008b",
            "\1\u008c",
            "\1\117\1\uffff\12\115",
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
            "",
            "",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u008e",
            "\1\u008f",
            "\1\u0090",
            "\1\u0092\11\uffff\1\u0091",
            "\1\u0093",
            "\1\u0094",
            "\1\u0095",
            "",
            "",
            "\1\u0096",
            "\1\u0097",
            "\1\u0098",
            "\1\u0099",
            "\1\u009a",
            "\1\u009b",
            "",
            "\1\u009c",
            "\1\u009d",
            "\1\u009e",
            "",
            "",
            "\1\u009f",
            "\1\u00a0",
            "\1\u00a1",
            "\1\u00a2",
            "\1\u00a3",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00a6",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "\1\u00a9",
            "\1\u00aa",
            "\1\u00ab",
            "\1\u00ac",
            "\1\u00ad",
            "\1\u00ae",
            "\1\u00af",
            "\2\27\13\uffff\12\27\7\uffff\2\27\1\u00b0\27\27\4\uffff\1"+
            "\27\1\uffff\32\27",
            "\1\u00b2",
            "\1\u00b3",
            "\1\u00b4",
            "\1\u00b5",
            "\1\u00b6",
            "\1\u00b7",
            "",
            "\1\u00b8",
            "\1\u00b9",
            "\1\u00ba",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00bc",
            "\1\u00bd",
            "\1\u00be",
            "\1\u00bf",
            "\1\u00c0",
            "\1\u00c1",
            "\1\u00c2",
            "\1\u00c3",
            "\1\u00c4",
            "\1\u00c5",
            "\1\u00c6",
            "\1\u00c7",
            "\1\u00c8",
            "\1\u00c9",
            "\1\u00ca",
            "\1\u00cb",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00cd",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00d0",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00d5",
            "\1\u00d6",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00d8",
            "\1\u00d9",
            "\1\u00da",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00dd",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00df",
            "",
            "\1\u00e0",
            "\1\u00e1",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00e4",
            "\1\u00e5",
            "\1\u00e6",
            "\1\u00e7",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00ea",
            "\1\u00eb",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00ee",
            "\1\u00ef",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00f5",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00f9",
            "\1\u00fa",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\1\u00fd",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "\1\u00ff",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "\1\u0101",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "",
            "\1\u0104",
            "\1\u0105",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "\1\u0108",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            "\2\27\13\uffff\12\27\7\uffff\32\27\4\uffff\1\27\1\uffff\32"+
            "\27",
            "",
            "",
            ""
    };

    static final short[] DFA10_eot = DFA.unpackEncodedString(DFA10_eotS);
    static final short[] DFA10_eof = DFA.unpackEncodedString(DFA10_eofS);
    static final char[] DFA10_min = DFA.unpackEncodedStringToUnsignedChars(DFA10_minS);
    static final char[] DFA10_max = DFA.unpackEncodedStringToUnsignedChars(DFA10_maxS);
    static final short[] DFA10_accept = DFA.unpackEncodedString(DFA10_acceptS);
    static final short[] DFA10_special = DFA.unpackEncodedString(DFA10_specialS);
    static final short[][] DFA10_transition;

    static {
        int numStates = DFA10_transitionS.length;
        DFA10_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA10_transition[i] = DFA.unpackEncodedString(DFA10_transitionS[i]);
        }
    }

    class DFA10 extends DFA {

        public DFA10(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 10;
            this.eot = DFA10_eot;
            this.eof = DFA10_eof;
            this.min = DFA10_min;
            this.max = DFA10_max;
            this.accept = DFA10_accept;
            this.special = DFA10_special;
            this.transition = DFA10_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__99 | T__100 | T__101 | T__102 | T__103 | T__104 | T__105 | T__106 | T__107 | T__108 | T__109 | T__110 | T__111 | T__112 | T__113 | T__114 | T__115 | T__116 | T__117 | T__118 | T__119 | T__120 | T__121 | T__122 | T__123 | T__124 | T__125 | T__126 | T__127 | T__128 | T__129 | T__130 | T__131 | T__132 | T__133 | T__134 | T__135 | T__136 | T__137 | T__138 | T__139 | T__140 | T__141 | T__142 | T__143 | T__144 | T__145 | INTERVAL | N | ASC | DESC | EXPONENT | ID | PLUS | NUMBER | MINUS | DOT | COMMA | EQ | DIVIDE | ASTERISK | ARROW | DOUBLEVERTBAR | RPAREN | LPAREN | LTH | NOT_EQ | LEQ | GEQ | GTH | QUOTED_STRING | WS | BITOR | BITAND | BITXOR | SHIFTLEFT | SHIFTRIGHT );";
        }
    }
 

}