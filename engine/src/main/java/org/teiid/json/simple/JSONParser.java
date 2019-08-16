// see http://www.apache.org/licenses/LICENSE-2.0

/*
 * $Id: JSONParser.java,v 1.1 2006/04/15 14:10:48 platform Exp $
 * Created on 2006-4-15
 */
package org.teiid.json.simple;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedList;


/**
 * Parser for JSON text. Please note that JSONParser is NOT thread-safe.
 *
 * @author FangYidong fangyidong@yahoo.com.cn
 */
public class JSONParser {

    /**
     * Modified from JSONValue
     * @param s - Must not be null.
     * @param sb
     * @throws IOException
     */
    public static void escape(CharSequence s, Appendable sb) throws IOException {
                for(int i=0;i<s.length();i++){
                        char ch=s.charAt(i);
                        switch(ch){
                        case '"':
                                sb.append("\\\"");  //$NON-NLS-1$
                                break;
                        case '\\':
                                sb.append("\\\\"); //$NON-NLS-1$
                                break;
                        case '\b':
                                sb.append("\\b"); //$NON-NLS-1$
                                break;
                        case '\f':
                                sb.append("\\f"); //$NON-NLS-1$
                                break;
                        case '\n':
                                sb.append("\\n"); //$NON-NLS-1$
                                break;
                        case '\r':
                                sb.append("\\r"); //$NON-NLS-1$
                                break;
                        case '\t':
                                sb.append("\\t"); //$NON-NLS-1$
                                break;
                        case '/':
                                sb.append("\\/"); //$NON-NLS-1$
                                break;
                        default:
                //Reference: http://www.unicode.org/versions/Unicode5.1.0/
                                if((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')){
                                        String ss=Integer.toHexString(ch);
                                        sb.append("\\u");
                                        for(int k=0;k<4-ss.length();k++){
                                                sb.append('0');
                                        }
                                        sb.append(ss.toUpperCase());
                                }
                                else{
                                        sb.append(ch);
                                }
                        }
                }//for
        }

    public static final int S_INIT=0;
    public static final int S_IN_FINISHED_VALUE=1;//string,number,boolean,null,object,array
    public static final int S_IN_OBJECT=2;
    public static final int S_IN_ARRAY=3;
    public static final int S_PASSED_PAIR_KEY=4;
    public static final int S_IN_PAIR_VALUE=5;
    public static final int S_END=6;
    public static final int S_IN_ERROR=-1;

    private LinkedList handlerStatusStack;
    private Yylex lexer = new Yylex((Reader)null);
    private Yytoken token = null;
    private int status = S_INIT;

    private int peekStatus(LinkedList statusStack){
        if(statusStack.size()==0)
            return -1;
        Integer status=(Integer)statusStack.getFirst();
        return status.intValue();
    }

    /**
     *  Reset the parser to the initial state without resetting the underlying reader.
     *
     */
    public void reset(){
        token = null;
        status = S_INIT;
        handlerStatusStack = null;
    }

    /**
     * Reset the parser to the initial state with a new character reader.
     *
     * @param in - The new character reader.
     */
    public void reset(Reader in){
        lexer.yyreset(in);
        reset();
    }

    /**
     * @return The position of the beginning of the current token.
     */
    public int getPosition(){
        return lexer.getPosition();
    }

    private void nextToken() throws ParseException, IOException{
        token = lexer.yylex();
        if(token == null)
            token = Yytoken.EOF;
    }

    public void parse(String s, ContentHandler contentHandler) throws ParseException{
        parse(s, contentHandler, false);
    }

    public void parse(String s, ContentHandler contentHandler, boolean isResume) throws ParseException{
        StringReader in=new StringReader(s);
        try{
            parse(in, contentHandler, isResume);
        }
        catch(IOException ie){
            /*
             * Actually it will never happen.
             */
            throw new ParseException(-1, ParseException.ERROR_UNEXPECTED_EXCEPTION, ie);
        }
    }

    public void parse(Reader in, ContentHandler contentHandler) throws IOException, ParseException{
        parse(in, contentHandler, false);
    }

    /**
     * Stream processing of JSON text.
     *
     * @see ContentHandler
     *
     * @param in
     * @param contentHandler
     * @param isResume - Indicates if it continues previous parsing operation.
     *                   If set to true, resume parsing the old stream, and parameter 'in' will be ignored.
     *                   If this method is called for the first time in this instance, isResume will be ignored.
     *
     * @throws IOException
     * @throws ParseException
     */
    public void parse(Reader in, ContentHandler contentHandler, boolean isResume) throws IOException, ParseException{
        if(!isResume){
            reset(in);
            handlerStatusStack = new LinkedList();
        }
        else{
            if(handlerStatusStack == null){
                isResume = false;
                reset(in);
                handlerStatusStack = new LinkedList();
            }
        }

        LinkedList statusStack = handlerStatusStack;

        try{
            do{
                switch(status){
                case S_INIT:
                    contentHandler.startJSON();
                    nextToken();
                    switch(token.type){
                    case Yytoken.TYPE_VALUE:
                        status=S_IN_FINISHED_VALUE;
                        statusStack.addFirst(status);
                        if(!contentHandler.primitive(token.value))
                            return;
                        break;
                    case Yytoken.TYPE_LEFT_BRACE:
                        status=S_IN_OBJECT;
                        statusStack.addFirst(status);
                        if(!contentHandler.startObject())
                            return;
                        break;
                    case Yytoken.TYPE_LEFT_SQUARE:
                        status=S_IN_ARRAY;
                        statusStack.addFirst(status);
                        if(!contentHandler.startArray())
                            return;
                        break;
                    default:
                        status=S_IN_ERROR;
                    }//inner switch
                    break;

                case S_IN_FINISHED_VALUE:
                    nextToken();
                    if(token.type==Yytoken.TYPE_EOF){
                        contentHandler.endJSON();
                        status = S_END;
                        return;
                    }
                    else{
                        status = S_IN_ERROR;
                        throw new ParseException(getPosition(), ParseException.ERROR_UNEXPECTED_TOKEN, token);
                    }

                case S_IN_OBJECT:
                    nextToken();
                    switch(token.type){
                    case Yytoken.TYPE_COMMA:
                        break;
                    case Yytoken.TYPE_VALUE:
                        if(token.value instanceof String){
                            String key=(String)token.value;
                            status=S_PASSED_PAIR_KEY;
                            statusStack.addFirst(status);
                            if(!contentHandler.startObjectEntry(key))
                                return;
                        }
                        else{
                            status=S_IN_ERROR;
                        }
                        break;
                    case Yytoken.TYPE_RIGHT_BRACE:
                        if(statusStack.size()>1){
                            statusStack.removeFirst();
                            status=peekStatus(statusStack);
                        }
                        else{
                            status=S_IN_FINISHED_VALUE;
                        }
                        if(!contentHandler.endObject())
                            return;
                        break;
                    default:
                        status=S_IN_ERROR;
                        break;
                    }//inner switch
                    break;

                case S_PASSED_PAIR_KEY:
                    nextToken();
                    switch(token.type){
                    case Yytoken.TYPE_COLON:
                        break;
                    case Yytoken.TYPE_VALUE:
                        statusStack.removeFirst();
                        status=peekStatus(statusStack);
                        if(!contentHandler.primitive(token.value))
                            return;
                        if(!contentHandler.endObjectEntry())
                            return;
                        break;
                    case Yytoken.TYPE_LEFT_SQUARE:
                        statusStack.removeFirst();
                        statusStack.addFirst(S_IN_PAIR_VALUE);
                        status=S_IN_ARRAY;
                        statusStack.addFirst(status);
                        if(!contentHandler.startArray())
                            return;
                        break;
                    case Yytoken.TYPE_LEFT_BRACE:
                        statusStack.removeFirst();
                        statusStack.addFirst(S_IN_PAIR_VALUE);
                        status=S_IN_OBJECT;
                        statusStack.addFirst(status);
                        if(!contentHandler.startObject())
                            return;
                        break;
                    default:
                        status=S_IN_ERROR;
                    }
                    break;

                case S_IN_PAIR_VALUE:
                    /*
                     * S_IN_PAIR_VALUE is just a marker to indicate the end of an object entry, it doesn't proccess any token,
                     * therefore delay consuming token until next round.
                     */
                    statusStack.removeFirst();
                    status = peekStatus(statusStack);
                    if(!contentHandler.endObjectEntry())
                        return;
                    break;

                case S_IN_ARRAY:
                    nextToken();
                    switch(token.type){
                    case Yytoken.TYPE_COMMA:
                        break;
                    case Yytoken.TYPE_VALUE:
                        if(!contentHandler.primitive(token.value))
                            return;
                        break;
                    case Yytoken.TYPE_RIGHT_SQUARE:
                        if(statusStack.size()>1){
                            statusStack.removeFirst();
                            status=peekStatus(statusStack);
                        }
                        else{
                            status=S_IN_FINISHED_VALUE;
                        }
                        if(!contentHandler.endArray())
                            return;
                        break;
                    case Yytoken.TYPE_LEFT_BRACE:
                        status=S_IN_OBJECT;
                        statusStack.addFirst(status);
                        if(!contentHandler.startObject())
                            return;
                        break;
                    case Yytoken.TYPE_LEFT_SQUARE:
                        status=S_IN_ARRAY;
                        statusStack.addFirst(status);
                        if(!contentHandler.startArray())
                            return;
                        break;
                    default:
                        status=S_IN_ERROR;
                    }//inner switch
                    break;

                case S_END:
                    return;

                case S_IN_ERROR:
                    throw new ParseException(getPosition(), ParseException.ERROR_UNEXPECTED_TOKEN, token);
                }//switch
                if(status==S_IN_ERROR){
                    throw new ParseException(getPosition(), ParseException.ERROR_UNEXPECTED_TOKEN, token);
                }
            }while(token.type!=Yytoken.TYPE_EOF);
        }
        catch(IOException ie){
            status = S_IN_ERROR;
            throw ie;
        }
        catch(ParseException pe){
            status = S_IN_ERROR;
            throw pe;
        }
        catch(RuntimeException re){
            status = S_IN_ERROR;
            throw re;
        }
        catch(Error e){
            status = S_IN_ERROR;
            throw e;
        }

        status = S_IN_ERROR;
        throw new ParseException(getPosition(), ParseException.ERROR_UNEXPECTED_TOKEN, token);
    }
}
