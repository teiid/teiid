/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.teiid.odbc;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.TreeMap;

/**
 * This class can split SQL scripts to single SQL statements.
 * Each SQL statement ends with the character ';', however it is ignored
 * in comments and quotes.
 */
public class ScriptReader {

    private static TreeMap<String, String> FUNCTION_MAPPING = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
    static {
        FUNCTION_MAPPING.put("textcat", "concat"); //$NON-NLS-1$ //$NON-NLS-2$
        FUNCTION_MAPPING.put("rtrunc", "right"); //$NON-NLS-1$ //$NON-NLS-2$
        FUNCTION_MAPPING.put("ltrunc", "left"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private Reader reader;
    private StringBuilder builder;
    private boolean endOfFile;
    private boolean insideRemark;
    private boolean blockRemark;
    private boolean rewrite;
    private int expressionStart=-1;
    private int expressionEnd=-1;

    /**
     * Create a new SQL script reader from the given reader
     *
     * @param reader the reader
     */
    public ScriptReader(Reader reader) {
        this.reader = reader;
        this.builder = new StringBuilder(1<<13);
    }

    public ScriptReader(String string) {
        this.reader = new StringReader(string);
        this.builder = new StringBuilder(string.length());
    }

    /**
     * Close the underlying reader.
     */
    public void close() throws IOException{
        reader.close();
    }

    /**
     * Read a statement from the reader. This method returns null if the end has
     * been reached.
     *
     * @return the SQL statement or null
     */
    public String readStatement() throws IOException{
        if (endOfFile) {
            return null;
        }
        while (true) {
            String result = readStatementLoop();
            if (result != null || endOfFile) {
                return result;
            }
        }

    }

    private String readStatementLoop() throws IOException {
        int c = read();
        while (true) {
            if (c < 0) {
                endOfFile = true;
                break;
            } else if (c == ';') {
                builder.setLength(builder.length()-1);
                break;
            }
            switch (c) {
            case '(': {
                if (rewrite) {
                    //check if this is a function the driver uses to handle escape functions
                    int start = builder.length();
                    int functionStart = start - 2;
                    for (; functionStart>=0; functionStart--) {
                        if (!Character.isLetterOrDigit(builder.charAt(functionStart))) {
                            break;
                        }
                    }
                    if (functionStart != start -2) {
                        String functionName = builder.substring(functionStart + 1, start -1);
                        String mappedFunction = FUNCTION_MAPPING.get(functionName);
                        if (mappedFunction != null) {
                            builder.replace(functionStart + 1, start - 1, mappedFunction);
                        }
                    }
                }
                c = read();
                break;
            }
            case '$': {
                c = read();
                if (c == '$' && (builder.length() < 3 || builder.charAt(builder.length() - 3) <= ' ')) {
                    // dollar quoted string
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (c == '$') {
                            c = read();
                            if (c < 0) {
                                break;
                            }
                            if (c == '$') {
                                break;
                            }
                        }
                    }
                    c = read();
                }
                break;
            }
            case '\'':
                //TODO: in rewrite mode could handle the E' logic here rather than in the parser
                //however the parser now uses E' with like to detect pg specific handling
                if (expressionEnd != builder.length() - 1) {
                    expressionStart = builder.length() - 1;
                }
                while (true) {
                    c = read();
                    if (c < 0 || c == '\'') {
                        break;
                    }
                }
                expressionEnd = builder.length();
                c = read();
                break;
            case '"':
                while (true) {
                    c = read();
                    if (c < 0) {
                        break;
                    }
                    if (c == '\"') {
                        break;
                    }
                }
                c = read();
                break;
            case '/': {
                c = read();
                if (c == '*') {
                    // block comment
                    startRemark(false);
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (c == '*') {
                            c = read();
                            if (c < 0) {
                                break;
                            }
                            if (c == '/') {
                                endRemark();
                                break;
                            }
                        }
                    }
                    c = read();
                } else if (c == '/') {
                    // single line comment
                    startRemark(false);
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (c == '\r' || c == '\n') {
                            endRemark();
                            break;
                        }
                    }
                    c = read();
                }
                break;
            }
            case '-': {
                c = read();
                if (c == '-') {
                    // single line comment
                    startRemark(false);
                    while (true) {
                        c = read();
                        if (c < 0) {
                            break;
                        }
                        if (c == '\r' || c == '\n') {
                            endRemark();
                            break;
                        }
                    }
                    c = read();
                }
                break;
            }
            case ':': {
                if (rewrite) {
                    int start = builder.length();
                    c = read();
                    if (c == ':') {
                        while (true) {
                            c = read();
                            if (c < 0 || !Character.isLetterOrDigit(c)) {
                                String type = builder.substring(start+1, builder.length() - (c<0?0:1));
                                builder.setLength(start-1);
                                if (expressionStart != -1 && expressionEnd == start -1) {
                                    //special handling for regclass cast - it won't always work
                                    if ("regclass".equalsIgnoreCase(type)) { //$NON-NLS-1$
                                        builder.insert(expressionStart, "regclass("); //$NON-NLS-1$
                                        builder.append(")"); //$NON-NLS-1$
                                    } else if ("regproc".equalsIgnoreCase(type)) { //$NON-NLS-1$
                                        String name = builder.substring(expressionStart);
                                        if (name.startsWith("'\"") && name.length() > 4) { //$NON-NLS-1$
                                            name = name.substring(2, name.length()-2);
                                            name = '\''+ name + '\'';
                                        }
                                        if (name.startsWith("'")) { //$NON-NLS-1$
                                            builder.setLength(expressionStart);
                                            builder.append(name.toUpperCase());
                                        }
                                        builder.insert(expressionStart, "(SELECT oid FROM pg_catalog.pg_proc WHERE upper(proname) = "); //$NON-NLS-1$
                                        builder.append(")"); //$NON-NLS-1$
                                    } else {
                                        builder.insert(expressionStart, "cast("); //$NON-NLS-1$
                                        builder.append(" AS ").append(type).append(")"); //$NON-NLS-1$ //$NON-NLS-2$
                                    }
                                }
                                if (c != -1) {
                                    builder.append((char)c);
                                }
                                break;
                            }
                        }
                    }
                    break;
                }
                c = read();
                break;
            }
            case '~': {
                if (rewrite) {
                    int start = builder.length() - 1;
                    boolean not = false;
                    if (start > 0 && builder.charAt(start - 1) == '!') {
                        not = true;
                        start -= 1;
                    }
                    c = read();
                    boolean like = false;
                    if (c == '~') {
                        like = true;
                        c = read();
                    }
                    if (c == '*') {
                        break; //can't handle
                    }
                    builder.setLength(start);
                    if (not) {
                        builder.append(" NOT"); //$NON-NLS-1$
                    }
                    if (like) {
                        builder.append(" LIKE "); //$NON-NLS-1$
                    } else {
                        builder.append(" LIKE_REGEX "); //$NON-NLS-1$
                    }
                    if (c != -1) {
                        builder.append((char)c);
                    }
                }
                c = read();
                break;
            }
            default: {
                c = read();
            }
            }
        }
        String result = builder.toString();
        builder.setLength(0);
        if (result.length() == 0) {
            return null;
        }
        return result;
    }

    private void startRemark(boolean block) {
        blockRemark = block;
        insideRemark = true;
    }

    private void endRemark() {
        insideRemark = false;
    }

    private int read() throws IOException {
       int c = reader.read();
       if (c != -1) {
           builder.append((char)c);
       }
       return c;
    }

    /**
     * Check if this is the last statement, and if the single line or block
     * comment is not finished yet.
     *
     * @return true if the current position is inside a remark
     */
    public boolean isInsideRemark() {
        return insideRemark;
    }

    /**
     * If currently inside a remark, this method tells if it is a block comment
     * (true) or single line comment (false)
     *
     * @return true if inside a block comment
     */
    public boolean isBlockRemark() {
        return blockRemark;
    }

    public void setRewrite(boolean rewrite) {
        this.rewrite = rewrite;
    }

}
