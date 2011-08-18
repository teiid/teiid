/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.query.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;

/**
 * <p>Converts a SQL-string to an object version of a query.  This
 * QueryParser can be reused but is NOT thread-safe as the parser uses an
 * input stream.  Putting multiple queries into the same stream will result
 * in unpredictable and most likely incorrect behavior.</p>
 */
public class QueryParser {
    
    private static ThreadLocal<QueryParser> QUERY_PARSER = new ThreadLocal<QueryParser>() {
        /** 
         * @see java.lang.ThreadLocal#initialValue()
         */
        @Override
        protected QueryParser initialValue() {
            return new QueryParser();
        }
    };

	// Used in parsing TokenMgrError message
	private static final String LINE_MARKER = "line "; //$NON-NLS-1$
	private static final String COL_MARKER = "column "; //$NON-NLS-1$
    
    private static final String XQUERY_DECLARE = "declare"; //$NON-NLS-1$
    private static final String XML_OPEN_BRACKET = "<"; //$NON-NLS-1$

	private SQLParser parser;
    
	/**
	 * Construct a QueryParser - this may be reused.
	 */
	public QueryParser() {}
    
    public static QueryParser getQueryParser() {
        return QUERY_PARSER.get();
    }
	
	/**
	 * Helper method to get a SQLParser instace for given sql string.
	 */
	private SQLParser getSqlParser(String sql) {
		sql = sql != null ? sql : "";  //$NON-NLS-1$
		if(parser == null) {
			parser = new SQLParser(new StringReader(sql));
		} else {
			parser.ReInit(new StringReader(sql));	
		}
		return parser;		
	}

	/**
	 * Takes a SQL string representing a Command and returns the object
	 * representation.
	 * @param sql SQL string 
	 * instead of string litral
	 * @return SQL object representation
	 * @throws QueryParserException if parsing fails
	 * @throws IllegalArgumentException if sql is null
	 */	
	public Command parseCommand(String sql) throws QueryParserException {
	    return parseCommand(sql, new ParseInfo());
	}
	
	public Command parseUpdateProcedure(String sql) throws QueryParserException {
		try{
            Command result = getSqlParser(sql).updateProcedure(new ParseInfo());
            result.setCacheHint(SQLParserUtil.getQueryCacheOption(sql));
            return result;
        } catch(ParseException pe) {
            throw convertParserException(pe);
        } catch(TokenMgrError tme) {
            throw handleTokenMgrError(tme);
        }
	}
	
	/**
	 * Takes a SQL string representing a Command and returns the object
	 * representation.
	 * @param sql SQL string
	 * @param parseInfo - instructions to parse
	 * @return SQL object representation
	 * @throws QueryParserException if parsing fails
	 * @throws IllegalArgumentException if sql is null
	 */
    public Command parseCommand(String sql, ParseInfo parseInfo) throws QueryParserException {
        return parseCommand(sql, parseInfo, false);
    }
    
    public Command parseDesignerCommand(String sql) throws QueryParserException {
        return parseCommand(sql, new ParseInfo(), true);
    }

	public Command parseCommand(String sql, ParseInfo parseInfo, boolean designerCommands) throws QueryParserException {
        if(sql == null || sql.length() == 0) {
            throw new QueryParserException(QueryPlugin.Util.getString("QueryParser.emptysql")); //$NON-NLS-1$
        }
        
    	Command result = null;
        try{
            if (designerCommands) {
                result = getSqlParser(sql).designerCommand(parseInfo);
            } else {
                result = getSqlParser(sql).command(parseInfo);
            }
            result.setCacheHint(SQLParserUtil.getQueryCacheOption(sql));
        } catch(ParseException pe) {
        	if(sql.startsWith(XML_OPEN_BRACKET) || sql.startsWith(XQUERY_DECLARE)) {
            	throw new QueryParserException(pe, QueryPlugin.Util.getString("QueryParser.xqueryCompilation", sql)); //$NON-NLS-1$
            }
            throw convertParserException(pe);
        } catch(TokenMgrError tme) {
        	if(sql.startsWith(XML_OPEN_BRACKET) || sql.startsWith(XQUERY_DECLARE)) {
            	throw new QueryParserException(tme, QueryPlugin.Util.getString("QueryParser.xqueryCompilation", sql)); //$NON-NLS-1$
            }
            throw handleTokenMgrError(tme);
        }
		return result;
	}
	
	public CacheHint parseCacheHint(String sql) throws QueryParserException {
        if(sql == null || sql.length() == 0) {
            throw new QueryParserException(QueryPlugin.Util.getString("QueryParser.emptysql")); //$NON-NLS-1$
        }        
        return SQLParserUtil.getQueryCacheOption(sql);        
	}	

    /**
     * Takes a SQL string representing an SQL criteria (i.e. just the WHERE
     * clause) and returns the object representation.
     * @param sql SQL criteria (WHERE clause) string
     * @return Criteria SQL object representation
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if sql is null
     */
    public Criteria parseCriteria(String sql) throws QueryParserException {
        if(sql == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlCrit")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        Criteria result = null;
        try{
            result = getSqlParser(sql).criteria(dummyInfo);

        } catch(ParseException pe) {
            throw convertParserException(pe);

        } catch(TokenMgrError tme) {
            throw handleTokenMgrError(tme);
        }
        return result;
    }

    private QueryParserException convertParserException(ParseException pe) {
        QueryParserException qpe = new QueryParserException(QueryPlugin.Util.getString("QueryParser.parsingError", getMessage(pe, 1, 10))); //$NON-NLS-1$
        qpe.setParseException(pe);
        if (pe.currentToken == null) {
        	pe.currentToken = parser.token;
        }
        return qpe;
    }
        
    public static String getMessage(ParseException pe, int maxTokenSequence, int maxExpansions) {
		if (!pe.specialConstructor) {
			return pe.getMessage();
		}
		StringBuffer expected = new StringBuffer();
		int[][] expectedTokenSequences = pe.expectedTokenSequences;
		String[] tokenImage = pe.tokenImage;
		String eol = pe.eol;
		Token currentToken = pe.currentToken;
		HashSet<List<Integer>> expansions = new HashSet<List<Integer>>();
		Arrays.sort(expectedTokenSequences, new Comparator<int[]>() {
			@Override
			public int compare(int[] o1, int[] o2) {
				return o2.length - o1.length;
			}
		});
		int maxSize = expectedTokenSequences[0].length;
		StringBuilder retval = new StringBuilder("Encountered \""); //$NON-NLS-1$
		Token tok = currentToken.next;
		for (int i = 0; i < maxSize; i++) {
			if (i != 0)
				retval.append(" "); //$NON-NLS-1$
			if (tok.kind == 0) {
				retval.append(tokenImage[0]);
				break;
			}
			retval.append(pe.add_escapes(tok.image));
			tok = tok.next;
		}
		retval.append("\" at line " + currentToken.next.beginLine + ", column " //$NON-NLS-1$ //$NON-NLS-2$
				+ currentToken.next.beginColumn);
		retval.append("." + eol); //$NON-NLS-1$
		for (int i = 0; i < expectedTokenSequences.length; i++) {
			boolean truncateStart = expectedTokenSequences[i].length == maxSize && maxSize > 1 && maxSize > maxTokenSequence;
			int start = 0;
			if (truncateStart) {
				start = expectedTokenSequences[i].length - maxTokenSequence;
			}
			List<Integer> expansion = new ArrayList<Integer>(Math.min(maxTokenSequence, expectedTokenSequences[i].length));
			for (int j = start; j < start+maxTokenSequence; j++) {
				expansion.add(expectedTokenSequences[i][j]);
			}
			if (!expansions.add(expansion) || (!truncateStart && expectedTokenSequences[i][start] == currentToken.next.kind)) {
				continue;
			}
			if (expansions.size() > maxExpansions) {
				expected.append("...").append(eol).append("    "); //$NON-NLS-1$ //$NON-NLS-2$
				break;
			}
			if (truncateStart) {
				expected.append("... "); //$NON-NLS-1$
			}
			for (int j = start; j < expectedTokenSequences[i].length && j < start+maxTokenSequence; j++) {
				expected.append(tokenImage[expectedTokenSequences[i][j]])
						.append(" "); //$NON-NLS-1$
			}
			if (expectedTokenSequences[i][Math.min(maxTokenSequence, expectedTokenSequences[i].length - 1)] != 0) {
				expected.append("..."); //$NON-NLS-1$
			}
			expected.append(eol).append("    "); //$NON-NLS-1$
		}
		if (expansions.size() == 1) {
			retval.append("Was expecting:" + eol + "    "); //$NON-NLS-1$ //$NON-NLS-2$
		} else {
			retval.append("Was expecting one of:" + eol + "    "); //$NON-NLS-1$ //$NON-NLS-2$
		}
		retval.append(expected.toString());
		return retval.toString();
    }

    /**
     * Takes a SQL string representing an SQL expression
     * and returns the object representation.
     * @param sql SQL expression string
     * @return SQL expression object
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if sql is null
     */
    public Expression parseExpression(String sql) throws QueryParserException {
        if(sql == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlExpr")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        Expression result = null;
        try{
            result = getSqlParser(sql).expression(dummyInfo);

        } catch(ParseException pe) {
            throw convertParserException(pe);

        } catch(TokenMgrError tme) {
            throw handleTokenMgrError(tme);
        }
        return result;
    }
    
    public SingleElementSymbol parseSelectExpression(String sql) throws QueryParserException {
        if(sql == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlExpr")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        SingleElementSymbol result = null;
        try{
            result = getSqlParser(sql).selectExpression(dummyInfo);

        } catch(ParseException pe) {
            throw convertParserException(pe);

        } catch(TokenMgrError tme) {
            throw handleTokenMgrError(tme);
        }
        return result;
    }

    private QueryParserException handleTokenMgrError(TokenMgrError tme) {
//            LogManager.logError( LogConstants.CTX_QUERY_PARSER, tme, new Object[] {"Exception parsing: ", sql} );

        // From TokenMgrError, here is format of lexical error:
        //
        // "Lexical error at line " + errorLine + ", column " + errorColumn +
        // ".  Encountered: " + (EOFSeen ? "<EOF> " : ("\"" +
        // addEscapes(String.valueOf(curChar)) + "\"") + " (" + (int)curChar + "), ") +
        // "after : \"" + addEscapes(errorAfter) + "\""

        String msg = tme.getMessage();
        int index = msg.indexOf(LINE_MARKER);
        if(index > 0) {
            index += LINE_MARKER.length();
            int lastIndex = msg.indexOf(",", index); //$NON-NLS-1$
            
            index = msg.indexOf(COL_MARKER, lastIndex);
            if(index > 0) {
                index += COL_MARKER.length();
                lastIndex = msg.indexOf(".", index); //$NON-NLS-1$
                
                return new QueryParserException(QueryPlugin.Util.getString("QueryParser.lexicalError", tme.getMessage())); //$NON-NLS-1$
            }

        }
        return new QueryParserException(QueryPlugin.Util.getString("QueryParser.parsingError", tme.getMessage())); //$NON-NLS-1$
    }

}
