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

package com.metamatrix.query.parser;

import java.io.StringReader;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.xquery.XQueryExpression;
import com.metamatrix.query.xquery.saxon.SaxonXQueryExpression;

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
    private static final String XML_CLOSE_BRACKET = ">"; //$NON-NLS-1$

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
        if(sql == null || sql.length() == 0) {
            throw new QueryParserException(QueryPlugin.Util.getString("QueryParser.emptysql")); //$NON-NLS-1$
        }
        
        QueryParserException qpe = null;
        try {
	        Command result = parseCommandWithParser(sql, parseInfo);
	        result.setCache(parseInfo.cache);
			return result;
        } catch (QueryParserException e) {
        	qpe = e;
        }
        
        try {
            // Check for OPTION
            Option option = null;
            int closeBracket = sql.lastIndexOf(XML_CLOSE_BRACKET);
            int optionIndex = sql.toUpperCase().lastIndexOf(ReservedWords.OPTION);
            if (optionIndex != -1 && optionIndex > closeBracket){
                String optionSQL = sql.substring(optionIndex);
                option = getOption(optionSQL, parseInfo);
                sql = sql.substring(0, optionIndex-1);
            }
            
            XQueryExpression expr = new SaxonXQueryExpression();
            expr.compileXQuery(sql);
            XQuery xquery = new XQuery(sql, expr);
            if (option != null){
                xquery.setOption(option);
            }
            return xquery;        
        } catch (MetaMatrixProcessingException e) {
            if(sql.startsWith(XML_OPEN_BRACKET) || sql.startsWith(XQUERY_DECLARE)) {
            	throw new QueryParserException(e, QueryPlugin.Util.getString("QueryParser.xqueryCompilation")); //$NON-NLS-1$
            }
        }

        throw qpe;
	}

    /**
     * Parses Option object given option SQL fragment.
     * @param optionSQL option SQL
     * @return Option object
     */
    private Option getOption(String optionSQL, ParseInfo parseInfo) throws QueryParserException {
    	Option result = null;
        try{
            result = getSqlParser(optionSQL).option(parseInfo);
            
        } catch(ParseException pe) {
            throw convertParserException(pe);

        } catch(TokenMgrError tme) {
            handleTokenMgrError(tme);
        }
        return result;
    }

    /**
     * Parse the String sql into a Command using the MetaMatrix parser.
     * @param sql to parse
     * @return parsed Command
     * @throws QueryParserException
     */
    private Command parseCommandWithParser(String sql, ParseInfo parseInfo) throws QueryParserException {
        Command result = null;
        try{
            result = getSqlParser(sql).command(parseInfo);
            
        } catch(ParseException pe) {
            throw convertParserException(pe);

        } catch(TokenMgrError tme) {
            handleTokenMgrError(tme);
        }
        return result;        
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
            handleTokenMgrError(tme);
        }
        return result;
    }

    private QueryParserException convertParserException(ParseException pe) {
        return new QueryParserException(QueryPlugin.Util.getString("QueryParser.parsingError", pe.getMessage())); //$NON-NLS-1$                        
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
            handleTokenMgrError(tme);
        }
        return result;
    }

    private void handleTokenMgrError(TokenMgrError tme) throws QueryParserException{
//            LogManager.logError( LogConstants.CTX_QUERY_PARSER, tme, new Object[] {"Exception parsing: ", sql} );

        try {
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
                    
                    QueryParserException qpe = new QueryParserException(QueryPlugin.Util.getString("QueryParser.lexicalError", tme.getMessage())); //$NON-NLS-1$
                    throw qpe;
                }

            }

        } catch(QueryParserException e) {
            throw e;
        } catch(Throwable e) {
            throw new QueryParserException(e, e.getMessage());
        }

        throw new QueryParserException(QueryPlugin.Util.getString("QueryParser.parsingError", tme.getMessage())); //$NON-NLS-1$
    }

}
