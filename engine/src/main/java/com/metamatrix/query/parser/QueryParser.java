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
import com.metamatrix.query.sql.proc.Block;
import com.metamatrix.query.sql.proc.CriteriaSelector;
import com.metamatrix.query.sql.proc.Statement;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.xquery.XQueryExpression;
import com.metamatrix.query.xquery.saxon.SaxonXQueryExpression;

/**
 * <p>Converts a SQL-string to an object version of a query.  This
 * QueryParser can be reused but is NOT thread-safe as the parser uses an
 * input stream.  Putting multiple queries into the same stream will result
 * in unpredictable and most likely incorrect behavior.</p>
 *
 * <p>In the future this class may hide a single- or multi-thread cache
 * of parsed queries. </p>
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
    private static final String BOGUS_SELECT = "select x "; //$NON-NLS-1$

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
        
        // For XQueries
        int type = getCommandType(sql);
        if (type == Command.TYPE_XQUERY || type == Command.TYPE_UNKNOWN){
            
            try {
                
                // Check for OPTION
                Option option = null;
                int optionIndex = sql.toUpperCase().lastIndexOf(ReservedWords.OPTION);
                if (optionIndex != -1){
                    String optionSQL = sql.substring(optionIndex);
                    option = getOption(optionSQL);
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
                if (type == Command.TYPE_XQUERY){
                    throw new QueryParserException(QueryPlugin.Util.getString("QueryParser.xqueryCompilation", e.getMessage())); //$NON-NLS-1$
                } // else let unknown query type fall through to code below for better error handling
            }
        }

        Command result = parseCommandWithParser(sql, parseInfo);
		return result;
	}

    /**
     * Parses Option object given option SQL fragment.
     * @param optionSQL option SQL
     * @return Option object
     */
    private Option getOption(String optionSQL) throws QueryParserException {
        String sql = BOGUS_SELECT + optionSQL;
        Command command = parseCommandWithParser(sql, new ParseInfo());
        return command.getOption();
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
     * Takes a SQL string representing an SQL statement
     * and returns the procedure object representation.
     * @param stmt SQL statement string
     * @return Statement Procedure object representation
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if stmt is null
     */
     Statement parseStatement(String stmt) throws QueryParserException {
        if(stmt == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlCrit")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        Statement result = null;
        try{
            result = getSqlParser(stmt).statement(dummyInfo);

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

    /**
     * Takes a SQL string representing a block
     * and returns the object representation.
     * @param block Block string
     * @return Block Block object representation
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if sql is null
     */
     Block parseBlock(String block) throws QueryParserException {
        if(block == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullBlock")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        Block result = null;
        try{
            result = getSqlParser(block).block(dummyInfo);

        } catch(ParseException pe) {
            throw convertParserException(pe);

        } catch(TokenMgrError tme) {
            handleTokenMgrError(tme);
        }
        return result;
    }
     
    private QueryParserException convertParserException(ParseException pe) {
        QueryParserException qpe = null;
            
        qpe = new QueryParserException(QueryPlugin.Util.getString("QueryParser.parsingError", pe.getMessage())); //$NON-NLS-1$                        
        
        return qpe;
    }

    /**
     * Takes a SQL string representing a criteria selector
     * and returns the object representation.
     * @param selector criteria selector string
     * @return CriteriaSelector CriteriaSelector object representation
     * @throws QueryParserException if parsing fails
     * @throws IllegalArgumentException if sql is null
     */
    CriteriaSelector parseCriteriaSelector(String selector) throws QueryParserException {
        if(selector == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlCrit")); //$NON-NLS-1$
        }

        CriteriaSelector result = null;
        try{
            result = getSqlParser(selector).criteriaSelector();

        } catch(ParseException pe) {
            throw convertParserException(pe);

        } catch(TokenMgrError tme) {
            handleTokenMgrError(tme);
        }
        return result;
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

	/**
	 * Takes a SQL string and determines the command type, as defined in the constants
	 * of {@link com.metamatrix.query.sql.lang.Command}.
	 * @param sql SQL string
	 * @return Command type code, as defined in {@link com.metamatrix.query.sql.lang.Command}.
     * Note: not all XQuery queries will necessarily be detected - if type UNKNOWN is indicated
     * it's possible it is an XQuery.
	 * @throws IllegalArgumentException if sql is null
	 */
	public static int getCommandType(String sql) {
		if(sql == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.emptysql")); //$NON-NLS-1$
		}

        // Shortcut for most XQuery commands
        if(sql.startsWith(XML_OPEN_BRACKET) || sql.startsWith(XQUERY_DECLARE)) {
            return Command.TYPE_XQUERY;
        }

		int startCommand = -1;
		for(int index = 0; index < sql.length(); index++) {
			char c = sql.charAt(index);
			if(Character.isLetter(c) || c == '{') {
			    startCommand = index;
			    break;
			} 
		}

		if(startCommand == -1) {
			return Command.TYPE_UNKNOWN;
		}

		String commandWord = sql.substring(startCommand, Math.min(startCommand+6, sql.length())).toUpperCase();
        if(commandWord.startsWith(ReservedWords.SELECT)) {
			return Command.TYPE_QUERY;
		} else if(commandWord.startsWith(ReservedWords.INSERT)) {
			return Command.TYPE_INSERT;
		} else if(commandWord.startsWith(ReservedWords.UPDATE)) {
			return Command.TYPE_UPDATE;
		} else if(commandWord.startsWith(ReservedWords.DELETE)) {
			return Command.TYPE_DELETE;
		} else if(commandWord.startsWith(ReservedWords.EXEC) || commandWord.startsWith("{")) { //$NON-NLS-1$
			return Command.TYPE_STORED_PROCEDURE;
        } else if(commandWord.startsWith(ReservedWords.CREATE)) { 
            return Command.TYPE_UPDATE_PROCEDURE;
		} else {
			return Command.TYPE_UNKNOWN;
		}
	}

}
