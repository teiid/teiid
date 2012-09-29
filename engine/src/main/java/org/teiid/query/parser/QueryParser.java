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

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.connector.DataPlugin;
import org.teiid.metadata.DuplicateRecordException;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Parser;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.Expression;

/**
 * <p>Converts a SQL-string to an object version of a query.  This
 * QueryParser can be reused but is NOT thread-safe as the parser uses an
 * input stream.  Putting multiple queries into the same stream will result
 * in unpredictable and most likely incorrect behavior.</p>
 */
public class QueryParser implements Parser {
    
    private static ThreadLocal<QueryParser> QUERY_PARSER = new ThreadLocal<QueryParser>() {
        /** 
         * @see java.lang.ThreadLocal#initialValue()
         */
        @Override
        protected QueryParser initialValue() {
            return new QueryParser();
        }
    };

    private static final String XQUERY_DECLARE = "declare"; //$NON-NLS-1$
    private static final String XML_OPEN_BRACKET = "<"; //$NON-NLS-1$

	private SQLParser parser;
	private TeiidSQLParserTokenManager tm;
    
	/**
	 * Construct a QueryParser - this may be reused.
	 */
	public QueryParser() {}
    
    public static QueryParser getQueryParser() {
        return QUERY_PARSER.get();
    }
	
	/**
	 * Helper method to get a SQLParser instance for given sql string.
	 */
	private SQLParser getSqlParser(String sql) {
		return getSqlParser(new StringReader(sql));
	}
	
	private SQLParser getSqlParser(Reader sql) {
		if(parser == null) {
			JavaCharStream jcs = new JavaCharStream(sql);
			tm = new TeiidSQLParserTokenManager(new JavaCharStream(sql));
			parser = new SQLParser(tm);
			parser.jj_input_stream = jcs;
		} else {
			parser.ReInit(sql);	
			tm.reinit();
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
	
	public Command parseProcedure(String sql, boolean update) throws QueryParserException {
		try{
			if (update) {
				return getSqlParser(sql).forEachRowTriggerAction(new ParseInfo());
			}
			Command result = getSqlParser(sql).procedureBodyCommand(new ParseInfo());
            result.setCacheHint(SQLParserUtil.getQueryCacheOption(sql));
            return result;
        } catch(ParseException pe) {
            throw convertParserException(pe);
        } finally {
        	tm.reinit();
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
             throw new QueryParserException(QueryPlugin.Event.TEIID30377, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30377));
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
            	 throw new QueryParserException(QueryPlugin.Event.TEIID30378, pe, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30378, sql));
            }
            throw convertParserException(pe);
        } finally {
        	tm.reinit();
        }
		return result;
	}
	
	public CacheHint parseCacheHint(String sql) {
        if(sql == null || sql.length() == 0) {
             return null;
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
        } finally {
        	tm.reinit();
        }
        return result;
    }

    private QueryParserException convertParserException(ParseException pe) {
    	if (pe.currentToken == null) {
    		List<Token> preceeding = findPreceeding(parser.token, 1);
    		if (!preceeding.isEmpty()) {
    			pe.currentToken = preceeding.get(0);
    		} else {
    			pe.currentToken = parser.token;
    		}
        }
        QueryParserException qpe = new QueryParserException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31100, getMessage(pe, 1, 10)));
        qpe.setParseException(pe);
        return qpe;
    }
        
    public String getMessage(ParseException pe, int maxTokenSequence, int maxExpansions) {
		if (!pe.specialConstructor) {
			if (pe.currentToken == null) {
				return pe.getMessage();
			}
			StringBuilder sb = encountered(pe, 1);
			sb.append(pe.getMessage());
			return sb.toString();
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
		StringBuilder retval = encountered(pe, maxSize);
		if (currentToken.next.kind == -1) {
			retval.append(QueryPlugin.Util.getString("QueryParser.lexicalError", currentToken.next.image)); //$NON-NLS-1$
			return retval.toString();
		}
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
			int j = start;
			for (; j < expectedTokenSequences[i].length && j < start+maxTokenSequence; j++) {
				expected.append(tokenImage[expectedTokenSequences[i][j]])
						.append(" "); //$NON-NLS-1$
			}
			if (j < expectedTokenSequences[i].length) {
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

	private StringBuilder encountered(ParseException pe, int maxSize) {
		StringBuilder retval = new StringBuilder("Encountered \""); //$NON-NLS-1$
		Token currentToken = pe.currentToken;
		List<Token> preceeding = findPreceeding(currentToken, 2);
		if (!preceeding.isEmpty()) {
			addTokenSequence(preceeding.size() + 1, retval, preceeding.get(0));
		} else {
			addTokenSequence(1, retval, currentToken);
		}
		if (currentToken.next.kind == -1) {
			maxSize = 1;
		}
		retval.append(" [*]"); //$NON-NLS-1$
		Token last = addTokenSequence(maxSize, retval, currentToken.next);
		if (last.kind != 0) {
			retval.append("[*]"); //$NON-NLS-1$
			if (last.next == null) {
				this.parser.getNextToken();
			}
			if (last.next != null) {
				retval.append(" "); //$NON-NLS-1$
				addTokenSequence(1, retval, last.next);
			}
		}
		retval.append("\" at line ").append(currentToken.next.beginLine).append(", column ").append(currentToken.next.beginColumn); //$NON-NLS-1$ //$NON-NLS-2$
		retval.append(".").append(pe.eol); //$NON-NLS-1$
		return retval;
	}

	private List<Token> findPreceeding(Token currentToken, int count) {
		LinkedList<Token> preceeding = new LinkedList<Token>();
		Token tok = this.tm.head;
		boolean found = false;
		while (tok != null) {
			if (tok == currentToken) {
				found = true;
				break;
			}
			preceeding.add(tok);
			if (preceeding.size() > count) {
				preceeding.removeFirst();
			}
			tok = tok.next;
		}
		if (!found) {
			preceeding.clear();
		}
		return preceeding;
	}

	private Token addTokenSequence(int maxSize, StringBuilder retval,
			Token tok) {
		Token last = tok;
		for (int i = 0; i < maxSize && tok != null; i++) {
			if (i != 0)
				retval.append(" "); //$NON-NLS-1$
			if (tok.kind == 0) {
				retval.append(SQLParserConstants.tokenImage[0]);
				return tok;
			}
			last = tok;
			add_escapes(tok.image, retval);
			tok = tok.next;
		}
		return last;
	}
	
	  /**
	   * Used to convert raw characters to their escaped version
	   * when these raw version cannot be used as part of an ASCII
	   * string literal.  Also escapes double quotes.
	   */
	  protected void add_escapes(String str, StringBuilder retval) {
	      for (int i = 0; i < str.length(); i++) {
		      char ch = str.charAt(i); 
	        switch (ch)
	        {
	           case 0 :
	              continue;
	           case '\b':
	              retval.append("\\b"); //$NON-NLS-1$
	              continue;
	           case '\t':
	              retval.append("\\t"); //$NON-NLS-1$
	              continue;
	           case '\n':
	              retval.append("\\n"); //$NON-NLS-1$
	              continue;
	           case '\f':
	              retval.append("\\f"); //$NON-NLS-1$
	              continue;
	           case '\r':
	              retval.append("\\r"); //$NON-NLS-1$
	              continue;
	           case '\"':
	              retval.append("\\\""); //$NON-NLS-1$
	              continue;
	           case '\\':
	              retval.append("\\\\"); //$NON-NLS-1$
	              continue;
	           default:
	              if (ch < 0x20 || ch > 0x7e) {
	                 String s = "0000" + Integer.toString(ch, 16); //$NON-NLS-1$
	                 retval.append("\\u" + s.substring(s.length() - 4, s.length())); //$NON-NLS-1$
	              } else {
	                 retval.append(ch);
	              }
	              continue;
	        }
	      }
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
        } finally {
        	tm.reinit();
        }
        return result;
    }
    
    public Expression parseSelectExpression(String sql) throws QueryParserException {
        if(sql == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("QueryParser.nullSqlExpr")); //$NON-NLS-1$
        }

        ParseInfo dummyInfo = new ParseInfo();

        Expression result = null;
        try{
            result = getSqlParser(sql).selectExpression(dummyInfo);

        } catch(ParseException pe) {
            throw convertParserException(pe);
        } finally {
        	tm.reinit();
        }
        return result;
    }

    public void parseDDL(MetadataFactory factory, String ddl) {
    	parseDDL(factory, new StringReader(ddl));
    }
    
    public void parseDDL(MetadataFactory factory, Reader ddl) {
    	try {
			getSqlParser(ddl).parseMetadata(factory);
		} catch (ParseException e) {
			throw new org.teiid.metadata.ParseException(QueryPlugin.Event.TEIID30386, convertParserException(e));
		} finally {
        	tm.reinit();
        }
    	HashSet<FunctionMethod> functions = new HashSet<FunctionMethod>();
    	for (FunctionMethod functionMethod : factory.getSchema().getFunctions().values()) {
			if (!functions.add(functionMethod)) {
				throw new DuplicateRecordException(DataPlugin.Event.TEIID60015, DataPlugin.Util.gs(DataPlugin.Event.TEIID60015, functionMethod.getName()));
			}
		}
    }

}
