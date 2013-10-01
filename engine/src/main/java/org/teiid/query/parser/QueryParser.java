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

import static org.teiid.query.parser.SQLParserConstants.*;
import static org.teiid.query.parser.TeiidSQLParserTokenManager.*;

import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.connector.DataPlugin;
import org.teiid.language.SQLConstants;
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
            	 throw new QueryParserException(QueryPlugin.Event.TEIID30378, convertParserException(pe), QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30378, sql));
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
        QueryParserException qpe = new QueryParserException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31100, getMessage(pe, 10)));
        qpe.setParseException(pe);
        return qpe;
    }
        
    /**
     * The default JavaCC message is not very good.  This method produces a much more readable result.
     * @param pe
     * @param maxExpansions
     * @return
     */
    public String getMessage(ParseException pe, int maxExpansions) {
		if (pe.expectedTokenSequences == null) {
			if (pe.currentToken == null) {
				return pe.getMessage();
			}
			StringBuilder sb = encountered(pe, pe.currentToken.next!=null?1:0);
			if (pe.currentToken.kind == INVALID_TOKEN) {
				sb.append(QueryPlugin.Util.getString("QueryParser.lexicalError", pe.currentToken.image)); //$NON-NLS-1$
			} else if (pe.currentToken.next != null && pe.currentToken.next.kind == -1) {
				sb.append(QueryPlugin.Util.getString("QueryParser.lexicalError", pe.currentToken.next.image)); //$NON-NLS-1$
			}
			if (pe.getMessage() != null) {
				sb.append(pe.getMessage());
			}
			return sb.toString();
		}

		Token currentToken = pe.currentToken; 

		//if the next token is invalid, we wan to use a lexical message, not the sequences
		if (currentToken.next.kind == INVALID_TOKEN) {
			StringBuilder retval = encountered(pe, 1);
			retval.append(QueryPlugin.Util.getString("QueryParser.lexicalError", currentToken.next.image)); //$NON-NLS-1$
			return retval.toString();
		}

		//find the longest match chain an all possible end tokens
		int[][] expectedTokenSequences = pe.expectedTokenSequences;
		int[] ex = null;
		Set<Integer> last = new TreeSet<Integer>();
		outer : for (int i = 0; i < expectedTokenSequences.length; i++) {
			if (ex == null || expectedTokenSequences[i].length > ex.length) {
				ex = expectedTokenSequences[i];
				last.clear();
			} else if (expectedTokenSequences[i].length < ex.length) {
				continue;
			} else {
				for (int j = 0; j < ex.length -1; j++) {
					if (ex[j] != expectedTokenSequences[i][j]) {
						continue outer; //TODO : not sure how to handle this case
					}
				}
			}
			last.add(expectedTokenSequences[i][expectedTokenSequences[i].length-1]);
		}
		if (ex == null) {
			return pe.getMessage(); //shouldn't happen
		}
		
		StringBuilder retval = encountered(pe, ex.length);
		
		//output the expected tokens condensing the id/non-reserved
		retval.append("Was expecting: "); //$NON-NLS-1$ 
		boolean id = last.contains(SQLParserConstants.ID);
		int count = 0;
		for (Integer t : last) {
			String img = tokenImage[t];
			if (id && img.startsWith("\"") //$NON-NLS-1$ 
					&& Character.isLetter(img.charAt(1)) 
					&& !SQLConstants.isReservedWord(img.substring(1, img.length()-1))) {
				continue;
			}
			if (count > 0) {
				retval.append(" | "); //$NON-NLS-1$
			}
			count++;
			if (t == SQLParserConstants.ID) {
				retval.append("id"); //$NON-NLS-1$
			} else {
				retval.append(img);
			}
			if (count == maxExpansions) {
				retval.append(" ..."); //$NON-NLS-1$
				break;
			}
		}
		return retval.toString();
    }

	private StringBuilder encountered(ParseException pe, int offset) {
		StringBuilder retval = new StringBuilder("Encountered \""); //$NON-NLS-1$
		Token currentToken = pe.currentToken;
		for (int i = 1; i < offset; i++) {
			//TODO: for large offsets we don't have to call findPreceeding
			currentToken = currentToken.next;
		}
		List<Token> preceeding = findPreceeding(currentToken, 2);
		if (offset > 0 && !preceeding.isEmpty()) {
			addTokenSequence(preceeding.size() + 1, retval, null, preceeding.get(0), false);
		} else {
			addTokenSequence(1, retval, null, currentToken, offset==0);
		}
		if (currentToken.next != null && offset>0) {
			addTokenSequence(3, retval, currentToken, currentToken.next, true);
			currentToken = currentToken.next; //move to the error token
		}
		retval.append("\" at line ").append(currentToken.beginLine).append(", column ").append(currentToken.beginColumn); //$NON-NLS-1$ //$NON-NLS-2$
		retval.append(".\n"); //$NON-NLS-1$
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
			Token last, Token tok, boolean highlight) {
		for (int i = 0; i < maxSize && tok != null; i++) {
			if (last != null && last.endColumn + 1 != tok.beginColumn && tok.kind != SQLParserConstants.EOF) {
				retval.append(" "); //$NON-NLS-1$
			}
			last = tok;
			if (i == 0 && highlight) {
				retval.append("[*]"); //$NON-NLS-1$
			}
			if (tok.image != null && !tok.image.isEmpty()) {
				add_escapes(tok.image, retval);
				if (i == 0 && highlight) {
					retval.append("[*]"); //$NON-NLS-1$
				}
			}
			while (tok.next == null) {
				if (this.parser.getNextToken() == null) {
					break;
				}
			}
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
