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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.Assertion;
import org.teiid.core.util.StringUtil;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.proc.Statement;

public class SQLParserUtil {
	
	String matchesAny(String arg, String ... expected) {
		for (String string : expected) {
			if (string.equalsIgnoreCase(arg)) {
				return arg;
			}
		}
		return null;
	}
	
	String normalizeStringLiteral(String s) {
		int start = 1;
		boolean unescape = false;
  		if (s.charAt(0) == 'N') {
  			start++;
  		} else if (s.charAt(0) == 'E') {
  			start++;
  			unescape = true;
  		}
  		char tickChar = s.charAt(start - 1);
  		s = s.substring(start, s.length() - 1);
  		String result = removeEscapeChars(s, String.valueOf(tickChar));
  		if (unescape) {
  			result = FunctionMethods.unescape(result);
  		}
  		return result;
	}
	
	public static String normalizeId(String s) {
		if (s.indexOf('"') == -1) {
			return s;
		}
		List<String> nameParts = new LinkedList<String>();
		while (s.length() > 0) {
			if (s.charAt(0) == '"') {
				boolean escape = false;
				for (int i = 1; i < s.length(); i++) {
					if (s.charAt(i) != '"') {
						continue;
					}
					escape = !escape;
					boolean end = i == s.length() - 1;
					if (end || (escape && s.charAt(i + 1) == '.')) {
				  		String part = s.substring(1, i);
				  		s = s.substring(i + (end?1:2));
				  		nameParts.add(removeEscapeChars(part, "\"")); //$NON-NLS-1$
				  		break;
					}
				}
			} else {
				int index = s.indexOf('.');
				if (index == -1) {
					nameParts.add(s);
					break;
				} 
				nameParts.add(s.substring(0, index));
				s = s.substring(index + 1);
			}
		}
		StringBuilder sb = new StringBuilder();
		for (Iterator<String> i = nameParts.iterator(); i.hasNext();) {
			sb.append(i.next());
			if (i.hasNext()) {
				sb.append('.');
			}
		}
		return sb.toString();
	}
	
    /**
     * Check if this is a valid string literal
     * @param id Possible string literal
     */
    boolean isStringLiteral(String str, ParseInfo info) {
    	if (info.useAnsiQuotedIdentifiers() || str.charAt(0) != '"' || str.charAt(str.length() - 1) != '"') {
    		return false;
    	}
    	int index = 1;
    	while (index < str.length() - 1) {
    		index = str.indexOf('"', index);
    		if (index == -1 || index + 1 == str.length()) {
    			return true;
    		}
    		if (str.charAt(index + 1) != '"') {
    			return false;
    		}
    		index += 2;
    	}
    	return true;
    }    

    /**
     * Check that this is a valid alias, remove quotes, and return updated
     * alias string.
     * @param id Metadata alias
     */
    String validateAlias(String id) throws ParseException {
        return validateName(id, false);
    }

    private String validateName(String id, boolean element) throws ParseException {
        if(id.indexOf('.') != -1) { 
            String key = "SQLParser.Invalid_alias"; //$NON-NLS-1$
            if (element) {
                key = "SQLParser.Invalid_short_name"; //$NON-NLS-1$
            }
            throw new ParseException(QueryPlugin.Util.getString(key, id)); 
        }
        return id;
    }
    
    /**
     * Check that this is a valid element name, remove quotes
     * @param id Metadata alias
     */
    String validateElementName(String id) throws ParseException {
        return validateName(id, true);
    }
    
    static String removeEscapeChars(String str, String tickChar) {
        return StringUtil.replaceAll(str, tickChar + tickChar, tickChar);
    }
    
    void setFromClauseOptions(Token groupID, FromClause fromClause){
        String[] parts = getComment(groupID).split("\\s"); //$NON-NLS-1$

        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase(Option.OPTIONAL)) {
                fromClause.setOptional(true);
            } else if (parts[i].equalsIgnoreCase(Option.MAKEDEP)) {
                fromClause.setMakeDep(true);
            } else if (parts[i].equalsIgnoreCase(Option.MAKENOTDEP)) {
                fromClause.setMakeNotDep(true);
            } else if (parts[i].equalsIgnoreCase(FromClause.MAKEIND)) {
                fromClause.setMakeInd(true);
            }       
        }
    }
    
    SubqueryHint getSubqueryHint(Token t) {
    	SubqueryHint hint = new SubqueryHint();
    	String[] parts = getComment(t).split("\\s"); //$NON-NLS-1$
    	for (int i = 0; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase(SubqueryHint.MJ)) {
                hint.setMergeJoin(true);
            } else if (parts[i].equalsIgnoreCase(SubqueryHint.NOUNNEST)) {
            	hint.setNoUnnest(true);
            } else if (parts[i].equalsIgnoreCase(SubqueryHint.DJ)) {
                hint.setDepJoin(true);
            }
        }
    	return hint;
    }
    
	String getComment(Token t) {
		Token optToken = t.specialToken;
        if (optToken == null) { 
            return ""; //$NON-NLS-1$
        }
        String hint = optToken.image.substring(2, optToken.image.length() - 2);
        if (hint.startsWith("+")) { //$NON-NLS-1$
        	hint = hint.substring(1);
        }
        return hint;
	}
	
	private static Pattern CACHE_HINT = Pattern.compile("/\\*\\+?\\s*cache(\\(\\s*(pref_mem)?\\s*(ttl:\\d{1,19})?\\s*(updatable)?\\s*(scope:(session|vdb|user))?[^\\)]*\\))?[^\\*]*\\*\\/.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL); //$NON-NLS-1$
    
	static CacheHint getQueryCacheOption(String query) {
    	Matcher match = CACHE_HINT.matcher(query);
    	if (match.matches()) {
    		CacheHint hint = new CacheHint();
    		if (match.group(2) !=null) {
    			hint.setPrefersMemory(true);
    		}
    		String ttl = match.group(3);
    		if (ttl != null) {
    			hint.setTtl(Long.valueOf(ttl.substring(4)));
    		}
    		if (match.group(4) != null) {
    			hint.setUpdatable(true);
    		}
    		String scope =  match.group(5);
    		if (scope != null) {
    			scope = scope.substring(6);
    			hint.setScope(scope);
    		}    		
    		return hint;
    	}
    	return null;
    }

    /**
     * Helper for the FROM clause that takes the join type string and adds
     * the join type to the From clause based on that type.
     * @param groupID Left group ID
     * @param rid Right group ID
     * @param joinType Join type word from query
     * @param from From clause to update
     * @throws ParseException if parsing failed
     */
    JoinType getJoinType(Token joinTypeToken) throws ParseException {
        if(joinTypeToken == null) { 
            return JoinType.JOIN_INNER;
        }   
        String joinType = joinTypeToken.image;
        if(joinType.equalsIgnoreCase(Reserved.INNER)) {
            return JoinType.JOIN_INNER;
        } else if(joinType.equalsIgnoreCase(Reserved.CROSS)) {
            return JoinType.JOIN_CROSS;         
        } else if(joinType.equalsIgnoreCase(Reserved.LEFT)) {
            return JoinType.JOIN_LEFT_OUTER;
        } else if(joinType.equalsIgnoreCase(Reserved.RIGHT)) {
            return JoinType.JOIN_RIGHT_OUTER;
        } else if(joinType.equalsIgnoreCase(Reserved.FULL)) {
            return JoinType.JOIN_FULL_OUTER;
        } else if(joinType.equalsIgnoreCase(Reserved.UNION)) {
            return JoinType.JOIN_UNION;
        } else {
            Object[] params = new Object[] { joinType };
            throw new ParseException(QueryPlugin.Util.getString("SQLParser.Unknown_join_type", params)); //$NON-NLS-1$
        }
    }
    
    /**
     * Generate an expression name based on the function type and previous names.
     * @param info Parse info, including counts for each function type
     * @param functionType Null for expression, the function name for aggregates
     * @return New unique function name
     */
    String generateFunctionName(ParseInfo info, String functionType) {
    	if (functionType == null) {
    		functionType = "expr"; //$NON-NLS-1$
    	} else {
    		functionType = functionType.toLowerCase();
    	}
        Integer num = info.nameCounts.get(functionType);
        if (num == null) {
        	num = 0;
        }
        info.nameCounts.put(functionType, num + 1);
        return functionType + (num == 0 ? "" : ""+num); //$NON-NLS-1$   //$NON-NLS-2$  
    }
    
    int getOperator(String opString) {
        if (opString.equals("=")) { //$NON-NLS-1$
            return CriteriaSelector.COMPARE_EQ;
        } else if (opString.equals("<>") || opString.equals("!=")) { //$NON-NLS-1$ //$NON-NLS-2$
            return CriteriaSelector.COMPARE_NE;
        } else if (opString.equals("<")) { //$NON-NLS-1$
            return CriteriaSelector.COMPARE_LT;
        } else if (opString.equals(">")) { //$NON-NLS-1$
            return CriteriaSelector.COMPARE_GT;
        } else if (opString.equals("<=")) { //$NON-NLS-1$
            return CriteriaSelector.COMPARE_LE;
        } else if (opString.equals(">=")) { //$NON-NLS-1$
            return CriteriaSelector.COMPARE_GE;
        } else if (opString.equalsIgnoreCase("like")) { //$NON-NLS-1$
            return CriteriaSelector.LIKE;
        } else if (opString.equalsIgnoreCase("in")) { //$NON-NLS-1$
            return CriteriaSelector.IN;
        } else if (opString.equalsIgnoreCase("is")) { //$NON-NLS-1$
            return CriteriaSelector.IS_NULL;
        } else if (opString.equalsIgnoreCase("between")) { //$NON-NLS-1$
            return CriteriaSelector.BETWEEN;
        }
        
        Assertion.failed("unknown operator"); //$NON-NLS-1$
        return 0;
    }
    
    SetQuery addQueryToSetOperation(QueryCommand query, QueryCommand rightQuery, SetQuery.Operation type, boolean all) {
        SetQuery setQuery = new SetQuery(type, all, query, rightQuery);
        return setQuery;
    }
    
    static Block asBlock(Statement stmt) {
    	if (stmt == null) {
    		return null;
    	}
    	if (stmt instanceof Block) {
    		return (Block)stmt;
    	}
    	return new Block(stmt);
    }
    
}
