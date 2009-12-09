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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.proc.CriteriaSelector;

public class SQLParserUtil {
	
	String normalizeStringLiteral(String s) {
		int start = 1;
  		if (s.charAt(0) == 'N') {
  			start++;
  		}
  		char tickChar = s.charAt(start - 1);
  		s = s.substring(start, s.length() - 1);
  		return removeEscapeChars(s, String.valueOf(tickChar));
	}
	
	String normalizeId(String s) {
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
	
    String validateFunctionName(String id) throws ParseException {
    	int length = id.length();
        for(int i=0; i<length; i++) { 
            char c = id.charAt(i);
            if(! (c == '_' || StringUtil.isLetterOrDigit(c))) { 
                throw new ParseException(QueryPlugin.Util.getString("SQLParser.Invalid_func", id)); //$NON-NLS-1$   
            }
        }
        return id;
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
    
    String removeEscapeChars(String str, String tickChar) {
        return StringUtil.replaceAll(str, tickChar + tickChar, tickChar);
    }
    
    void setFromClauseOptions(Token groupID, FromClause fromClause){
        Token optToken = groupID.specialToken;
        if (optToken == null) { 
            return;
        }
        String hint = optToken.image.substring(2, optToken.image.length() - 2);
        String[] parts = hint.split("\\s"); //$NON-NLS-1$

        HashSet<String> set = new HashSet<String>();
        
        for (int i = 0; i < parts.length; i++) {
            set.add(parts[i].toLowerCase());
        }
        
        if (set.contains(Option.OPTIONAL)) {
            fromClause.setOptional(true);
        }        
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
        if(joinType.equalsIgnoreCase(ReservedWords.INNER)) {
            return JoinType.JOIN_INNER;
        } else if(joinType.equalsIgnoreCase(ReservedWords.CROSS)) {
            return JoinType.JOIN_CROSS;         
        } else if(joinType.equalsIgnoreCase(ReservedWords.LEFT)) {
            return JoinType.JOIN_LEFT_OUTER;
        } else if(joinType.equalsIgnoreCase(ReservedWords.RIGHT)) {
            return JoinType.JOIN_RIGHT_OUTER;
        } else if(joinType.equalsIgnoreCase(ReservedWords.FULL)) {
            return JoinType.JOIN_FULL_OUTER;
        } else if(joinType.equalsIgnoreCase(ReservedWords.UNION)) {
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
    String generateFunctionName(ParseInfo info, String functionType) throws ParseException {
        if(functionType == null) { 
            int num = info.anonExprCount++;
            return "expr" + (num == 0 ? "" : ""+num); //$NON-NLS-1$   //$NON-NLS-2$   //$NON-NLS-3$

        } else if(functionType.equals(ReservedWords.COUNT)) { 
            int num = info.anonCountCount++;
            return "count" + (num == 0 ? "" : ""+num);//$NON-NLS-1$   //$NON-NLS-2$   //$NON-NLS-3$

        } else if(functionType.equals(ReservedWords.SUM)) { 
            int num = info.anonSumCount++;
            return "sum" + (num == 0 ? "" : ""+num);//$NON-NLS-1$   //$NON-NLS-2$   //$NON-NLS-3$

        } else if(functionType.equals(ReservedWords.AVG)) { 
            int num = info.anonAvgCount++;
            return "avg" + (num == 0 ? "" : ""+num);//$NON-NLS-1$   //$NON-NLS-2$   //$NON-NLS-3$

        } else if(functionType.equals(ReservedWords.MIN)) { 
            int num = info.anonMinCount++;
            return "min" + (num == 0 ? "" : ""+num);//$NON-NLS-1$   //$NON-NLS-2$   //$NON-NLS-3$

        } else if(functionType.equals(ReservedWords.MAX)) { 
            int num = info.anonMaxCount++;
            return "max" + (num == 0 ? "" : ""+num);//$NON-NLS-1$   //$NON-NLS-2$   //$NON-NLS-3$
        } else {
            Object[] params = new Object[] { functionType };
            throw new ParseException(QueryPlugin.Util.getString("SQLParser.Unknown_agg_func", params)); //$NON-NLS-1$
        }
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
    
}
