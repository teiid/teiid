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

    /**
     * Check that this is a valid group or element ID
     * @param id Group ID string
     */
    boolean isMetadataID(String id) throws ParseException {
        int length = id.length();
        
        if(id.indexOf("mmuuid:") >= 0) { //$NON-NLS-1$
            // Validate modeler form.  Example: "mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0"
            
            int dotIndex = id.indexOf("."); //$NON-NLS-1$
            if(dotIndex >= 0) { 
                String groupPart = id.substring(0, dotIndex);
                String lastPart = id.substring(dotIndex+1);
                if(isModelerID(groupPart) || isMetadataPart(groupPart)) {
                    return (lastPart.equals("*") || isModelerID(lastPart)); //$NON-NLS-1$
                } 
                return false;
            } 
            return isModelerID(id);                
        } 
        
        // Validate server forms:   
        //  group, vdb.group, "group", vdb."group",
        //  group.*, vdb.group.*, "group".*, vdb."group".*,
        //  group.element, vdb.group.element, "group".element, vdb."group".element
        //  tag.tag.tag
        //  tag.tag.@attribute
        
        // Check first character - must be letter or "
        char c = id.charAt(0);
        if( ! (c == '\"' || c == '#' || StringUtil.isLetter(c)) ) {
            return false;
        }

        // Check middle characters - letter, number, _, "        
        if(length > 1) {
            for(int i=1; i<length; i++) { 
                c = id.charAt(i);
                if( ! (c == '.' || c == '_' || c == '\"' || c == '/' || c == '@' || StringUtil.isLetterOrDigit(c)) ) {                 
                    // Allow last character to be * as well
                    if( i == (length-1) ) {
                        if(c != '*') { 
                            return false;
                        }    
                    } else {
                        return false;
                    }    
                }
            }
        }
        
        return true;
    }    

    /**
     * Check that this is a valid mmuuid
     * @param id Group ID string
     */
    boolean isModelerID(String id) throws ParseException {
        int length = id.length();
        
        if(id.startsWith("mmuuid:")) { //$NON-NLS-1$
            // Validate modeler form.  Example: "mmuuid:345f22c0-3236-1dfa-9931-e83d04ce10a0"
            for(int i=7; i<length; i++) { 
                char c = id.charAt(i);
                if( ! (c == '-' || (c >= 'a' && c <= 'f') || StringUtil.isDigit(c)) ) {
                    return false;
                }
            }  
            return true;
        }  
        return false;
    }        
        
    /**
     * Check that this is a valid function name
     * @param id Function name string
     */
    boolean isFunctionName(String id) throws ParseException {
        int length = id.length();
        for(int i=0; i<length; i++) { 
            char c = id.charAt(i);
            if(! (c == '_' || StringUtil.isLetterOrDigit(c))) { 
                return false;    
            }
        }
        return true;
    } 
    
    String validateFunctionName(String id) throws ParseException {
        if(isFunctionName(id)) {
            return id;
        } 

        Object[] params = new Object[] { id };
        throw new ParseException(QueryPlugin.Util.getString("SQLParser.Invalid_func", params)); //$NON-NLS-1$
    }

    /**
     * Check that this is a valid alias
     * @param alias Alias string
     */
    boolean isAlias(String alias) throws ParseException {
        if((alias.charAt(0) == '\"' && alias.charAt(alias.length()-1) == '\"')
        ||(alias.charAt(0) == '\'' && alias.charAt(alias.length()-1) == '\'')) {
            return isMetadataPart(alias.substring(1, alias.length()-1));
        }   
        return isMetadataPart(alias);
    }

    /**
     * Check that this is a valid metadata part - starts with a letter and contains alpha, numeric, _
     * @param part Metadata part string
     */
    boolean isMetadataPart(String part) throws ParseException {
        int length = part.length();
        
        // Check first character - must be letter
        char c = part.charAt(0);
        if( ! StringUtil.isLetter(c) ) {
            return false;
        }
        
        // Check other characters - letter, number, _       
        if(length > 1) {
            for(int i=1; i<length; i++) { 
                c = part.charAt(i);
                if( ! (c == '_' || StringUtil.isLetterOrDigit(c)) ) {                 
                    return false;
                }
            }
        }
        
        return true;
    }    

    /**
     * Check if this is a valid string literal
     * @param id Possible string literal
     */
    boolean isStringLiteral(String str, ParseInfo info) throws ParseException {
        // Check first last characters first - this is a requirement and should
        // fail quickly in most cases
        if(str.charAt(0) != '\"' || str.charAt(str.length()-1) != '\"') {
            return false;
        }
        
        // Check whether this is a string like "abcdefg" or a variable like "category.group"."element"
        
        // First, tokenize on periods (note that periods may be embedded in quoted parts)
        List tokens = StringUtil.split(str, "."); //$NON-NLS-1$
        if(tokens.size() < 2) { 
            // No periods, so this must be a string literal
            return info.allowDoubleQuotedVariable()? false : true;
        }   
        // Start at second token (i=1) and look at pairs of this and previous for 
        // a pair that ends and begins in ".  Also, have to make sure that " is not 
        // part of an escaped quote: "abc"".""def" should be a string literal while
        // "abc"."def" should be a variable.
        for(int i=1; i<tokens.size(); i++) {
            String first = (String) tokens.get(i-1);
            String second = (String) tokens.get(i);
            if( second.charAt(0) == '\"' &&
                second.charAt(1) != '\"' && 
                first.charAt(first.length()-1) == '\"' && 
                first.charAt(first.length()-2) != '\"' ) {
                
                return false;
            }
        }
        
        // Didn't find any evidence that this is a dotted variable, so must be a string
        // unless we are allowing double quoted variable, like ODBC metadata tools use.
        return info.allowDoubleQuotedVariable()? false : true;
    }    

    /**
     * Check that this is a valid metadata ID, remove quotes, and return updated
     * ID string.
     * @param id Metadata ID string
     */
    String validateMetadataID(String id) throws ParseException {
        if(! isMetadataID(id)) { 
            Object[] params = new Object[] { id };
            throw new ParseException(QueryPlugin.Util.getString("SQLParser.Invalid_id", params)); //$NON-NLS-1$
        }
        id = id.replace('/', '.');
        id = id.replaceAll("\"", ""); //$NON-NLS-1$ //$NON-NLS-2$
        return id;
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
        if(! isAlias(id)) { 
            Object[] params = new Object[] { id };
            String key = "SQLParser.Invalid_alias"; //$NON-NLS-1$
            if (element) {
                key = "SQLParser.Invalid_short_name"; //$NON-NLS-1$
            }
            throw new ParseException(QueryPlugin.Util.getString(key, params)); 
        }
        // Remove Quotes and Single Tick
        id = removeCharacter(id, '"');
        return removeCharacter(id, '\'');
    }
    
    /**
     * Check that this is a valid element name, remove quotes
     * @param id Metadata alias
     */
    String validateElementName(String id) throws ParseException {
        return validateName(id, true);
    }
    
    /**
     * Remove all quotes from the specified id string
     * @param id Input string
     * @param remove character to be removed from id
     * @return string from which remove character is removed, 
     * if no instances of remove character is found original string returned
     */
    String removeCharacter(String id, char remove) {
        if(id.indexOf(remove) >= 0) {
            StringBuffer newStr = new StringBuffer();
            int length = id.length();
            for(int i=0; i<length; i++) { 
                char c = id.charAt(i);
                if(c != remove) { 
                    newStr.append(c);  
                } 
            }
            return newStr.toString();    
        } 
        return id;
    } 

    String removeEscapeChars(String str, char tickChar) {
        String doubleTick = "" + tickChar + tickChar; //$NON-NLS-1$
        int index = str.indexOf(doubleTick);
        if(index < 0) { 
            return str;
        } 

        int last = 0;
        StringBuffer temp = new StringBuffer();         
        while(index >= 0) {
            temp.append(str.substring(last, index));
            temp.append(tickChar);
            last = index+2;
            index = str.indexOf(doubleTick, last);
        }
        
        if(last <= (str.length()-1)) {
            temp.append(str.substring(last));    
        }
        
        return temp.toString();         
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
