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

package org.teiid.language.visitor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.DerivedColumn;
import org.teiid.language.DerivedTable;
import org.teiid.language.Exists;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Function;
import org.teiid.language.GroupBy;
import org.teiid.language.In;
import org.teiid.language.Insert;
import org.teiid.language.IsNull;
import org.teiid.language.Join;
import org.teiid.language.LanguageObject;
import org.teiid.language.Like;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Not;
import org.teiid.language.OrderBy;
import org.teiid.language.QueryExpression;
import org.teiid.language.SQLReservedWords;
import org.teiid.language.ScalarSubquery;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.Select;
import org.teiid.language.SetClause;
import org.teiid.language.SetQuery;
import org.teiid.language.SortSpecification;
import org.teiid.language.SubqueryComparison;
import org.teiid.language.SubqueryIn;
import org.teiid.language.TableReference;
import org.teiid.language.Update;
import org.teiid.language.Argument.Direction;
import org.teiid.language.SQLReservedWords.NonReserved;
import org.teiid.language.SQLReservedWords.Tokens;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.metadata.AbstractMetadataRecord;


/**
 * Creates a SQL string for a LanguageObject subtree. Instances of this class
 * are not reusable, and are not thread-safe.
 */
public class SQLStringVisitor extends AbstractLanguageVisitor {
   
    private Set<String> infixFunctions = new HashSet<String>(Arrays.asList("%", "+", "-", "*", "+", "/", "||", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ 
    		"&", "|", "^", "#"));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
	
    protected static final String UNDEFINED = "<undefined>"; //$NON-NLS-1$
    protected static final String UNDEFINED_PARAM = "?"; //$NON-NLS-1$
    
    protected StringBuilder buffer = new StringBuilder();
                
    /**
     * Gets the name of a group or element from the RuntimeMetadata
     * @param id the id of the group or element
     * @return the name of that element or group as defined in the source
     */
    protected String getName(AbstractMetadataRecord object) {
        String nameInSource = object.getNameInSource();
        if(nameInSource != null && nameInSource.length() > 0) {
            return nameInSource;
        }
        return object.getName();
    }
    
    /**
     * Appends the string form of the ILanguageObject to the current buffer.
     * @param obj the language object instance
     */
    public void append(LanguageObject obj) {
        if (obj == null) {
            buffer.append(UNDEFINED);
        } else {
            visitNode(obj);
        }
    }
    
    /**
     * Simple utility to append a list of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items a list of ILanguageObjects
     */
    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                buffer.append(Tokens.COMMA)
                      .append(Tokens.SPACE);
                append(items.get(i));
            }
        }
    }
    
    /**
     * Simple utility to append an array of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items an array of ILanguageObjects
     */
    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            append(items[0]);
            for (int i = 1; i < items.length; i++) {
                buffer.append(Tokens.COMMA)
                      .append(Tokens.SPACE);
                append(items[i]);
            }
        }
    }
    
    /**
     * Creates a SQL-safe string. Simply replaces all occurrences of ' with ''
     * @param str the input string
     * @return a SQL-safe string
     */
    protected String escapeString(String str, String quote) {
        return StringUtil.replaceAll(str, quote, quote + quote);
    }
    
    public String toString() {
        return buffer.toString();
    }
    
    public void visit(AggregateFunction obj) {
        buffer.append(obj.getName())
              .append(Tokens.LPAREN);
        
        if ( obj.isDistinct()) {
            buffer.append(SQLReservedWords.DISTINCT)
                  .append(Tokens.SPACE);
        }
        
        if (obj.getExpression() == null) {
             buffer.append(Tokens.ALL_COLS);
        } else {
            append(obj.getExpression());
        }
        buffer.append(Tokens.RPAREN);
    }

    public void visit(Comparison obj) {
        append(obj.getLeftExpression());
        buffer.append(Tokens.SPACE);
        buffer.append(obj.getOperator());
        buffer.append(Tokens.SPACE);
        append(obj.getRightExpression());
    }

    public void visit(AndOr obj) {
        String opString = obj.getOperator().toString();

        appendNestedCondition(obj, obj.getLeftCondition());
	    buffer.append(Tokens.SPACE)
	          .append(opString)
	          .append(Tokens.SPACE);
        appendNestedCondition(obj, obj.getRightCondition());
    }
    
    protected void appendNestedCondition(AndOr parent, Condition condition) {
    	if (condition instanceof AndOr) {
    		AndOr nested = (AndOr)condition;
    		if (nested.getOperator() != parent.getOperator()) {
    			buffer.append(Tokens.LPAREN);
    			append(condition);
    			buffer.append(Tokens.RPAREN);
    			return;
    		}
    	}
    	append(condition);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.language.Delete)
     */
    public void visit(Delete obj) {
        buffer.append(SQLReservedWords.DELETE)
              .append(Tokens.SPACE);
        buffer.append(getSourceComment(obj));
        buffer.append(SQLReservedWords.FROM)
              .append(Tokens.SPACE);
        append(obj.getTable());
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.WHERE)
                  .append(Tokens.SPACE);
            append(obj.getWhere());
        }
    }

    /**
     * Take the specified derived group and element short names and determine a 
     * replacement element name to use instead.  Most commonly, this is used to strip
     * the group name if the group is a pseudo-group (DUAL) or the element is a pseudo-group
     * (ROWNUM).  It may also be used to strip special information out of the name in source 
     * value in some specialized cases.  
     * 
     * By default, this method returns null, indicating that the normal group and element 
     * name logic should be used (group + "." + element).  Subclasses should override and
     * implement this method if desired.
     * 
     * @param group Group name, may be null
     * @param element Element name, never null
     * @return Replacement element name to be used as is (no modification will occur)
     * @since 5.0
     */
    protected String replaceElementName(String group, String element) {        
        return null;
    }
    
    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.language.ColumnReference)
     */
    public void visit(ColumnReference obj) {
        buffer.append(getElementName(obj, true));
    }

	private String getElementName(ColumnReference obj, boolean qualify) {
		String groupName = null;
        NamedTable group = obj.getTable();
        if (group != null && qualify) {
            if(group.getCorrelationName() != null) { 
                groupName = group.getCorrelationName();
            } else {  
                AbstractMetadataRecord groupID = group.getMetadataObject();
                if(groupID != null) {              
                    groupName = getName(groupID);
                } else {
                    groupName = group.getName();
                }
            }
        }
        
		String elemShortName = null;        
		AbstractMetadataRecord elementID = obj.getMetadataObject();
        if(elementID != null) {
            elemShortName = getName(elementID);            
        } else {
            String elementName = obj.getName();
            elemShortName = getShortName(elementName);
        }

        // Check whether a subclass wants to replace the element name to use in special circumstances
        String replacementElement = replaceElementName(groupName, elemShortName);
        if(replacementElement != null) {
            // If so, use it as is
            return replacementElement;
        } 
        StringBuffer elementName = new StringBuffer(elemShortName.length());
        // If not, do normal logic:  [group + "."] + element
        if(groupName != null) {
        	elementName.append(groupName);
        	elementName.append(Tokens.DOT);
        }
        elementName.append(elemShortName);
        return elementName.toString();
    }

    /** 
     * @param elementName
     * @return
     * @since 4.3
     */
    public static String getShortName(String elementName) {
        int lastDot = elementName.lastIndexOf("."); //$NON-NLS-1$
        if(lastDot >= 0) {
            elementName = elementName.substring(lastDot+1);                
        } 
        return elementName;
    }
    
    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IExecute)
     */
    public void visit(Call obj) {              
        buffer.append(SQLReservedWords.EXEC)
              .append(Tokens.SPACE);
        
        if(obj.getMetadataObject() != null) {
            buffer.append(getName(obj.getMetadataObject()));                         
        } else {
            buffer.append(obj.getProcedureName());
        }
              
        buffer.append(Tokens.LPAREN);
        final List<Argument> params = obj.getArguments();
        if (params != null && params.size() != 0) {
            Argument param = null;
            for (int i = 0; i < params.size(); i++) {
                param = params.get(i);
                if (param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT) {
                    if (i != 0) {
                        buffer.append(Tokens.COMMA)
                              .append(Tokens.SPACE);
                    }
                    append(param);
                }
            }
        }
        buffer.append(Tokens.RPAREN);
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IExistsCriteria)
     */
    public void visit(Exists obj) {
        buffer.append(SQLReservedWords.EXISTS)
              .append(Tokens.SPACE)
              .append(Tokens.LPAREN);
        append(obj.getSubquery());
        buffer.append(Tokens.RPAREN);
    }
    
    protected boolean isInfixFunction(String function) {
    	return infixFunctions.contains(function);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.language.Function)
     */
    public void visit(Function obj) {

        String name = obj.getName();
        List<Expression> args = obj.getParameters();
        if(name.equalsIgnoreCase(SQLReservedWords.CONVERT) || name.equalsIgnoreCase(SQLReservedWords.CAST)) { 
            
            Object typeValue = ((Literal)args.get(1)).getValue();
               
            buffer.append(name);
            buffer.append(Tokens.LPAREN); 
            
            append(args.get(0));

            if(name.equalsIgnoreCase(SQLReservedWords.CONVERT)) { 
                buffer.append(Tokens.COMMA); 
                buffer.append(Tokens.SPACE); 
            } else {
                buffer.append(Tokens.SPACE); 
                buffer.append(SQLReservedWords.AS); 
                buffer.append(Tokens.SPACE); 
            }
            buffer.append(typeValue);
            buffer.append(Tokens.RPAREN); 
        } else if(isInfixFunction(name)) { 
            buffer.append(Tokens.LPAREN); 

            if(args != null) {
                for(int i=0; i<args.size(); i++) {
                    append(args.get(i));
                    if(i < (args.size()-1)) {
                        buffer.append(Tokens.SPACE);
                        buffer.append(name);
                        buffer.append(Tokens.SPACE);
                    }
                }
            }
            buffer.append(Tokens.RPAREN);

        } else if(name.equalsIgnoreCase(NonReserved.TIMESTAMPADD) || name.equalsIgnoreCase(NonReserved.TIMESTAMPDIFF)) {
            buffer.append(name);
            buffer.append(Tokens.LPAREN); 

            if(args != null && args.size() > 0) {
                buffer.append(((Literal)args.get(0)).getValue());

                for(int i=1; i<args.size(); i++) {
                	buffer.append(Tokens.COMMA); 
                    buffer.append(Tokens.SPACE);
                	append(args.get(i));
                }
            }
            buffer.append(Tokens.RPAREN);

        } else {

            buffer.append(obj.getName())
                  .append(Tokens.LPAREN);
            append(obj.getParameters());
            buffer.append(Tokens.RPAREN);
        }
    }

    public void visit(NamedTable obj) {
    	AbstractMetadataRecord groupID = obj.getMetadataObject();
        if(groupID != null) {              
            buffer.append(getName(groupID));
        } else {
            buffer.append(obj.getName());
        }        
        
        if (obj.getCorrelationName() != null) {
            buffer.append(Tokens.SPACE);
            if (useAsInGroupAlias()){
                buffer.append(SQLReservedWords.AS)
                      .append(Tokens.SPACE);
            }
        	buffer.append(obj.getCorrelationName());
        }
    }
    
    /**
     * Indicates whether group alias should be of the form
     * "...FROM groupA AS X" or "...FROM groupA X".  Certain
     * data sources (such as Oracle) may not support the first
     * form. 
     * @return boolean
     */
    protected boolean useAsInGroupAlias(){
        return true;
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.language.GroupBy)
     */
    public void visit(GroupBy obj) {
        buffer.append(SQLReservedWords.GROUP)
              .append(Tokens.SPACE)
              .append(SQLReservedWords.BY)
              .append(Tokens.SPACE);
        append(obj.getElements());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.language.In)
     */
    public void visit(In obj) {
        append(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.NOT);
        }
        buffer.append(Tokens.SPACE)
              .append(SQLReservedWords.IN)
              .append(Tokens.SPACE)
              .append(Tokens.LPAREN);
        append(obj.getRightExpressions());
        buffer.append(Tokens.RPAREN);
    }

    public void visit(DerivedTable obj) {
        buffer.append(Tokens.LPAREN);
    	append(obj.getQuery());
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        if(useAsInGroupAlias()) {
            buffer.append(SQLReservedWords.AS);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(obj.getCorrelationName());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.language.Insert)
     */
    public void visit(Insert obj) {
    	buffer.append(SQLReservedWords.INSERT).append(Tokens.SPACE);
		buffer.append(getSourceComment(obj));
		buffer.append(SQLReservedWords.INTO).append(Tokens.SPACE);
		append(obj.getTable());
		buffer.append(Tokens.SPACE).append(Tokens.LPAREN);

		int elementCount = obj.getColumns().size();
		for (int i = 0; i < elementCount; i++) {
			buffer.append(getElementName(obj.getColumns().get(i), false));
			if (i < elementCount - 1) {
				buffer.append(Tokens.COMMA);
				buffer.append(Tokens.SPACE);
			}
		}

		buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        append(obj.getValueSource());
    }
    
    @Override
	public void visit(ExpressionValueSource obj) {
		buffer.append(SQLReservedWords.VALUES).append(Tokens.SPACE).append(Tokens.LPAREN);
		append(obj.getValues());
		buffer.append(Tokens.RPAREN);
	}
        
    public void visit(IsNull obj) {
        append(obj.getExpression());
        buffer.append(Tokens.SPACE)
              .append(SQLReservedWords.IS)
              .append(Tokens.SPACE);
        if (obj.isNegated()) {
            buffer.append(SQLReservedWords.NOT)
                  .append(Tokens.SPACE);
        }
        buffer.append(SQLReservedWords.NULL);
    }

    public void visit(Join obj) {
        TableReference leftItem = obj.getLeftItem();
        if(useParensForJoins() && leftItem instanceof Join) {
            buffer.append(Tokens.LPAREN);
            append(leftItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(leftItem);
        }
        buffer.append(Tokens.SPACE);
        
        switch(obj.getJoinType()) {
            case CROSS_JOIN:
                buffer.append(SQLReservedWords.CROSS);
                break;
            case FULL_OUTER_JOIN:
                buffer.append(SQLReservedWords.FULL)
                      .append(Tokens.SPACE)
                      .append(SQLReservedWords.OUTER);
                break;
            case INNER_JOIN:
                buffer.append(SQLReservedWords.INNER);
                break;
            case LEFT_OUTER_JOIN:
                buffer.append(SQLReservedWords.LEFT)
                      .append(Tokens.SPACE)
                      .append(SQLReservedWords.OUTER);
                break;
            case RIGHT_OUTER_JOIN:
                buffer.append(SQLReservedWords.RIGHT)
                      .append(Tokens.SPACE)
                      .append(SQLReservedWords.OUTER);
                break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(Tokens.SPACE)
              .append(SQLReservedWords.JOIN)
              .append(Tokens.SPACE);
        
        TableReference rightItem = obj.getRightItem();
        if(rightItem instanceof Join && (useParensForJoins() || obj.getJoinType() == Join.JoinType.CROSS_JOIN)) {
            buffer.append(Tokens.LPAREN);
            append(rightItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(rightItem);
        }
        
        final Condition condition = obj.getCondition();
        if (condition != null) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.ON)
                  .append(Tokens.SPACE);
            append(condition);                    
        }        
    }

    public void visit(Like obj) {
        append(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.NOT);
        }
        buffer.append(Tokens.SPACE)
              .append(SQLReservedWords.LIKE)
              .append(Tokens.SPACE);
        append(obj.getRightExpression());
        if (obj.getEscapeCharacter() != null) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.ESCAPE)
                  .append(Tokens.SPACE)
                  .append(Tokens.QUOTE)
                  .append(obj.getEscapeCharacter().toString())
                  .append(Tokens.QUOTE);
        }
        
    }
    
    public void visit(Limit obj) {
        buffer.append(SQLReservedWords.LIMIT)
              .append(Tokens.SPACE);
        if (obj.getRowOffset() > 0) {
            buffer.append(obj.getRowOffset())
                  .append(Tokens.COMMA)
                  .append(Tokens.SPACE);
        }
        buffer.append(obj.getRowLimit());
    }

    public void visit(Literal obj) {
    	if (obj.isBindValue()) {
    		buffer.append("?"); //$NON-NLS-1$
    	} else if (obj.getValue() == null) {
            buffer.append(SQLReservedWords.NULL);
        } else {
            Class<?> type = obj.getType();
            String val = obj.getValue().toString();
            if(Number.class.isAssignableFrom(type)) {
                buffer.append(val);
            } else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
                buffer.append("{b '") //$NON-NLS-1$
                      .append(val)
                      .append("'}"); //$NON-NLS-1$
            } else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
                buffer.append("{ts '") //$NON-NLS-1$
                      .append(val)
                      .append("'}"); //$NON-NLS-1$
            } else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
                buffer.append("{t '") //$NON-NLS-1$
                      .append(val)
                      .append("'}"); //$NON-NLS-1$
            } else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
                buffer.append("{d '") //$NON-NLS-1$
                      .append(val)
                      .append("'}"); //$NON-NLS-1$
            } else {
                buffer.append(Tokens.QUOTE)
                      .append(escapeString(val, Tokens.QUOTE))
                      .append(Tokens.QUOTE);
            }
        }
    }

    public void visit(Not obj) {
        buffer.append(SQLReservedWords.NOT)
              .append(Tokens.SPACE)
              .append(Tokens.LPAREN);
        append(obj.getCriteria());
        buffer.append(Tokens.RPAREN);
    }

    public void visit(OrderBy obj) {
        buffer.append(SQLReservedWords.ORDER)
              .append(Tokens.SPACE)
              .append(SQLReservedWords.BY)
              .append(Tokens.SPACE);
        append(obj.getSortSpecifications());
    }

    public void visit(SortSpecification obj) {
    	append(obj.getExpression());            
        if (obj.getOrdering() == Ordering.DESC) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.DESC);
        } // Don't print default "ASC"
    }

    public void visit(Argument obj) {
        buffer.append(obj.getArgumentValue());
    }

    public void visit(Select obj) {
		buffer.append(SQLReservedWords.SELECT).append(Tokens.SPACE);
        buffer.append(getSourceComment(obj));
        if (obj.isDistinct()) {
            buffer.append(SQLReservedWords.DISTINCT).append(Tokens.SPACE);
        }
        if (useSelectLimit() && obj.getLimit() != null) {
            append(obj.getLimit());
            buffer.append(Tokens.SPACE);
        }
        append(obj.getDerivedColumns());
        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
        	buffer.append(Tokens.SPACE).append(SQLReservedWords.FROM).append(Tokens.SPACE);      
            append(obj.getFrom());
        }
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.WHERE)
                  .append(Tokens.SPACE);
            append(obj.getWhere());
        }
        if (obj.getGroupBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getGroupBy());
        }
        if (obj.getHaving() != null) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.HAVING)
                  .append(Tokens.SPACE);
            append(obj.getHaving());
        }
        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }
        if (!useSelectLimit() && obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getLimit());
        }
    }

    public void visit(SearchedCase obj) {
        buffer.append(SQLReservedWords.CASE);
        for (SearchedWhenClause swc : obj.getCases()) {
			append(swc);
		}
        if (obj.getElseExpression() != null) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.ELSE)
                  .append(Tokens.SPACE);
            append(obj.getElseExpression());
        }
        buffer.append(Tokens.SPACE)
              .append(SQLReservedWords.END);
    }
    
    @Override
    public void visit(SearchedWhenClause obj) {
		buffer.append(Tokens.SPACE).append(SQLReservedWords.WHEN)
				.append(Tokens.SPACE);
		append(obj.getCondition());
		buffer.append(Tokens.SPACE).append(SQLReservedWords.THEN)
				.append(Tokens.SPACE);
		append(obj.getResult());
    }

    protected String getSourceComment(Command command) {
        return ""; //$NON-NLS-1$
    }
    
    public void visit(ScalarSubquery obj) {
        buffer.append(Tokens.LPAREN);   
        append(obj.getSubquery());     
        buffer.append(Tokens.RPAREN);        
    }

    public void visit(DerivedColumn obj) {
        append(obj.getExpression());
        if (obj.getAlias() != null) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.AS)
                  .append(Tokens.SPACE)
                  .append(obj.getAlias());
        }
    }

    public void visit(SubqueryComparison obj) {
        append(obj.getLeftExpression());
        buffer.append(Tokens.SPACE);
        
        switch(obj.getOperator()) {
            case EQ: buffer.append(Tokens.EQ); break;
            case GE: buffer.append(Tokens.GE); break;
            case GT: buffer.append(Tokens.GT); break;
            case LE: buffer.append(Tokens.LE); break;
            case LT: buffer.append(Tokens.LT); break;
            case NE: buffer.append(Tokens.NE); break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(Tokens.SPACE);
        switch(obj.getQuantifier()) {
            case ALL: buffer.append(SQLReservedWords.ALL); break;
            case SOME: buffer.append(SQLReservedWords.SOME); break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(Tokens.SPACE);
        buffer.append(Tokens.LPAREN);        
        append(obj.getSubquery());
        buffer.append(Tokens.RPAREN);        
    }

    public void visit(SubqueryIn obj) {
        append(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.NOT);
        }
        buffer.append(Tokens.SPACE)
              .append(SQLReservedWords.IN)
              .append(Tokens.SPACE)
              .append(Tokens.LPAREN);
        append(obj.getSubquery());
        buffer.append(Tokens.RPAREN);
    }

    public void visit(Update obj) {
        buffer.append(SQLReservedWords.UPDATE)
              .append(Tokens.SPACE);
        buffer.append(getSourceComment(obj));
        append(obj.getTable());
        buffer.append(Tokens.SPACE)
              .append(SQLReservedWords.SET)
              .append(Tokens.SPACE);
        append(obj.getChanges()); 
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE)
                  .append(SQLReservedWords.WHERE)
                  .append(Tokens.SPACE);
            append(obj.getWhere());
        }
    }
    
    public void visit(SetClause clause) {
        buffer.append(getElementName(clause.getSymbol(), false));
        buffer.append(Tokens.SPACE).append(Tokens.EQ).append(Tokens.SPACE);
        append(clause.getValue());
    }
    
    public void visit(SetQuery obj) {
        appendSetQuery(obj, obj.getLeftQuery(), false);
        
        buffer.append(Tokens.SPACE);
        
        appendSetOperation(obj.getOperation());

        if(obj.isAll()) {
            buffer.append(Tokens.SPACE);
            buffer.append(SQLReservedWords.ALL);                
        }
        buffer.append(Tokens.SPACE);

        appendSetQuery(obj, obj.getRightQuery(), true);
        
        OrderBy orderBy = obj.getOrderBy();
        if(orderBy != null) {
            buffer.append(Tokens.SPACE);
            append(orderBy);
        }

        Limit limit = obj.getLimit();
        if(limit != null) {
            buffer.append(Tokens.SPACE);
            append(limit);
        }
    }

    protected void appendSetOperation(SetQuery.Operation operation) {
        buffer.append(operation);
    }
    
    protected boolean useParensForSetQueries() {
    	return false;
    }

    protected void appendSetQuery(SetQuery parent, QueryExpression obj, boolean right) {
        if((!(obj instanceof SetQuery) && useParensForSetQueries()) 
        		|| (right && obj instanceof SetQuery 
        				&& ((parent.isAll() && !((SetQuery)obj).isAll()) 
        						|| parent.getOperation() != ((SetQuery)obj).getOperation()))) {
            buffer.append(Tokens.LPAREN);
            append(obj);
            buffer.append(Tokens.RPAREN);
        } else {
        	if (!parent.isAll() && obj instanceof SetQuery) {
        		((SetQuery)obj).setAll(false);
        	}
            append(obj);
        }
    }
 
    /**
     * Gets the SQL string representation for a given ILanguageObject.
     * @param obj the root of the ILanguageObject hierarchy that needs to be
     * converted. This can be any subtree, and does not need to be a top-level
     * command
     * @return the SQL representation of that ILanguageObject hierarchy
     */
    public static String getSQLString(LanguageObject obj) {
        SQLStringVisitor visitor = new SQLStringVisitor();
        visitor.append(obj);
        return visitor.toString();
    }
    
    protected boolean useParensForJoins() {
    	return false;
    }
    
    protected boolean useSelectLimit() {
    	return false;
    }
}
