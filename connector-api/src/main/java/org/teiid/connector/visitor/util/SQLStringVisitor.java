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

package org.teiid.connector.visitor.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICompoundCriteria;
import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IDelete;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExistsCriteria;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFrom;
import org.teiid.connector.language.IFromItem;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IGroupBy;
import org.teiid.connector.language.IInCriteria;
import org.teiid.connector.language.IInlineView;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.language.IIsNullCriteria;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.language.ILanguageObject;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.INotCriteria;
import org.teiid.connector.language.IOrderBy;
import org.teiid.connector.language.IOrderByItem;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IPredicateCriteria;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.language.IScalarSubquery;
import org.teiid.connector.language.ISearchedCaseExpression;
import org.teiid.connector.language.ISelect;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.ISetClause;
import org.teiid.connector.language.ISetClauseList;
import org.teiid.connector.language.ISetQuery;
import org.teiid.connector.language.ISubqueryCompareCriteria;
import org.teiid.connector.language.ISubqueryInCriteria;
import org.teiid.connector.language.IUpdate;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.metadata.runtime.MetadataObject;
import org.teiid.connector.visitor.framework.AbstractLanguageVisitor;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.StringUtil;

/**
 * Creates a SQL string for a LanguageObject subtree. Instances of this class
 * are not reusable, and are not thread-safe.
 */
public class SQLStringVisitor extends AbstractLanguageVisitor implements SQLReservedWords {
   
    private Set<String> infixFunctions = new HashSet<String>(Arrays.asList("%", "+", "-", "*", "+", "/", "||", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ 
    		"&", "|", "^", "#"));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
	
    private static final String ESCAPED_QUOTE = "''"; //$NON-NLS-1$    

    protected static final String UNDEFINED = "<undefined>"; //$NON-NLS-1$
    protected static final String UNDEFINED_PARAM = "?"; //$NON-NLS-1$
    
    protected StringBuffer buffer = new StringBuffer();
                
    /**
     * Gets the name of a group or element from the RuntimeMetadata
     * @param id the id of the group or element
     * @return the name of that element or group as defined in the source
     */
    protected String getName(MetadataObject object) {
        try {
            String nameInSource = object.getNameInSource();
            if(nameInSource != null && nameInSource.length() > 0) {
                return nameInSource;
            }
            return object.getName();
        } catch(ConnectorException e) {
            return object.getName();
        }
    }
    
    /**
     * Appends the string form of the ILanguageObject to the current buffer.
     * @param obj the language object instance
     */
    public void append(ILanguageObject obj) {
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
    protected void append(List<? extends ILanguageObject> items) {
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                buffer.append(COMMA)
                      .append(SPACE);
                append(items.get(i));
            }
        }
    }
    
    /**
     * Simple utility to append an array of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items an array of ILanguageObjects
     */
    protected void append(ILanguageObject[] items) {
        if (items != null && items.length != 0) {
            append(items[0]);
            for (int i = 1; i < items.length; i++) {
                buffer.append(COMMA)
                      .append(SPACE);
                append(items[i]);
            }
        }
    }
    
    /**
     * Creates a SQL-safe string. Simply replaces all occurrences of ' with ''
     * @param str the input string
     * @return a SQL-safe string
     */
    protected String escapeString(String str) {
        return StringUtil.replaceAll(str, QUOTE, ESCAPED_QUOTE);
    }
    
    public String toString() {
        return buffer.toString();
    }
    
    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IAggregate)
     */
    public void visit(IAggregate obj) {
        buffer.append(obj.getName())
              .append(LPAREN);
        
        if ( obj.isDistinct()) {
            buffer.append(DISTINCT)
                  .append(SPACE);
        }
        
        if (obj.getExpression() == null) {
             buffer.append(ALL_COLS);
        } else {
            append(obj.getExpression());
        }
        buffer.append(RPAREN);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.ICompareCriteria)
     */
    public void visit(ICompareCriteria obj) {
        append(obj.getLeftExpression());
        buffer.append(SPACE);
        
        switch(obj.getOperator()) {
            case EQ: buffer.append(EQ); break;
            case GE: buffer.append(GE); break;
            case GT: buffer.append(GT); break;
            case LE: buffer.append(LE); break;
            case LT: buffer.append(LT); break;
            case NE: buffer.append(NE); break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(SPACE);
        append(obj.getRightExpression());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.ICompoundCriteria)
     */
    public void visit(ICompoundCriteria obj) {
        String opString = null;
        switch(obj.getOperator()) {
            case AND: opString = AND; break;
            case OR:  opString = OR;  break;
            default: opString = UNDEFINED;
        }
        
        List criteria = obj.getCriteria();
        if (criteria == null || criteria.size() == 0) {
            buffer.append(UNDEFINED);
        } else if(criteria.size() == 1) {
            // Special case - should really never happen, but we are tolerant
            append((ILanguageObject)criteria.get(0));
        } else {
            buffer.append(LPAREN);
            append((ILanguageObject)criteria.get(0));
            buffer.append(RPAREN);
            for (int i = 1; i < criteria.size(); i++) {
                buffer.append(SPACE)
                      .append(opString)
                      .append(SPACE)
                      .append(LPAREN);
                append((ILanguageObject)criteria.get(i));
                buffer.append(RPAREN);
            }
            
        }
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IDelete)
     */
    public void visit(IDelete obj) {
        buffer.append(DELETE)
              .append(SPACE);
        buffer.append(getSourceComment(obj));
        buffer.append(FROM)
              .append(SPACE);
        append(obj.getGroup());
        if (obj.getCriteria() != null) {
            buffer.append(SPACE)
                  .append(WHERE)
                  .append(SPACE);
            append(obj.getCriteria());
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IElement)
     */
    public void visit(IElement obj) {
        buffer.append(getElementName(obj, true));
    }

	private String getElementName(IElement obj, boolean qualify) {
		String groupName = null;
        IGroup group = obj.getGroup();
        if (group != null && qualify) {
            if(group.getDefinition() != null) { 
                groupName = group.getContext();
            } else {  
                MetadataObject groupID = group.getMetadataObject();
                if(groupID != null) {              
                    groupName = getName(groupID);
                } else {
                    groupName = group.getContext();
                }
            }
        }
        
		String elemShortName = null;        
        MetadataObject elementID = obj.getMetadataObject();
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
        	elementName.append(DOT);
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
    public void visit(IProcedure obj) {              
        buffer.append(EXEC)
              .append(SPACE);
        
        if(obj.getMetadataObject() != null) {
            buffer.append(getName(obj.getMetadataObject()));                         
        } else {
            buffer.append(obj.getProcedureName());
        }
              
        buffer.append(LPAREN);
        final List params = obj.getParameters();
        if (params != null && params.size() != 0) {
            IParameter param = null;
            for (int i = 0; i < params.size(); i++) {
                param = (IParameter)params.get(i);
                if (param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT) {
                    if (i != 0) {
                        buffer.append(COMMA)
                              .append(SPACE);
                    }
                    if (param.getValue() != null) {
                        buffer.append(param.getValue().toString());
                    } else {
                        buffer.append(UNDEFINED_PARAM);
                    }
                }
            }
        }
        buffer.append(RPAREN);
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IExistsCriteria)
     */
    public void visit(IExistsCriteria obj) {
        buffer.append(EXISTS)
              .append(SPACE)
              .append(LPAREN);
        append(obj.getQuery());
        buffer.append(RPAREN);
    }
    
    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IFrom)
     */
    public void visit(IFrom obj) {
        buffer.append(FROM)
              .append(SPACE);
        append(obj.getItems());
    }
        
    protected boolean isInfixFunction(String function) {
    	return infixFunctions.contains(function);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IFunction)
     */
    public void visit(IFunction obj) {

        String name = obj.getName();
        List<IExpression> args = obj.getParameters();
        if(name.equalsIgnoreCase(CONVERT) || name.equalsIgnoreCase(CAST)) { 
            
            Object typeValue = ((ILiteral)args.get(1)).getValue();
               
            buffer.append(name);
            buffer.append(LPAREN); 
            
            append(args.get(0));

            if(name.equalsIgnoreCase(CONVERT)) { 
                buffer.append(COMMA); 
                buffer.append(SPACE); 
            } else {
                buffer.append(SPACE); 
                buffer.append(AS); 
                buffer.append(SPACE); 
            }
            buffer.append(typeValue);
            buffer.append(RPAREN); 
        } else if(isInfixFunction(name)) { 
            buffer.append(LPAREN); 

            if(args != null) {
                for(int i=0; i<args.size(); i++) {
                    append(args.get(i));
                    if(i < (args.size()-1)) {
                        buffer.append(SPACE);
                        buffer.append(name);
                        buffer.append(SPACE);
                    }
                }
            }
            buffer.append(RPAREN);

        } else if(name.equalsIgnoreCase(TIMESTAMPADD) || name.equalsIgnoreCase(TIMESTAMPDIFF)) {
            buffer.append(name);
            buffer.append(LPAREN); 

            if(args != null && args.size() > 0) {
                buffer.append(((ILiteral)args.get(0)).getValue());

                for(int i=1; i<args.size(); i++) {
                	buffer.append(COMMA); 
                    buffer.append(SPACE);
                	append(args.get(i));
                }
            }
            buffer.append(RPAREN);

        } else {

            buffer.append(obj.getName())
                  .append(LPAREN);
            append(obj.getParameters());
            buffer.append(RPAREN);
        }
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IGroup)
     */
    public void visit(IGroup obj) {
        MetadataObject groupID = obj.getMetadataObject();
        if(groupID != null) {              
            buffer.append(getName(groupID));
        } else {
            if(obj.getDefinition() == null) {
                buffer.append(obj.getContext());                
            } else {
                buffer.append(obj.getDefinition());
            }
        }        
        
        if (obj.getDefinition() != null) {
            buffer.append(SPACE);
            if (useAsInGroupAlias()){
                buffer.append(AS)
                      .append(SPACE);
            }
        	buffer.append(obj.getContext());
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IGroupBy)
     */
    public void visit(IGroupBy obj) {
        buffer.append(GROUP)
              .append(SPACE)
              .append(BY)
              .append(SPACE);
        append(obj.getElements());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IInCriteria)
     */
    public void visit(IInCriteria obj) {
        append(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(SPACE)
                  .append(NOT);
        }
        buffer.append(SPACE)
              .append(IN)
              .append(SPACE)
              .append(LPAREN);
        append(obj.getRightExpressions());
        buffer.append(RPAREN);
    }

    public void visit(IInlineView obj) {
        buffer.append(LPAREN);
        if (obj.getOutput() != null) {
        	buffer.append(obj.getOutput());
        } else {
        	append(obj.getQuery());
        }
        buffer.append(RPAREN);
        buffer.append(SPACE);
        if(useAsInGroupAlias()) {
            buffer.append(AS);
            buffer.append(SPACE);
        }
        buffer.append(obj.getContext());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IInsert)
     */
    public void visit(IInsert obj) {
    	buffer.append(INSERT).append(SPACE);
		buffer.append(getSourceComment(obj));
		buffer.append(INTO).append(SPACE);
		append(obj.getGroup());
		if (obj.getElements() != null && obj.getElements().size() != 0) {
			buffer.append(SPACE).append(LPAREN);

			int elementCount = obj.getElements().size();
			for (int i = 0; i < elementCount; i++) {
				buffer.append(getElementName(obj.getElements().get(i), false));
				if (i < elementCount - 1) {
					buffer.append(COMMA);
					buffer.append(SPACE);
				}
			}

			buffer.append(RPAREN);
		}
        buffer.append(SPACE);
        append(obj.getValueSource());
    }
    
    @Override
	public void visit(IInsertExpressionValueSource obj) {
		buffer.append(VALUES).append(SPACE).append(LPAREN);
		append(obj.getValues());
		buffer.append(RPAREN);
	}
        
    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IIsNullCriteria)
     */
    public void visit(IIsNullCriteria obj) {
        append(obj.getExpression());
        buffer.append(SPACE)
              .append(IS)
              .append(SPACE);
        if (obj.isNegated()) {
            buffer.append(NOT)
                  .append(SPACE);
        }
        buffer.append(NULL);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IJoin)
     */
    public void visit(IJoin obj) {
        IFromItem leftItem = obj.getLeftItem();
        if(useParensForJoins() && leftItem instanceof IJoin) {
            buffer.append(LPAREN);
            append(leftItem);
            buffer.append(RPAREN);
        } else {
            append(leftItem);
        }
        buffer.append(SPACE);
        
        switch(obj.getJoinType()) {
            case CROSS_JOIN:
                buffer.append(CROSS);
                break;
            case FULL_OUTER_JOIN:
                buffer.append(FULL)
                      .append(SPACE)
                      .append(OUTER);
                break;
            case INNER_JOIN:
                buffer.append(INNER);
                break;
            case LEFT_OUTER_JOIN:
                buffer.append(LEFT)
                      .append(SPACE)
                      .append(OUTER);
                break;
            case RIGHT_OUTER_JOIN:
                buffer.append(RIGHT)
                      .append(SPACE)
                      .append(OUTER);
                break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(SPACE)
              .append(JOIN)
              .append(SPACE);
        
        IFromItem rightItem = obj.getRightItem();
        if(rightItem instanceof IJoin && (useParensForJoins() || obj.getJoinType() == IJoin.JoinType.CROSS_JOIN)) {
            buffer.append(LPAREN);
            append(rightItem);
            buffer.append(RPAREN);
        } else {
            append(rightItem);
        }
        
        final List criteria = obj.getCriteria();
        if (criteria != null && criteria.size() != 0) {
            buffer.append(SPACE)
                  .append(ON)
                  .append(SPACE);

            Iterator critIter = criteria.iterator();
            while(critIter.hasNext()) {
                ICriteria crit = (ICriteria) critIter.next();
                if(crit instanceof IPredicateCriteria) {
                    append(crit);                    
                } else {
                    buffer.append(LPAREN);
                    append(crit);                    
                    buffer.append(RPAREN);
                }
                
                if(critIter.hasNext()) {
                    buffer.append(SPACE)
                          .append(AND)
                          .append(SPACE);
                }
            }
        }        
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.ILikeCriteria)
     */
    public void visit(ILikeCriteria obj) {
        append(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(SPACE)
                  .append(NOT);
        }
        buffer.append(SPACE)
              .append(LIKE)
              .append(SPACE);
        append(obj.getRightExpression());
        if (obj.getEscapeCharacter() != null) {
            buffer.append(SPACE)
                  .append(ESCAPE)
                  .append(SPACE)
                  .append(QUOTE)
                  .append(obj.getEscapeCharacter().toString())
                  .append(QUOTE);
        }
        
    }
    
    public void visit(ILimit obj) {
        buffer.append(LIMIT)
              .append(SPACE);
        if (obj.getRowOffset() > 0) {
            buffer.append(obj.getRowOffset())
                  .append(COMMA)
                  .append(SPACE);
        }
        buffer.append(obj.getRowLimit());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.ILiteral)
     */
    public void visit(ILiteral obj) {
    	if (obj.isBindValue()) {
    		buffer.append("?"); //$NON-NLS-1$
    	} else if (obj.getValue() == null) {
            buffer.append(NULL);
        } else {
            Class type = obj.getType();
            String val = obj.getValue().toString();
            if(Number.class.isAssignableFrom(type)) {
                buffer.append(val);
            } else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
                buffer.append("{b'") //$NON-NLS-1$
                      .append(val)
                      .append("'}"); //$NON-NLS-1$
            } else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
                buffer.append("{ts'") //$NON-NLS-1$
                      .append(val)
                      .append("'}"); //$NON-NLS-1$
            } else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
                buffer.append("{t'") //$NON-NLS-1$
                      .append(val)
                      .append("'}"); //$NON-NLS-1$
            } else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
                buffer.append("{d'") //$NON-NLS-1$
                      .append(val)
                      .append("'}"); //$NON-NLS-1$
            } else {
                buffer.append(QUOTE)
                      .append(escapeString(val))
                      .append(QUOTE);
            }
        }
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.INotCriteria)
     */
    public void visit(INotCriteria obj) {
        buffer.append(NOT)
              .append(SPACE)
              .append(LPAREN);
        append(obj.getCriteria());
        buffer.append(RPAREN);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IOrderBy)
     */
    public void visit(IOrderBy obj) {
        buffer.append(ORDER)
              .append(SPACE)
              .append(BY)
              .append(SPACE);
        append(obj.getItems());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IOrderByItem)
     */
    public void visit(IOrderByItem obj) {
        if(obj.getName() != null) {
            String name = getShortName(obj.getName());
            buffer.append(name);
        } else if (obj.getElement() != null) {
            append(obj.getElement());            
        } else {
            buffer.append(UNDEFINED);
        }
        if (obj.getDirection() == IOrderByItem.DESC) {
            buffer.append(SPACE)
                  .append(DESC);
        } // Don't print default "ASC"
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IParameter)
     */
    public void visit(IParameter obj) {
        if (obj.getValue() == null) {
            buffer.append(UNDEFINED_PARAM);
        } else if (obj.getValue() == null) {
            buffer.append(NULL);
        } else {
            buffer.append(obj.getValue().toString());
        }
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IQuery)
     */
    public void visit(IQuery obj) {
        visitSelect(obj.getSelect(), obj);
        if (obj.getFrom() != null) {
            buffer.append(SPACE);
            append(obj.getFrom());
        }
        if (obj.getWhere() != null) {
            buffer.append(SPACE)
                  .append(WHERE)
                  .append(SPACE);
            append(obj.getWhere());
        }
        if (obj.getGroupBy() != null) {
            buffer.append(SPACE);
            append(obj.getGroupBy());
        }
        if (obj.getHaving() != null) {
            buffer.append(SPACE)
                  .append(HAVING)
                  .append(SPACE);
            append(obj.getHaving());
        }
        if (obj.getOrderBy() != null) {
            buffer.append(SPACE);
            append(obj.getOrderBy());
        }
        if (obj.getLimit() != null) {
            buffer.append(SPACE);
            append(obj.getLimit());
        }
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.ISearchedCaseExpression)
     */
    public void visit(ISearchedCaseExpression obj) {
        buffer.append(CASE);
        final int whenCount = obj.getWhenCount();
        for (int i = 0; i < whenCount; i++) {
            buffer.append(SPACE)
                  .append(WHEN)
                  .append(SPACE);
            append(obj.getWhenCriteria(i));
            buffer.append(SPACE)
                  .append(THEN)
                  .append(SPACE);
            append(obj.getThenExpression(i));
        }
        if (obj.getElseExpression() != null) {
            buffer.append(SPACE)
                  .append(ELSE)
                  .append(SPACE);
            append(obj.getElseExpression());
        }
        buffer.append(SPACE)
              .append(END);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.ISelect)
     */
    public void visit(ISelect obj) {
    	visitSelect(obj, null);
    }

	private void visitSelect(ISelect obj, ICommand command) {
		buffer.append(SELECT).append(SPACE);
        buffer.append(getSourceComment(command));
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(SPACE);
        }
        append(obj.getSelectSymbols());
	}

    protected String getSourceComment(ICommand command) {
        return ""; //$NON-NLS-1$
    }
    
    /*
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IScalarSubquery)
     */
    public void visit(IScalarSubquery obj) {
        buffer.append(LPAREN);   
        append(obj.getQuery());     
        buffer.append(RPAREN);        
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.ISelectSymbol)
     */
    public void visit(ISelectSymbol obj) {
        append(obj.getExpression());
        if (obj.hasAlias()) {
            buffer.append(SPACE)
                  .append(AS)
                  .append(SPACE)
                  .append(obj.getOutputName());
        }
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISubqueryCompareCriteria)
     */
    public void visit(ISubqueryCompareCriteria obj) {
        append(obj.getLeftExpression());
        buffer.append(SPACE);
        
        switch(obj.getOperator()) {
            case EQ: buffer.append(EQ); break;
            case GE: buffer.append(GE); break;
            case GT: buffer.append(GT); break;
            case LE: buffer.append(LE); break;
            case LT: buffer.append(LT); break;
            case NE: buffer.append(NE); break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(SPACE);
        switch(obj.getQuantifier()) {
            case ALL: buffer.append(ALL); break;
            case SOME: buffer.append(SOME); break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(SPACE);
        buffer.append(LPAREN);        
        append(obj.getQuery());
        buffer.append(RPAREN);        
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISubqueryInCriteria)
     */
    public void visit(ISubqueryInCriteria obj) {
        append(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(SPACE)
                  .append(NOT);
        }
        buffer.append(SPACE)
              .append(IN)
              .append(SPACE)
              .append(LPAREN);
        append(obj.getQuery());
        buffer.append(RPAREN);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IUpdate)
     */
    public void visit(IUpdate obj) {
        buffer.append(UPDATE)
              .append(SPACE);
        buffer.append(getSourceComment(obj));
        append(obj.getGroup());
        buffer.append(SPACE)
              .append(SET)
              .append(SPACE);
        append(obj.getChanges()); 
        if (obj.getCriteria() != null) {
            buffer.append(SPACE)
                  .append(WHERE)
                  .append(SPACE);
            append(obj.getCriteria());
        }
    }
    
    public void visit(ISetClauseList obj) {
    	append(obj.getClauses());
    }
    
    public void visit(ISetClause clause) {
        buffer.append(getElementName(clause.getSymbol(), false));
        buffer.append(SPACE).append(EQ).append(SPACE);
        append(clause.getValue());
    }
    
    public void visit(ISetQuery obj) {
        appendSetQuery(obj.getLeftQuery());
        
        buffer.append(SPACE);
        
        appendSetOperation(obj.getOperation());

        if(obj.isAll()) {
            buffer.append(SPACE);
            buffer.append(ALL);                
        }
        buffer.append(SPACE);

        appendSetQuery(obj.getRightQuery());
        
        IOrderBy orderBy = obj.getOrderBy();
        if(orderBy != null) {
            buffer.append(SPACE);
            append(orderBy);
        }

        ILimit limit = obj.getLimit();
        if(limit != null) {
            buffer.append(SPACE);
            append(limit);
        }
    }

    protected void appendSetOperation(ISetQuery.Operation operation) {
        buffer.append(operation);
    }
    
    protected boolean useParensForSetQueries() {
    	return false;
    }

    protected void appendSetQuery(IQueryCommand obj) {
        if(obj instanceof ISetQuery || useParensForSetQueries()) {
            buffer.append(LPAREN);
            append(obj);
            buffer.append(RPAREN);
        } else {
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
    public static String getSQLString(ILanguageObject obj) {
        SQLStringVisitor visitor = new SQLStringVisitor();
        visitor.append(obj);
        return visitor.toString();
    }
    
    protected boolean useParensForJoins() {
    	return false;
    }
}
