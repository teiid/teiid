/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.visitor.util;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IAggregate;
import com.metamatrix.connector.language.IBulkInsert;
import com.metamatrix.connector.language.ICaseExpression;
import com.metamatrix.connector.language.ICompareCriteria;
import com.metamatrix.connector.language.ICompoundCriteria;
import com.metamatrix.connector.language.ICriteria;
import com.metamatrix.connector.language.IDelete;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IExistsCriteria;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFrom;
import com.metamatrix.connector.language.IFromItem;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.IGroup;
import com.metamatrix.connector.language.IGroupBy;
import com.metamatrix.connector.language.IInCriteria;
import com.metamatrix.connector.language.IInlineView;
import com.metamatrix.connector.language.IInsert;
import com.metamatrix.connector.language.IIsNullCriteria;
import com.metamatrix.connector.language.IJoin;
import com.metamatrix.connector.language.ILanguageObject;
import com.metamatrix.connector.language.ILikeCriteria;
import com.metamatrix.connector.language.ILimit;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.language.INotCriteria;
import com.metamatrix.connector.language.IOrderBy;
import com.metamatrix.connector.language.IOrderByItem;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.IPredicateCriteria;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.language.IScalarSubquery;
import com.metamatrix.connector.language.ISearchedCaseExpression;
import com.metamatrix.connector.language.ISelect;
import com.metamatrix.connector.language.ISelectSymbol;
import com.metamatrix.connector.language.ISetClause;
import com.metamatrix.connector.language.ISetClauseList;
import com.metamatrix.connector.language.ISetQuery;
import com.metamatrix.connector.language.ISubqueryCompareCriteria;
import com.metamatrix.connector.language.ISubqueryInCriteria;
import com.metamatrix.connector.language.IUpdate;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.MetadataObject;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.visitor.framework.AbstractLanguageVisitor;
import com.metamatrix.core.util.StringUtil;

/**
 * Creates a SQL string for a LanguageObject subtree. Instances of this class
 * are not reusable, and are not thread-safe.
 */
public class SQLStringVisitor extends AbstractLanguageVisitor implements SQLReservedWords {
   
    private static final String ESCAPED_QUOTE = "''"; //$NON-NLS-1$    

    protected static final String UNDEFINED = "<undefined>"; //$NON-NLS-1$
    protected static final String UNDEFINED_PARAM = "?"; //$NON-NLS-1$
    
    protected RuntimeMetadata metadata;
    protected StringBuffer buffer = new StringBuffer();
                
    /**
     * Gets the name of a group or element from the RuntimeMetadata
     * @param id the id of the group or element
     * @return the name of that element or group as defined in the source
     */
    protected String getName(MetadataID id) {
        if (metadata == null) {
            return id.getName();
        }
        
        try {
            MetadataObject obj = metadata.getObject(id);
            if (obj == null) {
                return id.getName();
            }
            String nameInSource = obj.getNameInSource();
            if(nameInSource != null && nameInSource.length() > 0) {
                return nameInSource;
            }
            return id.getName();
        } catch(ConnectorException e) {
            return id.getName();
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
    protected void append(List items) {
        if (items != null && items.size() != 0) {
            append((ILanguageObject)items.get(0));
            for (int i = 1; i < items.size(); i++) {
                buffer.append(COMMA)
                      .append(SPACE);
                append((ILanguageObject)items.get(i));
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IAggregate)
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.ICaseExpression)
     */
    public void visit(ICaseExpression obj) {
        final IElement element = obj.getExpression() instanceof IElement ? (IElement)obj.getExpression() : null;
        final IFunction function = obj.getExpression() instanceof IFunction ? (IFunction)obj.getExpression() : null ;   

        buffer.append(CASE);
        buffer.append(SPACE);
       
        // checking for null compare in decode string case 2969 GCSS
        for (int i =0; i < obj.getWhenCount(); i++) {                          
            if (NULL.equalsIgnoreCase(obj.getWhenExpression(i).toString() ) ) { 
                buffer.append(WHEN);
                buffer.append(SPACE);
                
                if(element != null) {
                    visit(element);
                }else if(function != null) {
                    visit(function);
                }else {
                    append(obj.getExpression() );
                }

                buffer.append(SPACE);
                buffer.append(IS);
                buffer.append(SPACE);
                buffer.append(NULL);
                buffer.append(SPACE);
                buffer.append(THEN);
                buffer.append(SPACE);
                append(obj.getThenExpression(i));
                buffer.append(SPACE);
            }
        }
        
        for (int i = 0; i < obj.getWhenCount(); i++) {
            if(!NULL.equalsIgnoreCase(obj.getWhenExpression(i).toString() ) ) {
                buffer.append(WHEN);
                buffer.append(SPACE);
                
                if(element != null) {
                    visit(element);
                }else if(function != null) {
                    visit(function);
                }else {
                    append(obj.getExpression() );
                }

                buffer.append(EQ);
                append(obj.getWhenExpression(i));
                buffer.append(SPACE);
                buffer.append(THEN);
                buffer.append(SPACE);
                append(obj.getThenExpression(i));
                buffer.append(SPACE);
            }
        }

        if (obj.getElseExpression() != null) {
            buffer.append(ELSE);
            buffer.append(SPACE);
            if(obj.getElseExpression() instanceof IElement || obj.getElseExpression() instanceof IFunction) {
                if(element != null) {
                    visit(element);
                }else if(function != null) {
                    visit(function);
                }else {
                    append(obj.getExpression() );
                }
            }else {
                append(obj.getElseExpression());
            }
            buffer.append(SPACE);
        }
        buffer.append(END);              
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.ICompareCriteria)
     */
    public void visit(ICompareCriteria obj) {
        append(obj.getLeftExpression());
        buffer.append(SPACE);
        
        final int op = obj.getOperator();
        switch(op) {
            case ICompareCriteria.EQ: buffer.append(EQ); break;
            case ICompareCriteria.GE: buffer.append(GE); break;
            case ICompareCriteria.GT: buffer.append(GT); break;
            case ICompareCriteria.LE: buffer.append(LE); break;
            case ICompareCriteria.LT: buffer.append(LT); break;
            case ICompareCriteria.NE: buffer.append(NE); break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(SPACE);
        append(obj.getRightExpression());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.ICompoundCriteria)
     */
    public void visit(ICompoundCriteria obj) {
        String opString = null;
        final int op = obj.getOperator();
        switch(op) {
            case ICompoundCriteria.AND: opString = AND; break;
            case ICompoundCriteria.OR:  opString = OR;  break;
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IDelete)
     */
    public void visit(IDelete obj) {
        buffer.append(DELETE)
              .append(SPACE);
        buffer.append(addProcessComment());
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IElement)
     */
    public void visit(IElement obj) {
        String groupName = null;
        IGroup group = obj.getGroup();
        if (group != null) {
            if(group.getDefinition() != null) { 
                groupName = group.getContext();
            } else {  
                MetadataID groupID = group.getMetadataID();
                if(groupID != null) {              
                    groupName = getName(groupID);
                } else {
                    groupName = group.getContext();
                }
            }
        }
        
        String elemShortName = getElementShortName(obj);

        // Check whether a subclass wants to replace the element name to use in special circumstances
        String replacementElement = replaceElementName(groupName, elemShortName);
        if(replacementElement != null) {
            // If so, use it as is
            buffer.append(replacementElement);
        } else {
            // If not, do normal logic:  [group + "."] + element
            if(groupName != null) {
                buffer.append(groupName);
                buffer.append(DOT);
            }
            buffer.append(elemShortName);
        }
    }

	public String getElementShortName(IElement obj) {
		String elemShortName = null;        
        MetadataID elementID = obj.getMetadataID();
        if(elementID != null) {
            elemShortName = getName(elementID);            
        } else {
            String elementName = obj.getName();
            elemShortName = getShortName(elementName);
        }
		return elemShortName;
	}

    /** 
     * @param elementName
     * @return
     * @since 4.3
     */
    public String getShortName(String elementName) {
        return getElementShortName(elementName);
    }      
    
    public static String getElementShortName(String elementName) {
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
        
        if(obj.getMetadataID() != null) {
            buffer.append(getName(obj.getMetadataID()));                         
        } else {
            buffer.append(obj.getProcedureName());
        }
              
        buffer.append(LPAREN);
        final List params = obj.getParameters();
        if (params != null && params.size() != 0) {
            IParameter param = null;
            for (int i = 0; i < params.size(); i++) {
                param = (IParameter)params.get(i);
                if (param.getDirection() == IParameter.IN || param.getDirection() == IParameter.INOUT) {
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IFrom)
     */
    public void visit(IFrom obj) {
        buffer.append(FROM)
              .append(SPACE);
        append(obj.getItems());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IFunction)
     */
    public void visit(IFunction obj) {

        String name = obj.getName();
        IExpression[] args = obj.getParameters();
        if(name.equalsIgnoreCase(CONVERT) || name.equalsIgnoreCase(CAST)) { 
            
            // Need to support both Oracle style convert - convert(expression, type)
            // and SQL Server style convert - convert(type, expression)
            Object firstArg = null;
            Object secondArg = null;
            if (args[1] instanceof IElement) {
                Object typeValue = ((ILiteral)args[0]).getValue();
                Object expression = args[1];
                firstArg = typeValue;
                secondArg = expression;
            } else {
                Object typeValue = ((ILiteral)args[1]).getValue();
                Object expression = args[0];
                firstArg = expression;
                secondArg = typeValue;
            }
               
            buffer.append(name);
            buffer.append(LPAREN); 
            
            if(firstArg instanceof IExpression) {
                this.append( (IExpression)firstArg);
            }else {
                buffer.append(firstArg);
            }

            if(name.equalsIgnoreCase(CONVERT)) { 
                buffer.append(COMMA); 
                buffer.append(SPACE); 
            } else {
                buffer.append(SPACE); 
                buffer.append(AS); 
                buffer.append(SPACE); 
            }
            
            if(secondArg instanceof IExpression) {
                this.append( (IExpression)secondArg);
            }else {
                buffer.append(secondArg);
            }
            buffer.append(RPAREN); 

        } else if(name.equals("+") || name.equals("-") || name.equals("*") || name.equals("/") || name.equals("||")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            buffer.append(LPAREN); 

            if(args != null) {
                for(int i=0; i<args.length; i++) {
                    append(args[i]);
                    if(i < (args.length-1)) {
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

            if(args != null && args.length > 0) {
                buffer.append(((ILiteral)args[0]).getValue());

                for(int i=1; i<args.length; i++) {
                    buffer.append(COMMA); 
                    buffer.append(SPACE); 
                    append(args[i]);
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IGroup)
     */
    public void visit(IGroup obj) {
        MetadataID groupID = obj.getMetadataID();
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IGroupBy)
     */
    public void visit(IGroupBy obj) {
        buffer.append(GROUP)
              .append(SPACE)
              .append(BY)
              .append(SPACE);
        append(obj.getElements());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IInCriteria)
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IInsert)
     */
    public void visit(IInsert obj) {
        formatBasicInsert(obj);
        buffer.append(SPACE)
              .append(VALUES)
              .append(SPACE)
              .append(LPAREN);
        append(obj.getValues());
        buffer.append(RPAREN);  
    }
    
    /**
     * @param obj
     */
    private void formatBasicInsert(IInsert obj) {
        buffer.append(INSERT)
              .append(SPACE);
        buffer.append(addProcessComment());
        buffer.append(INTO)
              .append(SPACE);
        append(obj.getGroup());
        if (obj.getElements() != null && obj.getElements().size() != 0) {
            buffer.append(SPACE)
                  .append(LPAREN);

            int elementCount= obj.getElements().size();
            for(int i=0; i<elementCount; i++) {
	           	String elementShortNmae = getElementShortName((IElement)obj.getElements().get( i ));
	           	String replacedEmentShortNmae = getElementTrueName(elementShortNmae);
	           	if(replacedEmentShortNmae != null){
	           		buffer.append(replacedEmentShortNmae);
	           	}else{
	           		buffer.append(elementShortNmae);
	           	}
                if (i<elementCount-1) {
                    buffer.append(COMMA);
                    buffer.append(SPACE);
                }
            }            

            buffer.append(RPAREN);
        }
    }
    
    protected String getElementTrueName(String element) {
    	return null;
    }

    public void visit(IBulkInsert obj) {
        formatBasicInsert(obj);
        buffer.append(SPACE)
              .append(VALUES)
              .append(SPACE)
              .append(LPAREN);
        int elementCount= obj.getElements().size();
        for(int i=0; i<elementCount; i++) {
            buffer.append(UNDEFINED_PARAM);
            if (i<elementCount-1) {
                buffer.append(COMMA);
            }
        }
        buffer.append(RPAREN);  
    }    
    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IIsNullCriteria)
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IJoin)
     */
    public void visit(IJoin obj) {
        IFromItem leftItem = obj.getLeftItem();
        if(leftItem instanceof IJoin) {
            buffer.append(LPAREN);
            append(leftItem);
            buffer.append(RPAREN);
        } else {
            append(leftItem);
        }
        buffer.append(SPACE);
        
        final int type = obj.getJoinType();
        switch(type) {
            case IJoin.CROSS_JOIN:
                buffer.append(CROSS);
                break;
            case IJoin.FULL_OUTER_JOIN:
                buffer.append(FULL)
                      .append(SPACE)
                      .append(OUTER);
                break;
            case IJoin.INNER_JOIN:
                buffer.append(INNER);
                break;
            case IJoin.LEFT_OUTER_JOIN:
                buffer.append(LEFT)
                      .append(SPACE)
                      .append(OUTER);
                break;
            case IJoin.RIGHT_OUTER_JOIN:
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
        if(rightItem instanceof IJoin) {
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.ILikeCriteria)
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.ILiteral)
     */
    public void visit(ILiteral obj) {
        if (obj.getValue() == null) {
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.INotCriteria)
     */
    public void visit(INotCriteria obj) {
        buffer.append(NOT)
              .append(SPACE)
              .append(LPAREN);
        append(obj.getCriteria());
        buffer.append(RPAREN);
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IOrderBy)
     */
    public void visit(IOrderBy obj) {
        buffer.append(ORDER)
              .append(SPACE)
              .append(BY)
              .append(SPACE);
        append(obj.getItems());
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IOrderByItem)
     */
    public void visit(IOrderByItem obj) {
        if(obj.getName() != null) {
            String name = getShortName(obj.getName());
            buffer.append(name);
        } else if (obj.getElement() != null) {
            visit(obj.getElement());            
        } else {
            buffer.append(UNDEFINED);
        }
        if (obj.getDirection() == IOrderByItem.DESC) {
            buffer.append(SPACE)
                  .append(DESC);
        } // Don't print default "ASC"
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IParameter)
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IQuery)
     */
    public void visit(IQuery obj) {
        appendQuery(obj);
    }

    protected void appendQuery(IQuery obj) {
        append(obj.getSelect());
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.ISearchedCaseExpression)
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.ISelect)
     */
    public void visit(ISelect obj) {
        visitSelect(obj);
    }
    

    protected String addProcessComment() {
        return ""; //$NON-NLS-1$
    }
    
    /**
     * This method outputs the 'SELECT' keyword and select symbols.  Subclasses
     * should override to change/add behavior.
     * @param obj ISelect object
     * @since 4.3
     */
    protected void visitSelect(ISelect obj) {
        buffer.append(SELECT).append(SPACE);
        buffer.append(addProcessComment());
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(SPACE);
        }
        append(obj.getSelectSymbols());
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.ISelectSymbol)
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
        
        final int op = obj.getOperator();
        switch(op) {
            case ISubqueryCompareCriteria.EQ: buffer.append(EQ); break;
            case ISubqueryCompareCriteria.GE: buffer.append(GE); break;
            case ISubqueryCompareCriteria.GT: buffer.append(GT); break;
            case ISubqueryCompareCriteria.LE: buffer.append(LE); break;
            case ISubqueryCompareCriteria.LT: buffer.append(LT); break;
            case ISubqueryCompareCriteria.NE: buffer.append(NE); break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(SPACE);
        switch(obj.getQuantifier()) {
            case ISubqueryCompareCriteria.ALL: buffer.append(ALL); break;
            case ISubqueryCompareCriteria.SOME: buffer.append(SOME); break;
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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IUpdate)
     */
    public void visit(IUpdate obj) {
        buffer.append(UPDATE)
              .append(SPACE);
        buffer.append(addProcessComment());
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
        buffer.append(getElementShortName(clause.getSymbol()));
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

    protected void appendSetQuery(IQueryCommand obj) {
        if(obj instanceof ISetQuery) {
            buffer.append(LPAREN);
            append(obj);
            buffer.append(RPAREN);
        } else {
            appendQuery((IQuery)obj);
        }
    }
 
    /**
     * Gets the SQL string representation for a given ILanguageObject.
     * @param obj the root of the ILanguageObject hierarchy that needs to be
     * converted. This can be any subtree, and does not need to be a top-level
     * command
     * @return the SQL representation of that ILanguageObject hierarchy
     */
    public static String getSQLString(ILanguageObject obj, RuntimeMetadata metadata) {
        SQLStringVisitor visitor = new SQLStringVisitor();
        visitor.setRuntimeMetadata(metadata);
        visitor.append(obj);
        return visitor.toString();
    }

    public void setRuntimeMetadata(RuntimeMetadata metadata) {
        this.metadata = metadata;        
    }
    
}
