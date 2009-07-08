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

package com.metamatrix.connector.metadata.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.IMetadataReference;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.metadata.IObjectQuery;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.MetadataObject;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.metadata.MetadataConnectorPlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;

/**
 * Reads a simple SQL parse tree to determine the table being queried, the columns being selected, and the criteria
 * being applied in the WHEN clause.
 * 
 * This only handles a very simple SQL statement, specifically:
 *     -one table in the FROM
 *     -only column names in the SELECT
 *     -WHERE clauses no more complicated than a series of compare criteria "AND"ed together
 */
public class ObjectQuery implements IObjectQuery {
    
    private RuntimeMetadata metadata;
    private IQuery query;
    private Map criteriaMap;

    private ArrayList columnNames = null;
    private ArrayList columnNamesInSource = null;
    private ArrayList columnTypes = null;
    private ArrayList caseTypes = null;    

    private String[] columnNamesArray;
    private String[] columnNamesInSourceArray;
    private Class[] columnTypesArray;
    private Integer[] caseTypeArray;    
    
    public ObjectQuery(RuntimeMetadata metadata, ICommand command) throws ConnectorException {
        ArgCheck.isNotNull(metadata);        
        ArgCheck.isNotNull(command);
        this.metadata = metadata;
        this.query = (IQuery) command;
        initColumnData();
    }
    
    public IQuery getQuery() {
        return this.query;
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.metadata.internal.IObjectQuery#getTableNameInSource()
	 */
    public String getTableNameInSource() throws ConnectorException {
		return getMetadataObjectName(getGroup());
    }
    
    private IGroup getGroup() {
        List fromItems = query.getFrom().getItems();        
        Assertion.assertTrue(fromItems.size() == 1, "Expected only one from item but found " + fromItems.size() + "."); //$NON-NLS-1$ //$NON-NLS-2$
        IGroup fromGroup = (IGroup) fromItems.get(0);
        return fromGroup;
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.connector.metadata.internal.IObjectQuery#getColumnNames()
	 */
    public String[] getColumnNames() {
        return columnNamesInSourceArray;
    }

    private void initColumnData() throws ConnectorException {
        List selectSymbols = query.getSelect().getSelectSymbols();
        columnNames = new ArrayList(selectSymbols.size());
        columnNamesInSource = new ArrayList(selectSymbols.size());
        columnTypes = new ArrayList(selectSymbols.size());
        caseTypes = new ArrayList(selectSymbols.size());
        for (Iterator selectIterator = selectSymbols.iterator(); selectIterator.hasNext();) {
            ISelectSymbol symbol = (ISelectSymbol) selectIterator.next();
            addColumn(symbol);
        }
        columnNamesArray = (String[]) columnNames.toArray(new String[columnNames.size()]);
        columnNamesInSourceArray = (String[]) columnNamesInSource.toArray(new String[columnNamesInSource.size()]);
        columnTypesArray = (Class[]) columnTypes.toArray(new Class[columnTypes.size()]);
        caseTypeArray = (Integer[]) caseTypes.toArray(new Integer[caseTypes.size()]);
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.connector.metadata.internal.IObjectQuery#checkType(int, java.lang.Object)
	 */
    public void checkType(int i, Object value) {
        if (value != null) {
            if (!columnTypesArray[i].isAssignableFrom(value.getClass())) {
                throw new MetaMatrixRuntimeException(
                    MetadataConnectorPlugin.Util.getString(
                        "ObjectQuery.Type_mismatch", //$NON-NLS-1$
                        new Object[] {
                            columnNamesArray[i],
                            columnTypesArray[i].getName(),
                            value.getClass().getName()}));
            }
        }
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.metadata.internal.IObjectQuery#checkCaseType(int, java.lang.Object)
	 */
    public void checkCaseType(int i, Object value) {
        if (value != null) {
            if(!(value instanceof String)) {
                Integer caseType = caseTypeArray[i];
                if(!caseType.equals(NO_CASE)) {
                    throw new MetaMatrixRuntimeException(MetadataConnectorPlugin.Util.getString("ObjectQuery.CASE_Function", columnNamesArray[i])); //$NON-NLS-1$
                }
            }
        }        
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.metadata.internal.IObjectQuery#getCaseType(int)
	 */
    public Integer getCaseType(int i) {
        return caseTypeArray[i];
    }

    private void addColumn(ISelectSymbol symbol) throws ConnectorException {
        IElement element = null;
        IExpression expression = symbol.getExpression();
        String functionName = null;
        if(expression instanceof IFunction) {
            IFunction function = (IFunction) expression;
            functionName = function.getName();
            List<IExpression> expressions = function.getParameters();
            element = (IElement)expressions.get(0);
        } else if(expression instanceof IElement) {
            element = (IElement)expression;
        }
        IMetadataReference reference = element;
        MetadataObject obj = reference.getMetadataObject();
        if (obj != null && obj.getNameInSource() != null) {
            Element elementMetadata = (Element) obj;
            columnTypes.add( elementMetadata.getJavaType() );
            columnNamesInSource.add(obj.getNameInSource());
            columnNames.add(obj.getFullName());
            if(functionName == null) {
                caseTypes.add(NO_CASE);
            } else if(functionName.equalsIgnoreCase("UPPER") || functionName.equalsIgnoreCase("UCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                caseTypes.add(UPPER_CASE);                
            } else if(functionName.equalsIgnoreCase("LOWER") || functionName.equalsIgnoreCase("LCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                caseTypes.add(LOWER_CASE);
            }
        }     
    }
    
    private String getMetadataObjectName(Object element) throws ConnectorException {
        IMetadataReference reference = (IMetadataReference) element;
        MetadataObject obj = reference.getMetadataObject();
        if (obj != null && obj.getNameInSource() != null) {
            return obj.getNameInSource();
        }
        throw new MetaMatrixRuntimeException(
            MetadataConnectorPlugin.Util.getString("ObjectQuery.Could_not_resolve_name_for_query___1", //$NON-NLS-1$
            new Object[] {query.toString()}));
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.metadata.internal.IObjectQuery#getCriteria()
	 */
    public Map getCriteria() throws ConnectorException {
		initCriteria();
        return this.criteriaMap;            
    }
    
    private void initCriteria() throws ConnectorException {
        if(this.criteriaMap == null) {        
            MetadataSearchCriteriaBuilder builder = new MetadataSearchCriteriaBuilder(this);
            this.criteriaMap = builder.getCriteria();
        }
    }
    
    Object getExpressionValue(IExpression expression) {
        ILiteral literal = null;
        String functionName = null;
        if(expression instanceof IFunction) {
            IFunction function = (IFunction) expression;
            List<IExpression> expressions = function.getParameters();
            literal = (ILiteral)expressions.get(0);
            functionName = function.getName();
        } else if(expression instanceof ILiteral) {
            literal = (ILiteral) expression;
        }
        Object value = literal.getValue();
        if(functionName != null && value != null) {
            if(functionName.equalsIgnoreCase("UPPER") || functionName.equalsIgnoreCase("UCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                value = value.toString().toUpperCase();
            } else if(functionName.equalsIgnoreCase("LOWER") || functionName.equalsIgnoreCase("LCASE")) { //$NON-NLS-1$ //$NON-NLS-2$
                value = value.toString().toLowerCase();
            }
        }
        return value;
    }
    
    String getElementName(IExpression expression) throws ConnectorException {
        IElement element = null;
        if(expression instanceof IFunction) {
            IFunction function = (IFunction) expression;
            List<IExpression> expressions = function.getParameters();
            element = (IElement)expressions.get(0);
        } else if(expression instanceof IElement) {
            element = (IElement)expression;
        }        
        return getMetadataObjectName(element);
    }
    
    String getFunctionName(IExpression expression) throws ConnectorException {
        if(expression instanceof IFunction) {
            IFunction function = (IFunction) expression;
            return function.getName();
        }        
        return null;
    }    
}
