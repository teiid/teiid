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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.language.ICompareCriteria;
import com.metamatrix.connector.language.ICompoundCriteria;
import com.metamatrix.connector.language.ICriteria;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IInCriteria;
import com.metamatrix.connector.language.IIsNullCriteria;
import com.metamatrix.connector.language.ILikeCriteria;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.ICompareCriteria.Operator;
import com.metamatrix.connector.metadata.MetadataConnectorConstants;
import com.metamatrix.connector.metadata.MetadataConnectorPlugin;
import com.metamatrix.connector.metadata.index.MetadataInCriteria;
import com.metamatrix.connector.metadata.index.MetadataLiteralCriteria;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.query.sql.lang.MatchCriteria.PatternTranslator;


/** 
 * Build MetadataSearchCriteria objects from a query's ICriteria or procedure in parameter
 * values. 
 * @since 4.3
 */
public class MetadataSearchCriteriaBuilder {
    
    
    // Map of fieldNames to MetadataSearchCriteria objects used to search
    private final Map criteriaMap = new HashMap();
    private ObjectQuery query;
    /** The default wildcard character - '%' */
    public static final char WILDCARD_CHAR = '%';

    /** The default single match character - '_' */
    public static final char MATCH_CHAR = '_';

    /** The internal null escape character */
    public static final char NULL_ESCAPE_CHAR = 0;

    private final static PatternTranslator LIKE_TO_INDEX = new PatternTranslator("*", "?", new char[0], '\\');  //$NON-NLS-1$ //$NON-NLS-2$

    /**
     * Constructor MetadataSearchCriteriaBuilder
     * @param query The ObjectQuery with actual IQuery on it
     * @throws ConnectorException
     * @since 4.3
     */
    public MetadataSearchCriteriaBuilder(final ObjectQuery query) throws ConnectorException {
        ArgCheck.isNotNull(query);
        this.query = query;
        initCriteria(query.getQuery());
    }

    /**
     * Constructor MetadataSearchCriteriaBuilder
     * @param query The ObjectProcedure with actual IProcedure on it
     * @throws ConnectorException 
     * @param proc
     * @throws ConnectorException
     * @since 4.3
     */
    public MetadataSearchCriteriaBuilder(final ObjectProcedure proc) throws ConnectorException {
        ArgCheck.isNotNull(proc);
        initCriteria(proc);        
    }

    /**
     * Initialize criteria map reading in ICriteria 
     * @throws ConnectorException 
     * @since 4.3
     */
    private void initCriteria(IQuery query) throws ConnectorException {
        ArgCheck.isNotNull(query);        
        buildMetadataSearchCriteria(query.getWhere());
    }
    
    /**
     * Build the criteriaMap used for querying the index files.
     * This is built by using all input parameters having get method names
     * as their name in sources.   
     * @since 4.2
     */
    private void initCriteria(ObjectProcedure proc) throws ConnectorException {
        Collection inParams = proc.getInParameters();
        if(inParams != null) {
            for(final Iterator iter = inParams.iterator(); iter.hasNext();) {
                IParameter parameter = (IParameter) iter.next();
                String nameInSource = proc.getParameterNameInSource(parameter);
                if(nameInSource != null) {
                    if(StringUtil.startsWithIgnoreCase(nameInSource, MetadataConnectorConstants.GET_METHOD_PREFIX)) {
                        Object value = parameter.getValue();
                        if(value != null) {
                            MetadataLiteralCriteria literalCriteria = new MetadataLiteralCriteria(nameInSource, value);                            
                            criteriaMap.put(nameInSource.toUpperCase(), literalCriteria);
                        }
                    }
                }
            }
        }
    }    
    
    /**
     * Map of fieldNames to MetadataSearchCriteria objects used to search 
     * @return Map object
     * @since 4.3
     */
    public Map getCriteria() {
        return this.criteriaMap;
    }

    /**
     * Build MetadataSearchCriteria objects given ICriteria which is part of 
     * query language getting submitted to the connector.
     * @param criteria ICriteria object part of query
     * @throws ConnectorException 
     * @since 4.3
     */
    public void buildMetadataSearchCriteria(ICriteria criteria) throws ConnectorException {
        if (criteria != null) {
            if (criteria instanceof ICompareCriteria) {
                buildMetadataCompareCriteria((ICompareCriteria) criteria);
            } else if (criteria instanceof ILikeCriteria) {
                buildMetadataLikeCriteria((ILikeCriteria) criteria);
            } else if (criteria instanceof IInCriteria) {
                buildMetadataInCriteria((IInCriteria) criteria);
            } else if (criteria instanceof ICompoundCriteria) {
                buildMetadataCompoundCriteria((ICompoundCriteria) criteria);
            } else if (criteria instanceof IIsNullCriteria) {
                buildMetadataIsNullCriteria((IIsNullCriteria) criteria);
            } else {
                Object[] params = new Object[] { criteria };
                throw new RuntimeException(MetadataConnectorPlugin.Util.getString("ObjectQuery.Unsupported_criteria_{0}", params)); //$NON-NLS-1$
            }
        }
    }
    
    /**
     * Build MetadataLiteralCriteria objects given IsNullCriteria which is part of 
     * query language getting submitted to the connector.
     * @param criteria ICompareCriteria object part of query
     */
    private void buildMetadataIsNullCriteria(IIsNullCriteria criteria) throws ConnectorException {
        if (criteria.isNegated()) {
            Object[] params = new Object[] { criteria };
            throw new RuntimeException(MetadataConnectorPlugin.Util.getString("ObjectQuery.Unsupported_criteria_{0}", params)); //$NON-NLS-1$
        }
   
        IExpression ltExpression = criteria.getExpression();

        String fieldName = this.query.getElementName(ltExpression);
        String fieldFunctionName = this.query.getFunctionName(ltExpression);
        
        MetadataLiteralCriteria literalCriteria = new MetadataLiteralCriteria(fieldName, null);
        literalCriteria.setFieldFunction(fieldFunctionName);
        // update criteria map with the criteria
        criteriaMap.put(fieldName.toUpperCase(), literalCriteria);
    }    
    
    /**
     * Build MetadataLiteralCriteria objects given ICompareCriteria which is part of 
     * query language getting submitted to the connector.
     * @param criteria ICompareCriteria object part of query
     * @since 4.3
     */
    private void buildMetadataCompareCriteria(ICompareCriteria criteria) throws ConnectorException {
        Assertion.assertTrue(criteria.getOperator() == Operator.EQ, "Only supports equals."); //$NON-NLS-1$


        IExpression ltExpression = criteria.getLeftExpression();
        IExpression rtExpression = criteria.getRightExpression();

        String fieldName = this.query.getElementName(ltExpression);
        Object literalValue = this.query.getExpressionValue(rtExpression);
        String fieldFunctionName = this.query.getFunctionName(ltExpression);
        String valueFunctionName = this.query.getFunctionName(rtExpression);        
        
        MetadataLiteralCriteria literalCriteria = new MetadataLiteralCriteria(fieldName, literalValue);
        literalCriteria.setFieldFunction(fieldFunctionName);
        literalCriteria.setValueFunction(valueFunctionName);
        // update criteria map with the criteria
        criteriaMap.put(fieldName.toUpperCase(), literalCriteria);
    }
    
    /**
     * Build MetadataLiteralCriteria objects given ILikeCriteria which is part of 
     * query language getting submitted to the connector.
     * @param criteria ILikeCriteria object part of query
     * @throws ConnectorException 
     * @since 4.3
     */    
    private void buildMetadataLikeCriteria(ILikeCriteria criteria) throws ConnectorException {
        IExpression ltExpression = criteria.getLeftExpression();
        IExpression rtExpression = criteria.getRightExpression();
        Character escape = criteria.getEscapeCharacter();
        String fieldName = this.query.getElementName(ltExpression);
        String literalValue = (String) this.query.getExpressionValue(rtExpression);
        StringBuffer rePattern = null;

        char escapeChar = NULL_ESCAPE_CHAR;
        if ( escape != null ) {
            escapeChar = escape.charValue();            
        }
        
        try {
            rePattern = LIKE_TO_INDEX.translate(literalValue, escapeChar);

        } catch ( CriteriaEvaluationException e ) {
            throw new ConnectorException( e );
        }
        literalValue = rePattern.toString();
        
        String fieldFunctionName = this.query.getFunctionName(ltExpression);
        String valueFunctionName = this.query.getFunctionName(rtExpression);        
        
        MetadataLiteralCriteria literalCriteria = new MetadataLiteralCriteria(fieldName, literalValue);
        literalCriteria.setFieldFunction(fieldFunctionName);
        literalCriteria.setValueFunction(valueFunctionName);
        // update criteria map with the criteria
        criteriaMap.put(fieldName.toUpperCase(), literalCriteria);        
    }
    
    /**
     * Build MetadataInCriteria objects given IInCriteria which is part of 
     * query language getting submitted to the connector.
     * @param criteria IInCriteria object part of query
     * @since 4.3
     */    
    private void buildMetadataInCriteria(IInCriteria criteria) throws ConnectorException {
        IExpression ltExpression = criteria.getLeftExpression();
        Collection rtExpressions = criteria.getRightExpressions();
        Collection literalValues = new ArrayList(rtExpressions.size());

        String fieldName = this.query.getElementName(ltExpression);
        for(final Iterator iter = rtExpressions.iterator(); iter.hasNext();) {
            literalValues.add(this.query.getExpressionValue((IExpression) iter.next()));
        }

        String fieldFunctionName = this.query.getFunctionName(ltExpression);
        MetadataInCriteria literalCriteria = new MetadataInCriteria(fieldName, literalValues);
        literalCriteria.setFieldFunction(fieldFunctionName);

        // update criteria map with the criteria
        criteriaMap.put(fieldName.toUpperCase(), literalCriteria);        
    }     

    /**
     * Build MetadataLiteralCriteria objects given ICompoundCriteria which is part of 
     * query language getting submitted to the connector.
     * @param criteria ICompoundCriteria object part of query
     * @throws ConnectorException 
     * @since 4.3
     */
    private void buildMetadataCompoundCriteria(ICompoundCriteria compoundCriteria) throws ConnectorException {
        if (compoundCriteria.getOperator() == com.metamatrix.connector.language.ICompoundCriteria.Operator.AND) {
            for(final Iterator critIter = compoundCriteria.getCriteria().iterator(); critIter.hasNext();) {
                buildMetadataSearchCriteria((ICriteria)critIter.next());
            }
        } else {
            throw new RuntimeException(MetadataConnectorPlugin.Util.getString("ObjectQuery.Only_supports_AND_operator")); //$NON-NLS-1$
        }
    }
   
}