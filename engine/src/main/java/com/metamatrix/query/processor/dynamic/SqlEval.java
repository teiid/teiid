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

package com.metamatrix.query.processor.dynamic;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamSource;

import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.processor.BatchIterator;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.symbol.Symbol;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.xquery.XQuerySQLEvaluator;


/** 
 * A SQL evaluator used in XQuery expression, where this will take SQL string and return a
 * XML 'Source' as output for the request evaluated. 
 */
public class SqlEval implements XQuerySQLEvaluator {

    private static final String NO_RESULTS_DOCUMENT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><results/>"; //$NON-NLS-1$
	
    private CommandContext context;
    private ArrayList<QueryProcessor> processors;
    private String parentGroup;
    private Map<String, Expression> params;
    private ProcessorDataManager dataManager;
        
    public static Source createSource(TupleSource source) 
        throws  MetaMatrixProcessingException {

        try {
            // we only want to return the very first document from the result set
            // as XML we expect in doc function to have single XML document
            List tuple = source.nextTuple();
            if (tuple != null) {                        
                Object value = tuple.get(0);
                if (value != null) {
                    // below we will catch any invalid LOB refereces and return them
                    // as processing excceptions.
                    if (value instanceof XMLType) {
                        XMLType xml = (XMLType)value;
                        return xml.getSource(null);
                    }
                    return new StreamSource(new StringReader((String)value));
                }
            }
        } catch (Exception e) { 
            throw new MetaMatrixProcessingException(e);
        }
        return new StreamSource(new StringReader(NO_RESULTS_DOCUMENT));
    }
    
    public static SAXSource createSource(String[] columns, Class[] types, TupleSource source) throws  MetaMatrixProcessingException, MetaMatrixComponentException {

        try {
            // get the sax parser and the its XML reader and replace with 
            // our own. and then supply the customized input source.            
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);            
            SAXParser sp = spf.newSAXParser();            
            XMLReader reader = sp.getXMLReader();
            
            return new SAXSource(new TupleXMLReader(reader), new TupleInputSource(columns, types, source));
            
        } catch (ParserConfigurationException e) {
            throw new MetaMatrixComponentException(e);
        } catch (SAXException e) {
            throw new MetaMatrixProcessingException(e);
        }        
    }
    
    public SqlEval(ProcessorDataManager dataManager, CommandContext context, String parentGroup, Map<String, Expression> params) {
        this.dataManager = dataManager;
        this.context = context;
        this.parentGroup = parentGroup;
        this.params = params;
    }

    /** 
     * @see com.metamatrix.query.xquery.XQuerySQLEvaluator#executeDynamicSQL(java.lang.String)
     * @since 4.3
     */
    public Source executeSQL(String sql) 
        throws QueryParserException, MetaMatrixProcessingException, MetaMatrixComponentException {

    	QueryProcessor processor = context.getQueryProcessorFactory().createQueryProcessor(sql, parentGroup, context);
    	processor.setNonBlocking(true);
        if (processors == null) {
        	processors = new ArrayList<QueryProcessor>();
        }
        processors.add(processor);
        TupleSource src = new BatchIterator(processor);
        
        String[] columns = elementNames(src.getSchema());
        Class[] types= elementTypes(src.getSchema());
        boolean xml = false;
        
        // check to see if we have XML results
        if (src.getSchema().size() > 0) {
            xml = src.getSchema().get(0).getType().equals(DataTypeManager.DefaultDataClasses.XML);
        }            
        
        if (xml) {
            return createSource(src);
        }
        return createSource(columns, types, src);
    }
    
    @Override
    public Object getParameterValue(String key) throws ExpressionEvaluationException, BlockedException, MetaMatrixComponentException {
    	String paramName = this.parentGroup + ElementSymbol.SEPARATOR + key;
    	paramName = paramName.toUpperCase();
    	Expression expr = this.params.get(paramName);
    	return new Evaluator(Collections.emptyMap(), this.dataManager, context).evaluate(expr, Collections.emptyList());
    }

    /**
     * Get the Column names from Element Objects 
     * @param elements
     * @return Names of all the columns in the set.
     */
    String[] elementNames(List elements) {
        String[] columns = new String[elements.size()];
        
        for(int i = 0; i < elements.size(); i++) {
            SingleElementSymbol element = (SingleElementSymbol)elements.get(i);
            String name =  ((Symbol)(element)).getName();
            int index = name.lastIndexOf('.');
            if (index != -1) {
                name = name.substring(index+1);
            }
            columns[i] = name;
        }        
        return columns;
    }
    
    /**
     * Get types of the all the elements 
     * @param elements
     * @return class[] of element types
     */
    Class[] elementTypes(List elements) {
        Class[] types = new Class[elements.size()];
        
        for(int i = 0; i < elements.size(); i++) {
            SingleElementSymbol element = (SingleElementSymbol)elements.get(i);
            types[i] = element.getType();
        }        
        return types;
    }        
    
    /**
     * Closes any resources opened during the evaluation 
     */
    public void close() throws MetaMatrixComponentException {
        if (processors != null) {
        	for (QueryProcessor processor : processors) {
				processor.closeProcessing();
			}
        }
    }
}
