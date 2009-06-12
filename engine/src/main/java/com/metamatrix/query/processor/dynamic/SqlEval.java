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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.transform.Source;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.symbol.Symbol;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.xquery.XQuerySQLEvaluator;


/** 
 * A SQL evaluator used in XQuery expression, where this will take SQL string and return a
 * XML 'Source' as output for the request evaluated. 
 */
public class SqlEval implements XQuerySQLEvaluator {

    private BufferManager bufferMgr;
    private CommandContext context;
    private ArrayList<TupleSourceID> openTupleList;
    private String parentGroup;
    
    public SqlEval(BufferManager bufferMgr, CommandContext context, String parentGroup) {
        this.bufferMgr = bufferMgr;
        this.context = context;
        this.parentGroup = parentGroup;
    }

    /** 
     * @see com.metamatrix.query.xquery.XQuerySQLEvaluator#executeDynamicSQL(java.lang.String)
     * @since 4.3
     */
    public Source executeSQL(String sql) 
        throws QueryParserException, MetaMatrixProcessingException, MetaMatrixComponentException {

    	QueryProcessor processor = context.getQueryProcessorFactory().createQueryProcessor(sql, parentGroup, context);
        // keep track of all the tuple sources opened during the 
        // xquery process; 
        if (openTupleList == null) {
            openTupleList = new ArrayList<TupleSourceID>();
        }
        openTupleList.add(processor.getResultsID());
        
        processor.process();
        
        TupleSourceID tsID = processor.getResultsID();
        TupleSource src = this.bufferMgr.getTupleSource(tsID);
        String[] columns = elementNames(src.getSchema());
        Class[] types= elementTypes(src.getSchema());
        boolean xml = false;
        
        // check to see if we have XML results
        if (src.getSchema().size() > 0) {
            xml = src.getSchema().get(0).getType().equals(DataTypeManager.DefaultDataClasses.XML);
        }            
        
        if (xml) {
            return XMLSource.createSource(columns, types, src, this.bufferMgr);
        }
        return SQLSource.createSource(columns, types, src);
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
        if (openTupleList != null && !openTupleList.isEmpty()) {
            for (Iterator i = openTupleList.iterator(); i.hasNext();) {
                TupleSourceID id = (TupleSourceID)i.next();
                try {
                    this.bufferMgr.removeTupleSource(id);
                } catch (TupleSourceNotFoundException e) {
                    // ignore and go on..
                }
            }
        }
        
    }
}
