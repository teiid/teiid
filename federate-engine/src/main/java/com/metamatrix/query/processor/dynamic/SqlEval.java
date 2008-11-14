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
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.multisource.PlanModifier;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.symbol.Symbol;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.TypeRetrievalUtil;
import com.metamatrix.query.xquery.XQuerySQLEvaluator;


/** 
 * A SQL evaluator used in XQuery expression, where this will take SQL string and return a
 * XML 'Source' as output for the request evaluated. 
 */
public class SqlEval implements XQuerySQLEvaluator {

    private QueryMetadataInterface metadata;
    private CommandContext context;
    private CapabilitiesFinder finder;
    private IDGenerator idGenerator;
    private BufferManager bufferMgr;
    private ProcessorDataManager dataMgr;
    private ArrayList openTupleList;
    
    public SqlEval(QueryMetadataInterface metadata, CommandContext context, CapabilitiesFinder finder, IDGenerator idGenerator, BufferManager bufferMgr, ProcessorDataManager dataMgr) {
        this.metadata = metadata;
        this.context = context;
        this.finder = finder;
        this.idGenerator = idGenerator;
        this.bufferMgr = bufferMgr;
        this.dataMgr = dataMgr;
    }

    /** 
     * @see com.metamatrix.query.xquery.XQuerySQLEvaluator#executeDynamicSQL(java.lang.String)
     * @since 4.3
     */
    public Source executeSQL(String sql) 
        throws QueryParserException, MetaMatrixProcessingException, MetaMatrixComponentException {
               
        QueryParser parser = new QueryParser();
        Command command = parser.parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);            
        QueryRewriter.rewrite(command, null, metadata, context);
        ProcessorPlan plan = QueryOptimizer.optimizePlan(command, metadata, idGenerator, finder, AnalysisRecord.createNonRecordingRecord(), context);
        
        PlanModifier multiSourcePlanModifier = (PlanModifier) context.getMultiSourcePlanModifier();
        
        if (multiSourcePlanModifier != null) {
            multiSourcePlanModifier.modifyPlan(plan, metadata);
        }
        
        List elements = plan.getOutputElements();
        CommandContext copy = (CommandContext) context.clone();
        TupleSourceID resultsId = bufferMgr.createTupleSource(elements, TypeRetrievalUtil.getTypeNames(elements), context.getConnectionID(), TupleSourceType.PROCESSOR);
        copy.setTupleSourceID(resultsId);
        
        // keep track of all the tuple sources opened during the 
        // xquery process; 
        if (openTupleList == null) {
            openTupleList = new ArrayList();
        }
        openTupleList.add(resultsId);
        
        QueryProcessor processor = new QueryProcessor(plan, copy, bufferMgr, dataMgr);
        try {
            processor.process();
        } catch(MetaMatrixComponentException e) {
            throw e;
        } catch(MetaMatrixProcessingException e) {
            throw e;
        } catch (MetaMatrixCoreException e) {
        	throw new MetaMatrixComponentException(e, e.getMessage());
		}
        
        TupleSourceID tsID = processor.getResultsID();
        TupleSource src = this.bufferMgr.getTupleSource(tsID);
        String[] columns = elementNames(elements);
        Class[] types= elementTypes(elements);
        boolean xml = false;
        
        // check to see if we have XML results
        if (elements.size() > 0) {
            xml = ((SingleElementSymbol)elements.get(0)).getType().equals(DataTypeManager.DefaultDataClasses.XML);
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
