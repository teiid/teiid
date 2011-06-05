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

package org.teiid.query.processor.xml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.util.VariableContext;



/** 
 * A XML context is an object which is passed between all the xml processing instructions.
 * context will have all the information possible for the block instructions to execute. It will
 * also have access to the parent context, however it will not have information on the sibiling
 * blocks.  
 */
class XMLContext {

    // map between variables and their values
    Map<String, PlanExecutor> resultsMap = new HashMap<String, PlanExecutor>();
    
    // reference to the parent variable context
    XMLContext parentContext;
    
    Map<String, PlanExecutor> executorMap = new HashMap<String, PlanExecutor>();
    
    VariableContext variableContext = new VariableContext();
    
    /**
     * Constructor for VariableContext.
     */
    public XMLContext() {
    }
    
    /**
     * Constructor for VariableContext.
     */
    public XMLContext(XMLContext parent) {
        this.parentContext = parent;
        this.variableContext.setParentContext(parent.variableContext);
    }
    
    public XMLContext getParentContext() {
        return this.parentContext;
    }
    
    /**
     * Get the current row of the given result set 
     * @param aliasResultName
     * @return
     */
    public List<?> getCurrentRow(String aliasResultName) throws TeiidComponentException, TeiidProcessingException {
        PlanExecutor executor = this.resultsMap.get(aliasResultName);
        if (executor == null) {
            if (this.parentContext != null) {
                return this.parentContext.getCurrentRow(aliasResultName);
            }
            throw new TeiidComponentException(QueryPlugin.Util.getString("results_not_found", aliasResultName)); //$NON-NLS-1$
        }
        return executor.currentRow();
    }

    /**
     * Get the next row from the result set given. 
     * @param aliasResultName
     * @return
     * @throws TeiidComponentException
     */
    public List<?> getNextRow(String aliasResultName) throws TeiidComponentException, TeiidProcessingException {
        PlanExecutor executor = this.resultsMap.get(aliasResultName);
        if (executor == null) {
            if (this.parentContext != null) {
                return this.parentContext.getNextRow(aliasResultName);
            }
            throw new TeiidComponentException(QueryPlugin.Util.getString("results_not_found", aliasResultName)); //$NON-NLS-1$
        }
        return executor.nextRow();
    }
    
    /**
     * Register the Result Set with the current context
     * @param resultName
     * @param id
     */
    public void setResultSet(String resultName, PlanExecutor executor) {
        this.resultsMap.put(resultName, executor);
    }

    /**
     * Be sure that when removing the results, we do not walk into the parent context
     * as there may be another resultset with same name. (recursive condition) 
     */
    public void removeResultSet(String resultName) throws TeiidComponentException {
        PlanExecutor executor = this.resultsMap.remove(resultName);
        if (executor != null) {
            executor.close();
        }
    }
    
    /**
     * Get the schema elements for the resultset. (note that the alias names only apply to results
     * not any thing else)
     * @param resultName
     * @return
     * @throws TeiidComponentException
     */
    public List<?> getOutputElements(String resultName) throws TeiidComponentException {
        PlanExecutor executor = this.resultsMap.get(resultName);
        if (executor == null) {
            if (this.parentContext != null) {
                return this.parentContext.getOutputElements(resultName);
            }
            throw new TeiidComponentException(QueryPlugin.Util.getString("results_not_found", resultName)); //$NON-NLS-1$
        }
        return executor.getOutputElements();        
    }
    
   
    /**
     * This will return element map containing the current row of all the parent resultset objects.
     * @return
     */    
    public Map getReferenceValues() {
        HashMap map = new HashMap();
        variableContext.getFlattenedContextMap(map);
        return map;
    }    
    
    /**
     * Get a registered plan executor object for the result
     * @param resultName
     * @return
     */
    public PlanExecutor getResultExecutor(String resultName) {
        return this.executorMap.get(resultName);
    }

    /**
     * Set a registered plan executor object for the result
     * @param resultName
     * @param executor
     */
    public void setResultExecutor(String resultName, PlanExecutor executor) {
        this.executorMap.put(resultName, executor);
    }
    
    /**
     * Remove registered plan executor object for the result
     * @param resultName
     */
    public void removeResultExecutor(String resultName) {
        this.executorMap.remove(resultName);
    }  
    
    public VariableContext getVariableContext() {
        return variableContext;
    }
    

    void setVariableValues(String resultSetName,
                                   List<?> row) throws TeiidComponentException {
        List elements = getOutputElements(resultSetName);
        
        for (int index = 0; index < elements.size(); index++) {
        	if (!(elements.get(index) instanceof ElementSymbol)) {
        		continue;
        	}
            ElementSymbol symbol = (ElementSymbol)elements.get(index);
            variableContext.setValue(new ElementSymbol(resultSetName + ElementSymbol.SEPARATOR + symbol.getShortName()), row.get(index));
        }
    }
}
