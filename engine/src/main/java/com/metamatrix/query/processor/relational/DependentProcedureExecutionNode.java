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

package com.metamatrix.query.processor.relational;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.query.sql.lang.Criteria;

public class DependentProcedureExecutionNode extends PlanExecutionNode {

    private Criteria inputCriteria;
    private List inputReferences;
    private List inputDefaults;

    // processing state
    private DependentProcedureCriteriaProcessor criteriaProcessor;

    public DependentProcedureExecutionNode(int nodeID,
                                           Criteria crit,
                                           List references,
                                           List defaults) {
        super(nodeID);

        this.inputCriteria = crit;
        this.inputDefaults = defaults;
        this.inputReferences = references;
    }

    /*
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Dependent Procedure Execution"); //$NON-NLS-1$
        return props;
    }

    /**
     * @see com.metamatrix.query.processor.relational.PlanExecutionNode#clone()
     */
    public Object clone() {
        DependentProcedureExecutionNode copy = new DependentProcedureExecutionNode(getID(), (Criteria)inputCriteria.clone(),
                                                                                   new ArrayList(inputReferences),
                                                                                   new ArrayList(inputDefaults));
        copy(this, copy);
        return copy;
    }
    
    public void reset() {
        super.reset();
        criteriaProcessor = null;
    }
    
    public void close() throws MetaMatrixComponentException {
        if (isClosed()) {
            return;
        }
        super.close();

        if (criteriaProcessor != null) {
            criteriaProcessor.close();
        }
    }

    /**
     * @throws TupleSourceNotFoundException
     * @see com.metamatrix.query.processor.relational.PlanExecutionNode#prepareNextCommand()
     */
    protected boolean prepareNextCommand() throws BlockedException,
                                          MetaMatrixComponentException, MetaMatrixProcessingException {

        if (this.criteriaProcessor == null) {
            this.criteriaProcessor = new DependentProcedureCriteriaProcessor(this, (Criteria)inputCriteria.clone(), inputReferences, inputDefaults);
        }
        
        return criteriaProcessor.prepareNextCommand();
    }

    /**
     * @see com.metamatrix.query.processor.relational.PlanExecutionNode#hasNextCommand()
     */
    protected boolean hasNextCommand() {
        return criteriaProcessor.hasNextCommand();
    }

    /** 
     * @return Returns the inputCriteria.
     */
    public Criteria getInputCriteria() {
        return this.inputCriteria;
    }

}
