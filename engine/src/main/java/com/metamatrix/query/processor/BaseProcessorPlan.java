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

package com.metamatrix.query.processor;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.query.util.CommandContext;

/**
 */
public abstract class BaseProcessorPlan implements ProcessorPlan{

    private List warnings = null;
    
    private CommandContext context;

    /**
     * Constructor for BaseProcessorPlan.
     */
    public BaseProcessorPlan() {
        super();
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#getAndClearWarnings()
     */
    public List getAndClearWarnings() {
        if (warnings == null) {
            return null;
        }
        List copied = warnings;
        warnings = null;
        return copied;
    }

    protected void addWarning(MetaMatrixException warning) {
        if (warnings == null) {
            warnings = new ArrayList(1);
        }
        warnings.add(warning);
    }

    /**
     * @return
     */
    public CommandContext getContext() {
        return context;
    }

    /**
     * @param context
     */
    public void setContext(CommandContext context) {
        this.context = context;
    }

    public void reset() {
        warnings = null;
    }       

	public abstract Object clone();

}
