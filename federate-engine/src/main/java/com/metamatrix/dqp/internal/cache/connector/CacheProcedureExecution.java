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

package com.metamatrix.dqp.internal.cache.connector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.dqp.internal.cache.ResultSetCache;

public class CacheProcedureExecution extends CacheBaseExecution implements ProcedureExecution{
	private ProcedureExecution actualExec;

	
	public CacheProcedureExecution(ProcedureExecution actualExec, ResultSetCache cache, ExecutionContext executionContext){
		super(actualExec, cache, executionContext);
		this.actualExec = actualExec;
	}

	public void execute(IProcedure procedure, int maxBatchSize) throws ConnectorException {
		super.setMaxBatchSize(maxBatchSize);
		
		List outParameter = new ArrayList();
		List params = procedure.getParameters();
        if(params != null && !params.isEmpty()){
            Iterator iter = params.iterator();
            while(iter.hasNext()){
                IParameter param = (IParameter)iter.next();
                if(param.getDirection() == IParameter.RETURN
                		|| param.getDirection() == IParameter.OUT 
						|| param.getDirection() == IParameter.INOUT){
                	outParameter.add(param.getMetadataID().getFullName());
                }
            }
        }
        super.setParameters(outParameter);

		if(!super.areResultsInCache(procedure.toString())){
			actualExec.execute(procedure, maxBatchSize);
		}
	}

	public Object getOutputValue(IParameter parameter) throws ConnectorException {
		if(super.hasResults()){
			return super.getOutputValue(parameter.getMetadataID().getFullName());
		}
		Object outValue = actualExec.getOutputValue(parameter);
		super.setOutputValue(parameter.getMetadataID().getFullName(), outValue);
		return outValue;
	}
}
