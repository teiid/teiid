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

/**
 * 
 */
package com.metamatrix.dqp.internal.datamgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.connector.api.ProcedureExecution;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.dqp.DQPPlugin;

class ProcedureBatchHandler {
	private IProcedure proc;
	private ProcedureExecution procExec;
	private int paramCols = 0;
	private int resultSetCols = 0;
	private List filler;
    
	public ProcedureBatchHandler(IProcedure proc, ProcedureExecution procExec) throws ConnectorException {
		this.proc = proc;
		this.procExec = procExec;
        List params = proc.getParameters();

        if(params != null && !params.isEmpty()){
            Iterator iter = params.iterator();
            while(iter.hasNext()){
                IParameter param = (IParameter)iter.next();
                if (param.getDirection() == IParameter.RESULT_SET) {
                    resultSetCols = param.getMetadataID().getChildIDs().size();
                } else if(param.getDirection() == IParameter.RETURN || param.getDirection() == IParameter.OUT || param.getDirection() == IParameter.INOUT){
                    paramCols += 1;
                }
            }
        }
        if (paramCols > 0) {
        	filler = Collections.nCopies(paramCols, null);
        }
	}
	
	List padRow(List row) throws ConnectorException {
        if (row.size() != resultSetCols) {
            throw new ConnectorException(DQPPlugin.Util.getString("ConnectorWorker.ConnectorWorker_result_set_unexpected_columns", new Object[] {proc, new Integer(resultSetCols), new Integer(row.size())})); //$NON-NLS-1$
        }
        if (paramCols == 0) {
        	return row;
        }
        List result = new ArrayList(resultSetCols + paramCols);
        result.addAll(row);
        result.addAll(filler);
        return result;
	}
	
	List<List> getOutputRows() throws ConnectorException {
		if (this.paramCols == 0) {
			return Collections.emptyList();
		}
		List params = proc.getParameters();
		List outParamValues = new ArrayList(this.paramCols);
		List<List> results = new ArrayList<List>(this.paramCols);
		Iterator iter = params.iterator();
        //return
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == IParameter.RETURN){
                outParamValues.add(procExec.getOutputValue(param));
            }
        }
        //out, inout
        iter = params.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == IParameter.OUT || param.getDirection() == IParameter.INOUT){
                outParamValues.add(procExec.getOutputValue(param));
            }
        }

        //add out/return values
        Iterator i = outParamValues.iterator();
        for(int index = resultSetCols; i.hasNext(); index++){
            Object[] newRow = new Object[paramCols + resultSetCols];
            newRow[index] = i.next();
            results.add(Arrays.asList(newRow));
        }
        return results;
	}
	
}
