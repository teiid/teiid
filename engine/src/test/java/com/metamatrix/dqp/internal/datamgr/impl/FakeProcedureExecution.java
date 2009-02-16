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

package com.metamatrix.dqp.internal.datamgr.impl;

import java.util.Arrays;
import java.util.List;

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.DataNotAvailableException;
import com.metamatrix.connector.api.ProcedureExecution;
import com.metamatrix.connector.basic.BasicExecution;
import com.metamatrix.connector.language.IParameter;

final class FakeProcedureExecution extends BasicExecution implements ProcedureExecution {

    int resultSetSize;
    int rowNum;

    public FakeProcedureExecution(int resultSetSize) {
        this.resultSetSize = resultSetSize;
    }
    
    @Override
    public void execute() throws ConnectorException {
    	
    }
    
    public Object getOutputValue(IParameter parameter) throws ConnectorException {
        return new Integer(parameter.getIndex());
    }

    public void close() throws ConnectorException {
    }

    public void cancel() throws ConnectorException {
    }
    
    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
    	if (rowNum == 1) {
    		return null;
    	}
    	rowNum++;
    	return Arrays.asList(new Object[resultSetSize]);
    }

}