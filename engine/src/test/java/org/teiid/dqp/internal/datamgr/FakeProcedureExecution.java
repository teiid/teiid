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

package org.teiid.dqp.internal.datamgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ProcedureExecution;


final class FakeProcedureExecution implements ProcedureExecution {

    int resultSetSize;
    int rowNum;
    int paramSize;

    public FakeProcedureExecution(int resultSetSize, int paramSize) {
        this.resultSetSize = resultSetSize;
        this.paramSize = paramSize;
    }
    
    @Override
    public void execute() throws TranslatorException {
    	
    }
    
    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
    	List<Object> result = new ArrayList<Object>(paramSize);
    	for (int i = 0; i < paramSize; i++) {
    		result.add(i);
    	}
    	return result;
    }
    
    public void close() {
    }

    public void cancel() throws TranslatorException {
    }
    
    @Override
    public List next() throws TranslatorException, DataNotAvailableException {
    	if (rowNum == 1) {
    		return null;
    	}
    	rowNum++;
    	return Arrays.asList(new Object[resultSetSize]);
    }

}