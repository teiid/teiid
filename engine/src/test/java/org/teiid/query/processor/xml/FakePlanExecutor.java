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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.processor.FakeTupleSource;
import org.teiid.query.processor.xml.PlanExecutor;



class FakePlanExecutor implements PlanExecutor{
    String resultName;
    FakeTupleSource tupleSource;
    List currentRow;
    List schema;
    
    FakePlanExecutor(String resultName, List[] rows){
        this.resultName = resultName;
        this.tupleSource = new FakeTupleSource(Collections.EMPTY_LIST, rows);
    }
    
    FakePlanExecutor(String resultName, List schema, List[] rows){
        this.resultName = resultName;
        this.schema = schema;
        this.tupleSource = new FakeTupleSource(schema, rows);
    }    
    public void close() throws TeiidComponentException {
    }

    public List currentRow() throws TeiidComponentException {
        return this.currentRow;
    }

    public void execute(Map values, boolean openOnly) throws TeiidComponentException, BlockedException {
        tupleSource.openSource();
    }

    public List nextRow() throws TeiidComponentException {
        currentRow = tupleSource.nextTuple();
        return currentRow;
    }

    public List getOutputElements() throws TeiidComponentException {
        return this.schema;
    }
}