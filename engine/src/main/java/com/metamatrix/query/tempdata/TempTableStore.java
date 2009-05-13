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

package com.metamatrix.query.tempdata;

import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.sql.lang.Command;

/** 
 * @since 5.5
 */
public interface TempTableStore {
    
    void removeTempTable(Command command) throws MetaMatrixComponentException;

    void removeTempTables() throws MetaMatrixComponentException;
  
    TempMetadataStore getMetadataStore();

    public TupleSource registerRequest(Command command)  throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    public boolean hasTempTable(Command command);
    
    public Set getAllTempTables();
    
    public void removeTempTableByName(String tempTableName) throws MetaMatrixComponentException;
    
    public TupleSourceID getTupleSourceID(String tempTableName);
}
