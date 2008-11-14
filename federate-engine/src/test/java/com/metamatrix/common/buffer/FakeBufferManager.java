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

package com.metamatrix.common.buffer;

import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.lob.LobChunk;


/** 
 * Fake BufferManager for testing.
 * @since 4.2
 */
public class FakeBufferManager implements BufferManager {
    public void addStorageManager(StorageManager storageManager)  {}
    
    public void addTupleBatch(TupleSourceID tupleSourceID,TupleBatch tupleBatch) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {}
    
    public TupleSourceID createTupleSource(List elements,String[] types,String groupName,TupleSourceType tupleSourceType) 
        throws MetaMatrixComponentException {return null;}
    
    public int getConnectorBatchSize()  {return 0;}
    
    public int getFinalRowCount(TupleSourceID tupleSourceID) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {return 0;}
    
    public int getProcessorBatchSize()  {return 0;}
    
    public int getRowCount(TupleSourceID tupleSourceID) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {return 0;}
    
    public TupleSourceStatus getStatus(TupleSourceID tupleSourceID) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {return TupleSourceStatus.ACTIVE;}
    
    public List getTupleSchema(TupleSourceID tupleSourceID) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {return null;}
    
    public IndexedTupleSource getTupleSource(TupleSourceID tupleSourceID) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {return null;}
    
    public void initialize(String lookup,Properties properties) 
        throws MetaMatrixComponentException {}
    
    public TupleBatch pinTupleBatch(TupleSourceID tupleSourceID,int beginRow,int maxEndRow) 
        throws TupleSourceNotFoundException,MemoryNotAvailableException,MetaMatrixComponentException {return null;}
    
    public void removeTupleSource(TupleSourceID tupleSourceID) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {}
    
    public void removeTupleSources(String groupName) throws MetaMatrixComponentException {}
    
    public void setStatus(TupleSourceID tupleSourceID,TupleSourceStatus status) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {}
    
    public void stop()  {}
    
    public void unpinTupleBatch(TupleSourceID tupleSourceID,int firstRow,int lastRow) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {}
    
    public void addStreamablePart(TupleSourceID tupleSourceID,LobChunk streamGlob,int beginRow) 
    throws TupleSourceNotFoundException,MetaMatrixComponentException {}

    public LobChunk getStreamablePart(TupleSourceID tupleSourceID,int beginRow) 
        throws TupleSourceNotFoundException,MetaMatrixComponentException {return null;}

    public void releasePinnedBatches() {
    }
}
