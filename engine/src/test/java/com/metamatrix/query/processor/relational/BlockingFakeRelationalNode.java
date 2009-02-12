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

package com.metamatrix.query.processor.relational;

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.*;

/**
 * @author amiller
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class BlockingFakeRelationalNode extends FakeRelationalNode {

    private boolean blocked = false;
    /**
     * @param nodeID
     * @param data
     */
    public BlockingFakeRelationalNode(int nodeID, List[] data) {
        super(nodeID, data);
    }

    public BlockingFakeRelationalNode(int nodeID, List[] data, int batchSize) {
        super(nodeID, data, batchSize);
    }

    /**
     * @param nodeID
     * @param source
     * @param batchSize
     */
    public BlockingFakeRelationalNode(int nodeID, TupleSource source, int batchSize) {
        super(nodeID, source, batchSize);
    }

    public TupleBatch nextBatchDirect() throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        if(! blocked) {
            blocked = true;
            throw BlockedException.INSTANCE;            
        }
        return super.nextBatchDirect();
    }

}
