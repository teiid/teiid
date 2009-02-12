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

package com.metamatrix.common.buffer;

import com.metamatrix.api.exception.MetaMatrixComponentException;


/** 
 * @since 4.2
 */
public interface IndexedTupleSource extends
                                   TupleSource {

    
    /**
     * Returns the index of the current tuple in the tuple source.
     */
    int getCurrentTupleIndex();
    
    /**
     * Set the current index of the tuple source.  Allows the user of the tuple source to
     * set the row that is retrieved.
     */
    void setCurrentTupleIndex(int index) throws MetaMatrixComponentException;
}
