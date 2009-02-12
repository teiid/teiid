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

package com.metamatrix.common.id.dbid.spi;

import com.metamatrix.common.connection.TransactionInterface;
import com.metamatrix.common.id.dbid.ReservedIDBlock;


/**
 * Defines methods used by DBIDGenerator create ID blocks.
 */
public interface DBIDSourceTransaction extends TransactionInterface {


    /**
     * <p>Create and return a new ReservedIDBlock.</p>
     * <p>Read in nextID from database, createIDBlock, then update nextID in database.
     *
     * @param blockSize size of id block
     * @param context
     * @param wrapNumber is true when the context is at its maximum limit and
     *    the next block should start over at the beginning
     * @return ReservedIDBlock
     * @throws Exception when an error updating or reading the database occurs
     */
    ReservedIDBlock createIDBlock(long blockSize, String context, boolean wrapNumber) throws Exception;

}

