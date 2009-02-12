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

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.common.id.dbid.DBIDController;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.id.dbid.ReservedIDBlock;


public class InMemoryIDController implements DBIDController {

    private Map lastIDs = new HashMap();
    private static final long DEFAULT_ID_BLOCK_SIZE = 1000;

    private static Map idBlockMap = new HashMap();
    private static Map blockSizeMap = new HashMap();
    

    public InMemoryIDController() {
    }

   public ReservedIDBlock createIDBlock(long blockSize, String context) throws DBIDGeneratorException {
        Object obj = lastIDs.get(context);
        ReservedIDBlock block;
        if (obj == null) {
            block = new ReservedIDBlock(context, 1, blockSize, Long.MAX_VALUE);
        } else {
            ReservedIDBlock lastblock = (ReservedIDBlock) obj;
            long start = lastblock.getLast() + 1;
            block = new ReservedIDBlock(context, start, start + blockSize, Long.MAX_VALUE);
        }

        lastIDs.put(context, block);

        return block;

   }

   // the option to enableRollOver does not apply to memory ids
    public long getUniqueID(String context, boolean enableRollOver) throws DBIDGeneratorException {
        return getUniqueID(context);

    }

    public long getUniqueID(String context) throws DBIDGeneratorException {
        ReservedIDBlock idBlock = (ReservedIDBlock) idBlockMap.get(context);

        if (idBlock == null || idBlock.isDepleted()) {
            // get block size for context
            // if no block size exists then use default.
            Long bs = (Long) blockSizeMap.get(context);
            long bSize = DEFAULT_ID_BLOCK_SIZE;
            if (bs != null) {
                bSize = bs.longValue();
            }
            idBlock = createIDBlock(bSize, context);
            idBlockMap.put(context, idBlock);
        }
        return idBlock.getNextID();



    }


    public void setContextBlockSize(String context, long size) {
          blockSizeMap.put(context, new Long(size));
    }

    /**
    * Nothing to shutdown when inmemory is used.
    */
    public void shutDown() {

    }


} 
