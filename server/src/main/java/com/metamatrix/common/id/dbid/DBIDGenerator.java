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

package com.metamatrix.common.id.dbid;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.id.dbid.spi.InMemoryIDController;
import com.metamatrix.common.id.dbid.spi.jdbc.PersistentIDController;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class DBIDGenerator {

    public final static String VM_ID = "VM"; //$NON-NLS-1$
    public final static String SERVICE_ID = "Service"; //$NON-NLS-1$

    private static DBIDGenerator generator = new DBIDGenerator();
    private DBIDController controller;

    private DBIDGenerator() {
	    setUseMemoryIDGeneration(false);
    }
    /**
     * call to get a unique id for the given context and pass true if
     * the id numbers can be rolled over and reused.
     * @param context that identifies a unique entity
     * @param enableRollOver is true if the ids can reused
     * @return long is the next id
     */
    public long getID(String context, boolean enableRollOver) throws DBIDGeneratorException {
        try {
            return getUniqueID(context, enableRollOver);
        } catch (Exception e) {
            throw new DBIDGeneratorException(e, "Error creating id for " + context + " context."); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    /**
     * Call to get a unique id for the given context and by default the
     * the id numbers cannot be rolled over and reused.
     * @param context that identifies a unique entity
     * @return long is the next id
     */
    public long getID(String context) throws DBIDGeneratorException {
        try {
            return getUniqueID(context, true);
        } catch (Exception e) {
            throw new DBIDGeneratorException(e, ErrorMessageKeys.ID_ERR_0011, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0011, context));
        }
    }

    /**
     * Call to set the incremental block size for a specific context.  This
     * is a way to tune the caching which will reduce the number of database
     * reads that occur.  By increasing the cache, it should reduce the
     * number of database reads.
     * @param context that identifies a unique entity
     * @param cache is the size of the blocks to use

     */
    public void setCacheBlockSize(String context, long cache) throws DBIDGeneratorException {
        try {
            setContextBlockSize(context, cache);
        } catch (Exception e) {
            throw new DBIDGeneratorException(e, ErrorMessageKeys.ID_ERR_0012, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0012, context));
        }
    }

    /**
    *  Call when the persistent storage of ID's is not to be used.  This will cause
    *  the id's to be generated in memory and will not be written to the database
    *  for later use for a starting point.
    *  This was made available for the CDK because it needs to load runtime metadata
    *  disconnected from the application server or database.
    */
    public void setUseMemoryIDGeneration()  {
        controller = new InMemoryIDController();
    }

    /**
     * Call to switch whether persistent storage of ID's is to be used.
     * @param useInMemory whether or not to use in Memory ID generation
     */
    public void setUseMemoryIDGeneration(boolean useInMemory)  {
        if(useInMemory){
            controller = new InMemoryIDController();
        }else{
			try {
				controller = new PersistentIDController();
			} catch (ManagedConnectionException e) {
				throw new MetaMatrixRuntimeException(e);
			}
        }
    }

    /**
    * Call when the DBIDGenerator is no longer needed and the database connections
    * can be closed.
    */
    public void shutDown() {
        if (controller != null) {
            controller.shutDown();
        }
    }

    public static DBIDGenerator getInstance() {
        return generator;
    }

    private synchronized long getUniqueID(String context, boolean enableRollOver) throws DBIDGeneratorException {
        return controller.getUniqueID(context, enableRollOver);
    }
    private void setContextBlockSize(String context, long size) {
          controller.setContextBlockSize(context, size);
    }

}



