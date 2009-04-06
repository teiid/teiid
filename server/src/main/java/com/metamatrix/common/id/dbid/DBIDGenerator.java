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

import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.id.dbid.spi.InMemoryIDController;
import com.metamatrix.common.id.dbid.spi.jdbc.PersistentIDController;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class DBIDGenerator implements DBIDController {

    private static DBIDGenerator generator = new DBIDGenerator();
    private DBIDController controller;

    private DBIDGenerator() {
	    setUseMemoryIDGeneration(false);
    }

    /**
     * Call to get a unique id for the given context and by default the
     * the id numbers cannot be rolled over and reused.
     * @param context that identifies a unique entity
     * @return long is the next id
     */
    public long getID(String context) throws DBIDGeneratorException {
        try {
        	return controller.getID(context);
        } catch (Exception e) {
            throw new DBIDGeneratorException(e, "Error creating id for " + context + " context."); //$NON-NLS-1$ //$NON-NLS-2$
        }
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

    public static DBIDGenerator getInstance() {
        return generator;
    }

}