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

package com.metamatrix.common.id.dbid.spi.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.TransactionMgr;
import com.metamatrix.common.id.dbid.DBIDController;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.id.dbid.ReservedIDBlock;
import com.metamatrix.common.util.ErrorMessageKeys;

public class PersistentIDController implements DBIDController {

    private static TransactionMgr transMgr;

    private static Map<String, ReservedIDBlock> idBlockMap = new HashMap<String, ReservedIDBlock>();

    private static final long DEFAULT_ID_BLOCK_SIZE = 100;

    private static final String FACTORY = "com.metamatrix.common.id.dbid.spi.jdbc.DBIDResourceTransactionFactory"; //$NON-NLS-1$

	private static final String PRINCIPAL = "DBID_GENERATOR"; //$NON-NLS-1$

    public PersistentIDController() throws ManagedConnectionException {
        // get the resource connection properties, if system properties where set
        // prior to this being called they will override.

        Properties props = new Properties(); 
        	//PropertiesUtils.clone(cfgProps, false);
        props.setProperty(TransactionMgr.FACTORY, FACTORY);

        transMgr = new TransactionMgr(props, PRINCIPAL);
    }


    private void setNextBlockValues(long blockSize, String context, ReservedIDBlock block) throws DBIDGeneratorException {
        DBIDResourceTransaction transaction = null;
        try {
            transaction = (DBIDResourceTransaction) transMgr.getWriteTransaction();
            transaction.createIDBlock(blockSize, context, block);
            transaction.commit();
        } catch (Exception e) {
            try {
            	if (transaction != null) { 
            		transaction.rollback();
            	}
            } catch (Exception sqle) {
            }

            throw new DBIDGeneratorException(e,ErrorMessageKeys.ID_ERR_0014, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0014,
            		new Object[] {String.valueOf(blockSize), context}));
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
    }

    public long getID(String context)
			throws DBIDGeneratorException {
    	ReservedIDBlock idBlock = null;
    	synchronized (idBlockMap) {
			idBlock = idBlockMap.get(context);
			
			if (idBlock == null) {
				idBlock = new ReservedIDBlock();
				idBlockMap.put(context, idBlock);
			}
    	}
		synchronized (idBlock) {
	    	long result = idBlock.getNextID();
	    	if (result != ReservedIDBlock.NO_ID_AVAILABLE) {
	    		return result;
	    	}
	    	setNextBlockValues(DEFAULT_ID_BLOCK_SIZE, context, idBlock);
	    	return idBlock.getNextID();
		}
	}

}
