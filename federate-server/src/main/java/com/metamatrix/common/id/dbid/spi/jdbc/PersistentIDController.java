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

package com.metamatrix.common.id.dbid.spi.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.TransactionMgr;
import com.metamatrix.common.id.dbid.DBIDController;
import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.id.dbid.ReservedIDBlock;
import com.metamatrix.common.util.ErrorMessageKeys;

public class PersistentIDController implements DBIDController {

    private static TransactionMgr transMgr;

//    private static ManagedConnectionPool connectionPool;

    private static Map idBlockMap = new HashMap();
    private static Map blockSizeMap = new HashMap();

    // Contexts
    public final static String VM_ID = DBIDGenerator.VM_ID;
    public final static String SERVICE_ID = DBIDGenerator.SERVICE_ID;
    public static final String RESOURCE_POOL_MGR_ID = DBIDGenerator.RESOURCE_POOL_MGR_ID;
    
    private final static long VM_ID_BLOCK_SIZE = 1;
    private final static long SERVICE_ID_BLOCK_SIZE = 10;
    private final static long RESOURCE_POOL_MGR_ID_SIZE = 1;

    private static final long DEFAULT_ID_BLOCK_SIZE = 100;
//    private static final String DEFAULT_MAXIMUM_CONCURRENT_USERS = "1";

    private static final String FACTORY = "com.metamatrix.common.id.dbid.spi.jdbc.DBIDResourceTransactionFactory"; //$NON-NLS-1$

	private static final String PRINCIPAL = "DBID_GENERATOR"; //$NON-NLS-1$

    public PersistentIDController() throws ManagedConnectionException {

            // get the resource connection properties, if system properties where set
            // prior to this being called they will override.

            Properties props = new Properties(); 
            	//PropertiesUtils.clone(cfgProps, false);
            props.setProperty(TransactionMgr.FACTORY, FACTORY);

            transMgr = new TransactionMgr(props, PRINCIPAL);


            // Initialize block size for known contexts.
            blockSizeMap.put(VM_ID, new Long(VM_ID_BLOCK_SIZE));
            blockSizeMap.put(SERVICE_ID, new Long(SERVICE_ID_BLOCK_SIZE));
            blockSizeMap.put(RESOURCE_POOL_MGR_ID, new Long(RESOURCE_POOL_MGR_ID_SIZE));
    }


    private ReservedIDBlock createIDBlock(long blockSize, String context, boolean wrap) throws DBIDGeneratorException {

        ReservedIDBlock block = null;
        DBIDResourceTransaction transaction = null;
        try {
            transaction = (DBIDResourceTransaction) transMgr.getWriteTransaction();
            block = transaction.createIDBlock(blockSize, context, wrap);
            transaction.commit();
        } catch (Exception e) {
            try {
                transaction.rollback();
            } catch (Exception sqle) {
            }

            throw new DBIDGeneratorException(e,ErrorMessageKeys.ID_ERR_0014, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0014,
            		new Object[] {String.valueOf(blockSize), context}));
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return block;
    }

    public void setContextBlockSize(String context, long size) {
          blockSizeMap.put(context, new Long(size));
    }


    public long getUniqueID(String context) throws DBIDGeneratorException {
        return getUniqueID(context, false);
    }

    public long getUniqueID(String context, boolean enableRollOver) throws DBIDGeneratorException {

        ReservedIDBlock idBlock = (ReservedIDBlock) idBlockMap.get(context);

        if (idBlock == null) {
            long bSize = getBlockSize(context);
            idBlock = createIDBlock(bSize, context, false);

            idBlock.setIsWrappable(enableRollOver);
            idBlockMap.put(context, idBlock);

         } else if (idBlock.isDepleted()) {
            long bSize = getBlockSize(context);
            if (idBlock.isAtMaximum()) {
                if(idBlock.isWrappable()) {
                   idBlock = createIDBlock(bSize, context, true);
                } else {
                    throw new DBIDGeneratorException(ErrorMessageKeys.ID_ERR_0015, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0015,
                    		new Object[] {context, String.valueOf(idBlock.getMax())} ));
                }
            } else {
                idBlock = createIDBlock(bSize, context, false);
            }
            idBlock.setIsWrappable(enableRollOver);
            idBlockMap.put(context, idBlock);
        }

        return idBlock.getNextID();
    }

    private long getBlockSize(String context) {
            // get block size for context
            // if no block size exists then use default.

        Long bs = (Long) blockSizeMap.get(context);
        long bSize = DEFAULT_ID_BLOCK_SIZE;
        if (bs != null) {
            bSize = bs.longValue();
        }

        return bSize;
    }

    public void shutDown() {
    }



}
