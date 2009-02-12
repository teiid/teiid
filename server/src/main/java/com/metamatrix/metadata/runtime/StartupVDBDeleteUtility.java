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

package com.metamatrix.metadata.runtime;

import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.connection.TransactionMgr;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;
import com.metamatrix.platform.security.api.AuthorizationRealm;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.service.AuthorizationServicePropertyNames;
import com.metamatrix.platform.security.authorization.spi.AuthorizationSourceTransaction;


/** 
 * Implementation of VDBDeleteUtility to be used while starting the server.
 * This implementation does not need access to running services.
 * <p>
 * This implementation does not checking of sessions connected to a VDB - 
 * it assumes that it's safe to delete all VDBs marked for deletion,
 * because it is meant to be called only at server startup time.
 * @since 4.3
 */
public class StartupVDBDeleteUtility extends AbstractVDBDeleteUtility {


    /**
     * Check for any sessions logged into the specified VDB.
     * @param vdbID 
     * @param sessionID
     * @return true   This implementation always returns true because it does not have access to the SessionService.
     * @since 4.3
     */
    public boolean checkSessions(VirtualDatabaseID vdbID, MetaMatrixSessionID sessionID) throws MetaMatrixComponentException {
        return true;    
    }
    
    /**
     * Check for any sessions logged into the specified VDB.
     * @param vdbID 
     * @return true   This implementation always returns true because it does not have access to the SessionService.
     * @since 4.3
     */
    public boolean checkSessions(VirtualDatabaseID vdbID) throws MetaMatrixComponentException {
        return true;    
    }
    
    
    /**
     * For a given VDB version, delete all AuthorizationPolicies associated with it.
     */
    public void deleteAuthorizationPoliciesForVDB( String VDBName, String VDBVersion )
        throws MetaMatrixComponentException {
        
        //Get JDBCSessionTransaction connection properties
        Properties transactionProps = CurrentConfiguration.getInstance().getProperties();
        if (transactionProps.getProperty(AuthorizationServicePropertyNames.CONNECTION_FACTORY) == null) {
        	transactionProps.setProperty(AuthorizationServicePropertyNames.CONNECTION_FACTORY, AuthorizationServicePropertyNames.DEFAULT_FACTORY_CLASS);
        }       
        
         transactionProps.setProperty(TransactionMgr.FACTORY, 
                     transactionProps.getProperty(AuthorizationServicePropertyNames.CONNECTION_FACTORY));

        // Get a write transaction and delete old authorizations
        AuthorizationSourceTransaction transaction = null;
        try {
            TransactionMgr transMgr = new TransactionMgr(transactionProps, "MetaMatrixController"); //$NON-NLS-1$            
            transaction = (AuthorizationSourceTransaction) transMgr.getWriteTransaction();
            
            AuthorizationRealm realm = new AuthorizationRealm(VDBName, VDBVersion);
            transaction.removePrincipalsAndPoliciesForRealm(realm);                
            
            transaction.commit();            
        } catch (Exception e) {
            I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.VDBDU_0004, e,new Object[]{VDBName, VDBVersion});
            try {
                transaction.rollback();
            } catch( Exception ex ) { // Do nothing                    
            }
        } finally {
            if (transaction != null) {
                try {
                    transaction.close();
                } catch (Exception ex) { // Do nothing                                         
                }
            }
            transaction = null;
        }
    }
        
    
    
}
