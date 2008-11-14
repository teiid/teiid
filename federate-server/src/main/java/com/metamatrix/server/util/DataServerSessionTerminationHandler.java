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

package com.metamatrix.server.util;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.MetaMatrixProductNames;
import com.metamatrix.metadata.runtime.RuntimeVDBDeleteUtility;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.SessionTerminationHandler;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.util.VDBDeleteUtility;
import com.metamatrix.server.ServerPlugin;
import com.metamatrix.server.query.service.QueryServiceInterface;

/**
 * SessionTerminationHandler implementation that cleans up all resources associated
 * with a Session on the MetaMatrix server.
 */
public class DataServerSessionTerminationHandler implements SessionTerminationHandler {

    public static final String PRODUCT_NAME = MetaMatrixProductNames.MetaMatrixServer.PRODUCT_NAME;

    private QueryServiceInterface queryServiceProxy;

    public String getProductName() {
        return PRODUCT_NAME;
    }

    public void cleanup(SessionToken token) throws Exception {

        // Cancel all queries and clean up cursors with queryService
        try {
      		// Cancel all queries
            getQueryServiceProxy().cancelQueries(token, true);
        } catch (Exception e) {
            Object[] params = new Object[]{token.getSessionID()};
            LogManager.logWarning(LogCommonConstants.CTX_CONFIG, e, ServerPlugin.Util.getString("DataServerSessionTerminationHandler.Error_communicating_with_QueryService__Could_not_cancel_queries_for_{0}", params)); //$NON-NLS-1$
        }

        // Clear cache in the queryService
        try {
            getQueryServiceProxy().clearCache(token);
        } catch (Exception e) {
            Object[] params = new Object[]{token.getSessionID()};
            LogManager.logWarning(LogCommonConstants.CTX_CONFIG, e,ServerPlugin.Util.getString("DataServerSessionTerminationHandler.Error_communicating_with_QueryService__Could_not_clear_cache_for_{0}", params)); //$NON-NLS-1$
        }

        // Delete VDB versions marked for deletion if no sessions are logged in
        // using them, or if this is the last session.
        try {
      		// Delete VDBs
            VDBDeleteUtility vdbDeleter = new RuntimeVDBDeleteUtility();
            vdbDeleter.deleteVDBsMarkedForDelete(token.getSessionID());
        } catch (Exception e) {
            Object[] params = new Object[]{token.getSessionID()};
            LogManager.logWarning(LogCommonConstants.CTX_CONFIG, e, ServerPlugin.Util.getString("DataServerSessionTerminationHandler.Error_deleting_VDB_vesions_for_{0}", params)); //$NON-NLS-1$
        }
    }

    private QueryServiceInterface getQueryServiceProxy() throws ServiceException {

        if (queryServiceProxy == null) {

            queryServiceProxy = PlatformProxyHelper.getQueryServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL);
        }
        return queryServiceProxy;
    }
}
