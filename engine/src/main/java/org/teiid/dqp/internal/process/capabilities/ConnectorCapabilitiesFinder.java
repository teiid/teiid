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

package org.teiid.dqp.internal.process.capabilities;

import java.util.List;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;

/**
 */
public class ConnectorCapabilitiesFinder implements CapabilitiesFinder {

    private VDBService vdbService;
    private DataService dataService;
    private RequestMessage requestMessage;
    private DQPWorkContext workContext;

    /**
     * 
     */
    public ConnectorCapabilitiesFinder(VDBService vdbService, DataService dataService, RequestMessage requestMessage, DQPWorkContext workContext) {
        this.vdbService = vdbService;
        this.dataService = dataService;
        this.requestMessage = requestMessage;
        this.workContext = workContext;
    }

    /* 
     * @see com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder#findCapabilities(java.lang.String)
     */
    public SourceCapabilities findCapabilities(String modelName) throws MetaMatrixComponentException {
        List bindings = vdbService.getConnectorBindingNames(workContext.getVdbName(), workContext.getVdbVersion(), modelName);
        for(int i=0; i<bindings.size(); i++) {
            try {
                String connBinding = (String) bindings.get(i); 
                ConnectorID connector = dataService.selectConnector(connBinding);
                return dataService.getCapabilities(requestMessage, workContext, connector);
            }catch(MetaMatrixComponentException e) {
                if(i == bindings.size()-1) {
                    throw e;
                }
            }
        }
        return null;
    }

}
