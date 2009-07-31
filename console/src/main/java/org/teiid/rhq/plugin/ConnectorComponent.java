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
package org.teiid.rhq.plugin;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Connector;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Connector.Operations;


/**
 * MetaMatrix Connector component class
 * 
 */
public class ConnectorComponent extends Facet {

	private final Log LOG = LogFactory.getLog(ConnectorComponent.class);

	/**
	 * @see org.teiid.rhq.plugin.Facet#getComponentType()
	 * @since 1.0
	 */
	@Override
	String getComponentType() {
		return Connector.TYPE;
	}
	
	protected void setOperationArguments(String name, Configuration configuration,
			Map argumentMap) {

			if (name.equals(Operations.STOP_CONNECTOR)){
				Boolean stopNow = configuration.getSimple(ConnectionConstants.ComponentType.Operation.Value.STOP_NOW).getBooleanValue();
				argumentMap.put(ConnectionConstants.ComponentType.Operation.Value.STOP_NOW, stopNow);
			}
			//Need identifier for all Connector operations
			String key = ConnectionConstants.IDENTIFIER;
			argumentMap.put(key, getComponentIdentifier());				 
		
	}	

	@Override
	public void getValues(MeasurementReport arg0,
			Set<MeasurementScheduleRequest> arg1) throws Exception {
		// TODO Auto-generated method stub

	}

//	@Override
//	public OperationResult invokeOperation(String name,
//			Configuration configuration) {
//		Map valueMap = new HashMap();
//		Connection conn = null;
//
//		Set operationDefinitionSet = this.resourceContext.getResourceType()
//				.getOperationDefinitions();
//
//		ExecutedOperationResult result = initResult(name, operationDefinitionSet);
//			
//		setValueMap(name, configuration, valueMap);
//		
//		execute(conn, result, getComponentType(), name, valueMap);
//		
//		return ((ExecutedOperationResultImpl) result).getOperationResult();
//		
		
//		Connection conn = null;
//		Map valueMap = new HashMap();
//		MMOperationResult result = null;
//
//		// Add "stop now" value if we are attempting to stop a connector
//		if (name.equals(ComponentType.Operation.STOP_CONNECTOR)) {
//			Boolean stopNow = configuration.getSimple(
//					ConnectionConstants.ComponentType.Operation.Value.STOP_NOW)
//					.getBooleanValue();
//			valueMap.put(
//					ConnectionConstants.ComponentType.Operation.Value.STOP_NOW,
//					stopNow);
//		}
//
//		valueMap.put(ConnectionConstants.IDENTIFIER, getComponentIdentifier());
//
//		try {
//			conn = getConnection();
//
//            if (!conn.isValid()) {
//                return null;
//            }
//			// Object operationReturnObject = 
//            conn.executeOperation(result,
//					getComponentType(), name, valueMap);
//            
//            
//		} catch (Exception e) {
//			final String msg = "Failed to invoke operation [" + name + "]. Cause: " + e; //$NON-NLS-1$ //$NON-NLS-2$
//			LOG.error(msg);
//			throw new RuntimeException(msg); 
//		} finally {
//			conn.close();
//		}
//
//		return (OperationResult)result;
//	}
}