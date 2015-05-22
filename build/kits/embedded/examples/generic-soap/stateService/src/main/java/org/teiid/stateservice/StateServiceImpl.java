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
package org.teiid.stateservice;

import org.teiid.stateservice.StateService;
import org.teiid.stateservice.jaxb.StateInfo;

import javax.ejb.Stateless;
import javax.jws.WebService;

@Stateless
@WebService(serviceName = "stateService", endpointInterface = "org.teiid.stateservice.StateService", targetNamespace = "http://www.teiid.org/stateService/")
public class StateServiceImpl implements StateService {
	
	StateData stateData = new StateData();
	
	public java.util.List<org.teiid.stateservice.jaxb.StateInfo> getAllStateInfo() {
		return stateData.getAll();
	}

	public org.teiid.stateservice.jaxb.StateInfo getStateInfo(java.lang.String stateCode) throws GetStateInfoFault_Exception {
		StateInfo info = stateData.getData(stateCode);
		if(null == info) {
			throw new GetStateInfoFault_Exception(stateCode + " is not a valid state abbreviation");
		}
		return info;
	}
}