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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.dqp.message.RequestID;
import org.teiid.query.tempdata.TempTableStore;

/**
 *
 */
//TODO: merge with DQPWorkContext
class ClientState {
	List<RequestID> requests;
	TempTableStore sessionTables;
	volatile SessionMetadata session;
	
	public ClientState(TempTableStore tableStoreImpl) {
		this.sessionTables = tableStoreImpl;
	}
	
	public synchronized void addRequest(RequestID requestID) {
		if (requests == null) {
			requests = new LinkedList<RequestID>();
		}
		requests.add(requestID);
	}
	
	public synchronized List<RequestID> getRequests() {
		if (requests == null) {
			return Collections.emptyList();
		}
		return new ArrayList<RequestID>(requests);
	}

	public synchronized void removeRequest(RequestID requestID) {
		if (requests != null) {
			requests.remove(requestID);
		}
	}
}