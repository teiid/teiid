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

package com.metamatrix.server.dqp.service;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.dqp.message.AtomicResultsMessage;

public class RemoteResultsReceiver implements ResultsReceiver<AtomicResultsMessage>, Externalizable {

	private ResultsReceiver<AtomicResultsMessage> actualReceiver;
	private Object stub;
	private MessageBus messageBus;
	
	public RemoteResultsReceiver() {
		
	}
	
	public RemoteResultsReceiver(MessageBus bus) {
		this.messageBus = bus;
	}
	
	public void setActualReceiver(
			ResultsReceiver<AtomicResultsMessage> actualReceiver) {
		this.actualReceiver = actualReceiver;
	}
	
	public void exceptionOccurred(Throwable e) {
		actualReceiver.exceptionOccurred(e);
		shutdown();
	}

	public void receiveResults(AtomicResultsMessage results) {
		actualReceiver.receiveResults(results);
		if (results.isRequestClosed()) {
			shutdown();
		}
	}

	public void readExternal(ObjectInput arg0) throws IOException,
			ClassNotFoundException {
		this.stub = arg0.readObject();
		this.actualReceiver = (ResultsReceiver<AtomicResultsMessage>)this.messageBus.getRPCProxy(stub);
	}

	public void writeExternal(ObjectOutput arg0) throws IOException {
		stub = this.messageBus.export(this, new Class[] {ResultsReceiver.class});
	}
	
	public void shutdown() {
		if (stub != null) {
			this.messageBus.unExport(stub);
		}
	}
	
}
