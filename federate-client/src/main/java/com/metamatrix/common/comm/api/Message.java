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

package com.metamatrix.common.comm.api;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import com.metamatrix.common.comm.exception.CommunicationException;

public class Message implements Externalizable {
	public static final long serialVersionUID = 1063704220782714098L;
	private Serializable contents;
	private Serializable messageKey;

	public String toString() {
		return "MessageHolder: contents=" + contents; //$NON-NLS-1$
	}

	public void setContents(Serializable contents) {
		this.contents = contents;
	}

	public Serializable getContents() {
		return contents;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		try {
			this.contents = (Serializable) in.readObject();
		} catch (Throwable t) {
			this.contents = new CommunicationException(t);
		}
		this.messageKey = (Serializable) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(this.contents);
		out.writeObject(messageKey);
	}

	public Serializable getMessageKey() {
		return messageKey;
	}

	public void setMessageKey(Serializable messageKey) {
		this.messageKey = messageKey;
	}

}
