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

package org.teiid.net.socket;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;


public class Message implements Externalizable {
	public static final long serialVersionUID = 1063704220782714098L;
	private Object contents;
	private Serializable messageKey;

	public String toString() {
		return "MessageHolder: key=" + messageKey + " contents=" + contents; //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void setContents(Object contents) {
		this.contents = contents;
	}

	public Object getContents() {
		return contents;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.contents = in.readObject();
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
