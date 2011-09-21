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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.teiid.client.BatchSerializer;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.util.ExternalizeUtil;

public class SerializableTupleBatch extends TupleBatch implements Externalizable {
	
	private String[] types;
	
	public SerializableTupleBatch() {
		//for Externalizable
	}
	
	public SerializableTupleBatch(TupleBatch batch, String[] types) {
		super(batch.getBeginRow(), batch.getTuples());
		this.types = types;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		String[] types = ExternalizeUtil.readStringArray(in);
		this.setRowOffset(in.readInt());
		this.setTerminationFlag(in.readBoolean());
		this.tuples = (List)BatchSerializer.readBatch(in, types);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		ExternalizeUtil.writeArray(out, types);
		out.writeInt(this.getBeginRow());
		out.writeBoolean(this.getTerminationFlag());
		BatchSerializer.writeBatch(out, types, this.getTuples());
	}

}
