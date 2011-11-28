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

package org.teiid.replication.jboss;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Vector;

import org.jgroups.Address;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.teiid.core.types.Streamable;

public class JGroupsOutputStream extends OutputStream {
	
	static final int CHUNK_SIZE=Streamable.STREAMING_BATCH_SIZE_IN_BYTES;
    
	protected final RpcDispatcher disp;
    protected final Vector<Address> dests;
    protected final String stateId;
    protected final short methodOffset;
	
    private volatile boolean closed=false;
    private final byte[] buffer=new byte[CHUNK_SIZE];
    private int index=0;

    public JGroupsOutputStream(RpcDispatcher disp, Vector<Address> dests, String stateId, short methodOffset) {
        this.disp=disp;
        this.dests=dests;
        this.stateId=stateId;
        this.methodOffset = methodOffset;
    	disp.callRemoteMethods(this.dests, new MethodCall(methodOffset, new Object[] {stateId}), GroupRequest.GET_NONE, 0, dests != null);
    }

    public void close() throws IOException {
        if(closed) {
            return;
        }
        flush();
        try {
        	disp.callRemoteMethods(dests, new MethodCall((short)(methodOffset + 2), new Object[] {stateId}), GroupRequest.GET_NONE, 0, dests != null);
        } catch(Exception e) {
        }
        closed=true;
    }

    public void flush() throws IOException {
        checkClosed();
        try {
            if(index == 0) {
                return;
            }
        	disp.callRemoteMethods(dests, new MethodCall((short)(methodOffset + 1), new Object[] {stateId, Arrays.copyOf(buffer, index)}), GroupRequest.GET_NONE, 0, dests != null);
            index=0;
        } catch(Exception e) {
        	throw new IOException(e);
        }
    }

	private void checkClosed() throws IOException {
		if(closed) {
            throw new IOException("output stream is closed"); //$NON-NLS-1$
		}
	}

    public void write(int b) throws IOException {
        checkClosed();
        if(index >= buffer.length) {
            flush();
        }
        buffer[index++]=(byte)b;
    }

}