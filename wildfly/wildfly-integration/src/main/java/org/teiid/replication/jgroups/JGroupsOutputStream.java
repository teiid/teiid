/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.replication.jgroups;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;

public class JGroupsOutputStream extends OutputStream {

    static final int CHUNK_SIZE=1<<15; //need to stay under the default of 64000 for the JGroups bundling size

    protected final RpcDispatcher disp;
    protected final List<Address> dests;
    protected final Serializable stateId;
    protected final short methodOffset;

    private volatile boolean closed=false;
    private final byte[] buffer=new byte[CHUNK_SIZE];
    private int index=0;

    public JGroupsOutputStream(RpcDispatcher disp, List<Address> dests, Serializable stateId, short methodOffset, boolean sendCreate) throws IOException {
        this.disp=disp;
        this.dests=dests;
        this.stateId=stateId;
        this.methodOffset = methodOffset;
        if (sendCreate) {
            try {
                disp.callRemoteMethods(this.dests, new MethodCall(methodOffset, new Object[] {stateId}), new RequestOptions(ResponseMode.GET_NONE, 0).setAnycasting(dests != null));
            } catch(Exception e) {
                throw new IOException(e);
            }
        }
    }

    public void close() throws IOException {
        if(closed) {
            return;
        }
        flush();
        try {
            disp.callRemoteMethods(dests, new MethodCall((short)(methodOffset + 2), new Object[] {stateId}), new RequestOptions(ResponseMode.GET_NONE, 0).setAnycasting(dests != null));
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
            disp.callRemoteMethods(dests, new MethodCall((short)(methodOffset + 1), new Object[] {stateId, Arrays.copyOf(buffer, index)}), new RequestOptions(ResponseMode.GET_NONE, 0).setAnycasting(dests != null));
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