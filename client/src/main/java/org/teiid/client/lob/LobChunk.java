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

package org.teiid.client.lob;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/** 
 * A Lob Chunk object which carries information packets in byte streams. This 
 * class used as value object to transfer blob object's data chunk back and forth
 * between the client and server. 
 */
public class LobChunk implements Externalizable {
    static final long serialVersionUID = -5634014429424520672L;
    
    private byte[] data;
    private boolean last = false;
    
    public LobChunk() {
    	
    }
    
    public LobChunk(byte[] data, boolean last){
        this.last = last;
        this.data = data;
    }
           
    public byte[] getBytes() {
        return this.data;
    } 
    
    public boolean isLast() {
        return this.last;
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException,
    		ClassNotFoundException {
    	data = (byte[])in.readObject();
    	last = in.readBoolean();
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    	out.writeObject(data);
    	out.writeBoolean(last);
    }

}
