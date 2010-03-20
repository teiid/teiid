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

package org.teiid.client.metadata;

import java.io.*;
import java.io.Externalizable;

/**
 * Desccribes some parameter info to return when executing a CallableStatement -
 * this is used to avoid sending SPParameter, which contains references to metadata
 * objects and stuff we don't want to send.  
 */
public class ParameterInfo implements Externalizable {

    static final long serialVersionUID = -683851729051138932L;
    
    private int type;           // used outbound
    private int numColumns;     // if type is a result set - used outbound

	/** Constant identifying an IN parameter */
	public static final int IN = 1;

	/** Constant identifying an OUT parameter */
	public static final int OUT = 2;

	/** Constant identifying an INOUT parameter */
	public static final int INOUT = 3;

	/** Constant identifying a RETURN parameter */
	public static final int RETURN_VALUE = 4;

	/** Constant identifying a RESULT SET parameter */
	public static final int RESULT_SET = 5;

    // needed for Externalizable
    public ParameterInfo() {
    }

    /**
     * Create outbound parameter info
     * @param type
     * @param numColumns
     */
    public ParameterInfo(int type, int numColumns) {
        this.type = type;
        this.numColumns = numColumns;
    }
    
    public int getType() {
        return this.type;
    }
    
    public int getNumColumns() {
        return this.numColumns;
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(type);
        out.writeInt(numColumns);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = in.readInt();
        numColumns = in.readInt();
    }
    
    

}
