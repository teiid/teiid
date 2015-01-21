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

package org.teiid.core.types;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Blob;

public final class GeometryType extends BlobType {
    public static final int UNKNOWN_SRID = 0;
    
    private int srid;

    public GeometryType() {

    }

    public GeometryType(Blob blob) {
        this(blob, UNKNOWN_SRID);
    }

    public GeometryType(byte[] bytes) {
        this(bytes, UNKNOWN_SRID);
    }

    public GeometryType(Blob blob, int srid) {
        super(blob);
        setSrid(srid);
    }

    public GeometryType(byte[] bytes, int srid) {
        super(bytes);
        setSrid(srid);
    }

    public int getSrid() {
        return srid;
    }

    public void setSrid(int srid) {
        this.srid = srid;
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    	super.writeExternal(out);
    	out.writeInt(srid);
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException,
    		ClassNotFoundException {
    	super.readExternal(in);
    	srid = in.readInt();
    }
}
