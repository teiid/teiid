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

package org.teiid.client.xa;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigInteger;
import java.util.Arrays;

import javax.transaction.xa.Xid;

/**
 * Teiid implementation of Xid.
 */
public class XidImpl implements Xid, Externalizable {
    private static final long serialVersionUID = -7078441828703404308L;
    
    private int formatID;
	private byte[] globalTransactionId;
	private byte[] branchQualifier;
	private String toString;
	
	public XidImpl() {
	}
	
	public XidImpl(Xid xid) {
	    this.formatID = xid.getFormatId();
	    this.globalTransactionId = xid.getGlobalTransactionId();
	    this.branchQualifier = xid.getBranchQualifier();
	}
	
	public XidImpl(int formatID, byte[] globalTransactionId, byte[] branchQualifier){
		this.formatID = formatID;
		this.globalTransactionId = globalTransactionId;
		this.branchQualifier = branchQualifier;
	}
	
	/**
	 * @see javax.transaction.xa.Xid#getFormatId()
	 */
	public int getFormatId() {
		return formatID;
	}

	/**
	 * @see javax.transaction.xa.Xid#getGlobalTransactionId()
	 */
	public byte[] getGlobalTransactionId() {
		return globalTransactionId;
	}

	/**
	 * @see javax.transaction.xa.Xid#getBranchQualifier()
	 */
	public byte[] getBranchQualifier() {
		return branchQualifier;
	}
	
	public boolean equals(Object obj){
        if(obj == this) {
            return true;
        } 
		if(!(obj instanceof XidImpl)){
			return false;
		}
		XidImpl that = (XidImpl)obj;
		return this.formatID == that.formatID
				&& Arrays.equals(this.globalTransactionId, that.globalTransactionId)
				&& Arrays.equals(this.branchQualifier, that.branchQualifier);
	}
	
	/** 
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
	    if (toString == null) {
    	    StringBuffer sb = new StringBuffer();
    	    
    	    sb.append("Teiid-Xid global:"); //$NON-NLS-1$
            sb.append(getByteArrayString(globalTransactionId));
            sb.append(" branch:"); //$NON-NLS-1$
            sb.append(getByteArrayString(branchQualifier));
            sb.append(" format:"); //$NON-NLS-1$
            sb.append(getFormatId());
            toString = sb.toString();
	    }
        return toString;
	}
	
	static String getByteArrayString(byte[] bytes) {
	    if (bytes == null || bytes.length == 0) {
	        return null;
	    }
	    
	    return new BigInteger(bytes).toString();
	}
	
	/** 
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
	    return toString().hashCode();
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.formatID = in.readInt();
		this.globalTransactionId = (byte[])in.readObject();
		this.branchQualifier = (byte[])in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(this.formatID);
		out.writeObject(this.globalTransactionId);
		out.writeObject(this.branchQualifier);
	}
	
}
