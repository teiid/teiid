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

package com.metamatrix.common.xa;

import java.io.Serializable;
import java.math.BigInteger;

import javax.transaction.xa.Xid;

/**
 * MetaMatrix implementation of Xid.
 */
public class MMXid implements Xid, Serializable {
    private static final long serialVersionUID = -7078441828703404308L;
    
    private int formatID;
	private byte[] globalTransactionId;
	private byte[] branchQualifier;
	private String toString;
	
	public MMXid(Xid xid) {
	    this.formatID = xid.getFormatId();
	    this.globalTransactionId = xid.getGlobalTransactionId();
	    this.branchQualifier = xid.getBranchQualifier();
	}
	
	public MMXid(int formatID, byte[] globalTransactionId, byte[] branchQualifier){
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
		if(obj == null || !(obj instanceof MMXid)){
			return false;
		}
		MMXid that = (MMXid)obj;
		if(this.formatID != that.formatID){
			return false;
		}
		if(!areByteArraysEqual(this.globalTransactionId, that.globalTransactionId)){
			return false;
		}
		if(!areByteArraysEqual(this.branchQualifier, that.branchQualifier)){
			return false;
		}
		return true;
	}

	private boolean areByteArraysEqual(byte[] firstByteArray, byte[] secondByteArray){
		if(firstByteArray == null || secondByteArray == null){
			return false;
		}
		if(firstByteArray.length != secondByteArray.length){
			return false;
		}
		for(int i=0; i< firstByteArray.length; i++){
			if(firstByteArray[i] != secondByteArray[i]){
				return false;
			}
		}
		return true;
	}
	
	/** 
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
	    if (toString == null) {
    	    StringBuffer sb = new StringBuffer();
    	    
    	    sb.append("MMXid global:"); //$NON-NLS-1$
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
	
}
