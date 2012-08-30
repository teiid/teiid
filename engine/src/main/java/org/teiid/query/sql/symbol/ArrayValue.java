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

package org.teiid.query.sql.symbol;

import java.io.Serializable;
import java.util.Arrays;

import org.teiid.query.sql.visitor.SQLStringVisitor;

public class ArrayValue implements Comparable<ArrayValue>, Serializable {
	private static final long serialVersionUID = 517794153664734815L;
	private Object[] values;
	
	@SuppressWarnings("serial")
	public final static class NullException extends RuntimeException {};
	private final static NullException ex = new NullException();
	
	public ArrayValue(Object[] values) {
		this.values = values;
	}

	@Override
	public int compareTo(ArrayValue o) {
		return compareTo(o, false);
	}
		
	public int compareTo(ArrayValue o, boolean noNulls) {
		int len1 = values.length;
		int len2 = o.values.length;
	    int lim = Math.min(len1, len2);
	    for (int k = 0; k < lim; k++) {
	    	Object object1 = values[k];
			Object object2 = o.values[k];
			if (object1 == null) {
				if (noNulls) {
					throw ex;
				}
	    		if (object2 != null) {
	    			return -1;
	    		}
	    		continue;
	    	} else if (object2 == null) {
	    		if (noNulls) {
					throw ex;
				}
	    		return 1;
	    	}
			int comp = Constant.COMPARATOR.compare(object1, object2);
			if (comp != 0) {
			    return comp;
			}
	    }
	    return len1 - len2;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(values);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof ArrayValue)) {
			return false;
		}
		ArrayValue other = (ArrayValue)obj;
		return Arrays.equals(values, other.values);
	}
	
	public Object[] getValues() {
		return values;
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(new Constant(this));
	}
	
}
