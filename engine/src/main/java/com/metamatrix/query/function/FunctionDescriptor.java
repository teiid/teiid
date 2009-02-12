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

package com.metamatrix.query.function;

import java.io.Serializable;
import java.lang.reflect.Method;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.core.util.Assertion;

/**
 * The FunctionDescriptor describes a particular function instance enough
 * that the function library can retrieve a function instance based on the 
 * descriptor.
 */
public class FunctionDescriptor implements Serializable, Cloneable {
	
	private String name;
    private int pushdown;
	private Class[] types;
	private Class returnType;	
	private int hash;
    private boolean requiresContext;
    private boolean nullDependent;
    private int deterministic;
    
    // This is transient as it would be useless to invoke this method in 
    // a different VM.  This function descriptor can be used to look up 
    // the real VM descriptor for execution.
    private transient Method invocationMethod;
	
    FunctionDescriptor() {
    }
    
    /** 
     * Construct a function descriptor with all the info
     * @param name Name of function
     * @param types Types of the arguments
     * @param returnType Type of the return 
     * @param invocationMethod Reflection method used to invoke the function
     * @param requiresContext during execution requires command context to be pushed into method as first argument
     */
	FunctionDescriptor(String name, int pushdown, Class[] types, Class returnType, Method invocationMethod, boolean requiresContext, boolean nullDependent, int deterministic) {
		Assertion.isNotNull(name);
		Assertion.isNotNull(types);
		Assertion.isNotNull(returnType);
		
		this.name = name;
        this.pushdown = pushdown;
		this.types = types;
		this.returnType = returnType;
        this.invocationMethod = invocationMethod;
        this.requiresContext = requiresContext;
        this.nullDependent = nullDependent;
        this.deterministic = deterministic;
		
		// Compute hash code
		hash = HashCodeUtil.hashCode(0, name);
		for(int i=0; i<types.length; i++) {
			hash = HashCodeUtil.hashCode(hash, types[i]);
		}
	}

	public String getName() {
		return this.name;				
	}
    
    public int getPushdown() {
        return this.pushdown;
    }
    
    void setPushdown(int pushdown) {
        this.pushdown = pushdown;
    }
	
	public Class[] getTypes() {
		return this.types;
	}
	
	public Class getReturnType() {
		return this.returnType;
	}		
	
    Method getInvocationMethod() {
        return this.invocationMethod;
    }
       
    public boolean requiresContext() {
        return this.requiresContext;
    }
    
	public int hashCode() { 
		return this.hash;
	}
	
	public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}
		
		if(obj == null || !(obj instanceof FunctionDescriptor)) {
			return false;			
		}	
		FunctionDescriptor other = (FunctionDescriptor) obj;
		
		// Compare names
		if(! this.getName().equals(other.getName())) {
			return false;
		}
        
        // Compare arg types
		Class[] thisTypes = this.getTypes();
		Class[] otherTypes = other.getTypes();
		if(thisTypes.length != otherTypes.length) {
			return false;
		}
		for(int i=0; i<thisTypes.length; i++) { 
			if(! thisTypes[i].equals(otherTypes[i])) {
				return false;
			}
		}
        
        if (this.nullDependent != other.isNullDependent()) {
            return false;
        }
        
        if (this.deterministic != other.deterministic) {
            return false;
        }
		 
		// Must be a match
		return true;
	}
	
	public String toString() {
		StringBuffer str = new StringBuffer(this.name);
		str.append("("); //$NON-NLS-1$
		for(int i=0; i<types.length; i++) {
			if(types[i] != null) { 
				str.append(types[i].getName());
			} else {
				str.append("null"); //$NON-NLS-1$
			}		
			if(i<(types.length-1)) {
				str.append(", "); //$NON-NLS-1$
			}
		}
		str.append(") : "); //$NON-NLS-1$
        if(returnType == null) { 
            str.append("null"); //$NON-NLS-1$
        } else {    
    		str.append(returnType.getName());
        }    
		return str.toString();
	}

    public boolean isNullDependent() {
        return nullDependent;
    }
    
    public int getDeterministic() {
        return deterministic;
    }

    void setDeterministic(int deterministic) {
        this.deterministic = deterministic;
    }
    
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    void setReturnType(Class returnType) {
        this.returnType = returnType;
    }
    
}
