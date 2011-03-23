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

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents a function in a sql statement.  A function is a type of expression.  
 * Functions have a name and some arguments (0..n).  Each argument is also 
 * an expression.  After resolution, a function should have a type and a function
 * descriptor.
 */
public class Function implements Expression {

	private String name;
	private Expression[] args;
	private Class type;
	private FunctionDescriptor descriptor;
	private boolean implicit = false;
	
	/**
	 * Construct a function with function name and array of arguments.  For 
	 * functions that have no args, pass empty array, not null.
	 * @param name Name of function
	 * @param args Function arguments
	 */	
	public Function(String name, Expression[] args) {
		this.name = name;
		this.args = args;
	}
	
	/**
	 * Get name of function
	 * @return Name of function
	 */
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Get function arguments
	 * @return Get function arguments
	 */
	public Expression[] getArgs() {
		return this.args;
	}
	
	/**
	 * Get argument at specified index
	 * @param index Index of argument
	 */
	public Expression getArg(int index) {
		return this.args[index];
	}

	/**
	 * Set the function arguments - it is assumed that the args 
	 * are not null.  For no arg functions, use an empty Expression array.
	 * @param args Function arguments
	 */
	public void setArgs(Expression[] args) { 
		this.args = args;
	}

	/**
	 * Make this function implicit / hidden.
	 */
	public void makeImplicit() { 
		this.implicit = true;
	}
	
	/**
	 * Return true if this function is implicit and should not be shown in SQL representations
	 * @return True if implicit
	 */
	public boolean isImplicit() { 
		return this.implicit;
	}
	
	/** 
	 * Insert a conversion function at specified index.  This is a convenience 
	 * method to insert a conversion into the function tree.
	 * @param index Argument index to insert conversion function at
	 * @param functionDescriptor Conversion function descriptor
	 */
	public void insertConversion(int index, FunctionDescriptor functionDescriptor) { 
		// Get target type for conversion
		Class type = functionDescriptor.getReturnType();
		String typeName = DataTypeManager.getDataTypeName(type);
		
		// Pull old expression at index
		Expression newArg[] = new Expression[] { args[index], new Constant(typeName) };
		
		// Replace old expression with new expression, using old as arg
		Function func = new Function(functionDescriptor.getName(), newArg);
		args[index] = func;
		
		// Set function descriptor and type of new function
		func.setFunctionDescriptor(functionDescriptor);
		func.setType(type);
		func.makeImplicit();
	}

	/**
	 * Get type of function, if known
	 * @return Java class name of type, or null if not yet resolved
	 */
	public Class getType() {
		return this.type;
	}

	/**
	 * Set type of function
	 * @param type New type
	 */
	public void setType(Class type) { 
		this.type = type;
	}
	
	/**
	 * Get the function descriptor that this function resolves to.
	 * @return Descriptor or null if resolution has not yet occurred
	 */
	public FunctionDescriptor getFunctionDescriptor() {
		return this.descriptor;
	}
	
	/**
	 * Set the descriptor for this function.
	 * @param fd Function descriptor
	 */
	public void setFunctionDescriptor(FunctionDescriptor fd) {
		this.descriptor = fd;
	}
			
	/**
	 * Return true if expression has been fully resolved.  Typically the QueryResolver component
	 * will handle resolution of an expression.
	 * @return True if resolved
	 */
	public boolean isResolved() { 
		boolean resolved = true;
		for(int i=0; i<args.length; i++) {
			if(! args[i].isResolved()) {
				resolved = false;
			}				
		}
		return resolved;
	}
	
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

	/**
	 * Compare for equality
	 * @param obj Other object to compare
	 * @return Return true if objects are equivalent
	 */
	public boolean equals(Object obj) { 
		if(this == obj) { 
			return true;
		} 
		
		if(! (obj instanceof Function)) { 
			return false;			
		}
		
		Function other = (Function) obj;
		if (this.descriptor != null && other.descriptor != null) {
			if (!this.descriptor.getMethod().equals(other.descriptor.getMethod())) {
				return false;
			}
		} else if(! other.getName().equalsIgnoreCase(getName())) { 
			return false;
		}	
		
		if (this.isImplicit() != other.isImplicit()) {
		    return false;
		}
		
        Expression[] otherArgs = other.getArgs();
        Expression[] thisArgs = getArgs();
	
		return EquivalenceUtil.areEquivalent(thisArgs, otherArgs);
	}
	
    /**
     * Compute hash code for the object - based on name and hashcode of first arg (if there is one)
     * @return Hash code
     */
    public int hashCode() { 
        int hashCode = HashCodeUtil.hashCode(0, this.getName().toUpperCase());
        
        Expression[] thisArgs = getArgs();
        if(thisArgs != null && thisArgs.length > 0 && thisArgs[0] != null) { 
            hashCode = HashCodeUtil.hashCode(hashCode, thisArgs[0].hashCode());
        }
        
        return hashCode;
    }
    
	/**
	 * Return a deep copy of this object.
	 * @return Deep copy of the object
	 */
	public Object clone() {	
		Expression[] args = getArgs();
		Expression[] copyArgs = new Expression[args.length];
		for(int i=0; i<args.length; i++) { 
            if(args[i] != null) {
    			copyArgs[i] = (Expression) args[i].clone();
            }
		}
	
		Function copy = new Function(getName(), copyArgs);
		copy.setType(getType());
		copy.setFunctionDescriptor(getFunctionDescriptor());
		
		if(this.isImplicit()) { 
			copy.makeImplicit();
		}	
		
		return copy;	
	}
	
	/**
	 * Return string representation of the function.
	 * @return String representation
	 */
	public String toString() { 
		return SQLStringVisitor.getSQLString(this);    
	}	

}
