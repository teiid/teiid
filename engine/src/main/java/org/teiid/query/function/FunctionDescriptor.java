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

package org.teiid.query.function;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;


/**
 * The FunctionDescriptor describes a particular function instance enough
 * to invoke the function.
 */
public class FunctionDescriptor implements Serializable, Cloneable {
	private static final long serialVersionUID = 5374103983118037242L;

	private static final boolean ALLOW_NAN_INFINITY = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.allowNanInfinity", false); //$NON-NLS-1$
	
	private Class<?>[] types;
	private Class<?> returnType;	
    private boolean requiresContext;
    private FunctionMethod method;
    private String schema; //TODO: remove me - we need to create a proper schema for udf and system functions
    private Object metadataID;
    
    // This is transient as it would be useless to invoke this method in 
    // a different VM.  This function descriptor can be used to look up 
    // the real VM descriptor for execution.
    private transient Method invocationMethod;
	
    FunctionDescriptor() {
    }
    
	FunctionDescriptor(FunctionMethod method, Class<?>[] types,
			Class<?> outputType, Method invocationMethod,
			boolean requiresContext) {
		this.types = types;
		this.returnType = outputType;
        this.invocationMethod = invocationMethod;
        this.requiresContext = requiresContext;
        this.method = method;
	}
	
	public String getSchema() {
		return schema;
	}
	
	public void setSchema(String schema) {
		this.schema = schema;
	}
	
	public String getName() {
		return this.method.getName();				
	}
    
    public PushDown getPushdown() {
        return this.method.getPushdown();
    }
    
	public Class<?>[] getTypes() {
		return this.types;
	}
	
	public Class<?> getReturnType() {
		return this.returnType;
	}		
	
    Method getInvocationMethod() {
        return this.invocationMethod;
    }
       
    public boolean requiresContext() {
        return this.requiresContext;
    }
    
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer(this.method.getName());
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
        return !this.method.isNullOnNull();
    }
    
    public Determinism getDeterministic() {
        return this.method.getDeterminism();
    }

    @Override
	public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new TeiidRuntimeException(e);
        }
    }
    
    public FunctionMethod getMethod() {
		return method;
	}

    void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

	public Object getMetadataID() {
		return this.metadataID;
	}

	public void setMetadataID(Object metadataID) {
		this.metadataID = metadataID;
	}

	/**
	 * Invoke the function described in the function descriptor, using the
	 * values provided.  Return the result of the function.
	 * @param fd Function descriptor describing the name and types of the arguments
	 * @param values Values that should match 1-to-1 with the types described in the
	 * function descriptor
	 * @return Result of invoking the function
	 */
	public Object invokeFunction(Object[] values) throws FunctionExecutionException {

        if (!isNullDependent()) {
        	for (int i = 0; i < values.length; i++) {
				if (values[i] == null) {
					return null;
				}
			}
        }

        // If descriptor is missing invokable method, find this VM's descriptor
        // give name and types from fd
        Method method = getInvocationMethod();
        if(method == null) {
        	throw new FunctionExecutionException("ERR.015.001.0002", QueryPlugin.Util.getString("ERR.015.001.0002", getName())); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        // Invoke the method and return the result
        try {
        	if (method.isVarArgs()) {
        		int i = method.getParameterTypes().length;
        		Object[] newValues = Arrays.copyOf(values, i);
        		newValues[i - 1] = Arrays.copyOfRange(values, i - 1, values.length);
        		values = newValues;
        	}
            Object result = method.invoke(null, values);
            return importValue(result, getReturnType());
        } catch(ArithmeticException e) {
    		throw new FunctionExecutionException(e, "ERR.015.001.0003", QueryPlugin.Util.getString("ERR.015.001.0003", getName())); //$NON-NLS-1$ //$NON-NLS-2$
        } catch(InvocationTargetException e) {
            throw new FunctionExecutionException(e.getTargetException(), "ERR.015.001.0003", QueryPlugin.Util.getString("ERR.015.001.0003", getName())); //$NON-NLS-1$ //$NON-NLS-2$
        } catch(IllegalAccessException e) {
            throw new FunctionExecutionException(e, "ERR.015.001.0004", QueryPlugin.Util.getString("ERR.015.001.0004", method.toString())); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (TransformationException e) {
        	throw new FunctionExecutionException(e, e.getMessage());
		}
	}

	public static Object importValue(Object result, Class<?> expectedType)
			throws ArithmeticException, TransformationException {
		if (!ALLOW_NAN_INFINITY) {
			if (result instanceof Double) {
		    	Double floatVal = (Double)result;
		    	if (Double.isInfinite(floatVal) || Double.isNaN(floatVal)) {
		    		throw new ArithmeticException("Infinite or invalid result");  //$NON-NLS-1$
		    	}
		    } else if (result instanceof Float) {
		    	Float floatVal = (Float)result;
		    	if (Float.isInfinite(floatVal) || Float.isNaN(floatVal)) {
		    		throw new ArithmeticException("Infinite or invalid result");  //$NON-NLS-1$
		    	}
		    }
		}
		result = DataTypeManager.convertToRuntimeType(result);
		result = DataTypeManager.transformValue(result, expectedType);
		if (result instanceof String) {
			String s = (String)result;
			if (s.length() > DataTypeManager.MAX_STRING_LENGTH) {
				return s.substring(0, DataTypeManager.MAX_STRING_LENGTH);
			}
		}
		return result;
	}    
}
