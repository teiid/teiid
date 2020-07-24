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

package org.teiid.query.function;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.client.SourceWarning;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.Procedure;
import org.teiid.query.QueryPlugin;
import org.teiid.query.util.CommandContext;


/**
 * The FunctionDescriptor describes a particular function instance enough
 * to invoke the function.
 */
public class FunctionDescriptor implements Serializable, Cloneable {
    private static final long serialVersionUID = 5374103983118037242L;

    private static final boolean ALLOW_NAN_INFINITY = PropertiesUtils.getHierarchicalProperty("org.teiid.allowNanInfinity", false, Boolean.class); //$NON-NLS-1$

    private Class<?>[] types;
    private Class<?> returnType;
    private boolean requiresContext;
    private FunctionMethod method;
    private String schema; //TODO: remove me - we need to create a proper schema for udf and system functions
    private boolean hasWrappedArgs;

    // This is transient as it would be useless to invoke this method in
    // a different VM.  This function descriptor can be used to look up
    // the real VM descriptor for execution.
    private transient Method invocationMethod;

    private ClassLoader classLoader;

    private Procedure procedure;

    FunctionDescriptor() {
    }

    FunctionDescriptor(FunctionMethod method, Class<?>[] types,
            Class<?> outputType, Method invocationMethod,
            boolean requiresContext, ClassLoader classloader) {
        this.types = types;
        this.returnType = outputType;
        this.invocationMethod = invocationMethod;
        this.requiresContext = requiresContext;
        this.method = method;
        this.classLoader = classloader;
    }

    public Object newInstance() throws FunctionExecutionException {
        checkMethod();
        try {
            return invocationMethod.getDeclaringClass().newInstance();
        } catch (InstantiationException e) {
            throw new MetadataException(QueryPlugin.Event.TEIID30602, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30602, method.getName(), method.getInvocationClass()));
        } catch (IllegalAccessException e) {
            throw new MetadataException(QueryPlugin.Event.TEIID30602, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30602, method.getName(), method.getInvocationClass()));
        }
    }

    public void setHasWrappedArgs(boolean hasWrappedArgs) {
        this.hasWrappedArgs = hasWrappedArgs;
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

    public String getFullName() {
        if (CoreConstants.SYSTEM_MODEL.equals(this.schema)) {
            return getName();
        }
        return this.schema + AbstractMetadataRecord.NAME_DELIM_CHAR + getName();
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

    public Procedure getProcedure() {
        return procedure;
    }

    public void setProcedure(Procedure procedure) {
        this.procedure = procedure;
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
    public FunctionDescriptor clone() {
        try {
            return (FunctionDescriptor) super.clone();
        } catch (CloneNotSupportedException e) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30381, e);
        }
    }

    public FunctionMethod getMethod() {
        return method;
    }

    void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    /**
     * Invoke the function described in the function descriptor, using the
     * values provided.  Return the result of the function.
     * @param values Values that should match 1-to-1 with the types described in the
     * function descriptor
     * @param context
     * @param functionTarget the object to invoke the function on
     * @return Result of invoking the function
     */
    public Object invokeFunction(Object[] values, CommandContext context, Object functionTarget) throws FunctionExecutionException, BlockedException {
        return invokeFunction(values, context, functionTarget, false);
    }

    public Object invokeFunction(Object[] values, CommandContext context, Object functionTarget, boolean calledWithVarArgArrayParam) throws FunctionExecutionException, BlockedException {
        if (!isNullDependent()) {
            for (int i = requiresContext?1:0; i < values.length; i++) {
                if (values[i] == null) {
                    return null;
                }
            }
        }

        checkMethod();

        // Invoke the method and return the result
        try {
            if (hasWrappedArgs) {
                for (int i = 0; i < values.length; i++) {
                    Object val = values[i];
                    if (val != null && types[i] == DataTypeManager.DefaultDataClasses.VARBINARY) {
                        values[i] = ((BinaryType)val).getBytesDirect();
                    }
                }
            }
            if (method.isVarArgs()) {
                if (calledWithVarArgArrayParam) {
                    ArrayImpl av = (ArrayImpl)values[values.length -1];
                    if (av != null) {
                        Object[] vals = av.getValues();
                        values[values.length - 1] = vals;
                        if (hasWrappedArgs && types[types.length - 1] == DataTypeManager.DefaultDataClasses.VARBINARY) {
                            vals = Arrays.copyOf(vals, vals.length);
                            for (int i = 0; i < vals.length; i++) {
                                if (vals[i] != null) {
                                    vals[i] = ((BinaryType)vals[i]).getBytesDirect();
                                }
                            }
                            values[values.length - 1] = vals;
                        }
                        Class<?> arrayType = invocationMethod.getParameterTypes()[types.length - 1];
                        if (arrayType.getComponentType() != Object.class
                                && vals.getClass() != arrayType) {
                            Object varArgs = Array.newInstance(arrayType.getComponentType(), vals.length);
                            for (int i = 0; i < vals.length; i++) {
                                Array.set(varArgs, i, vals[i]);
                            }
                            values[values.length -1] = varArgs;
                        }
                    }
                } else {
                    int i = invocationMethod.getParameterTypes().length;
                    Object[] newValues = Arrays.copyOf(values, i);
                    Object varArgs = null;
                    if (invocationMethod.getParameterTypes()[i - 1].getComponentType() != Object.class) {
                        int varArgCount = values.length - i + 1;
                        varArgs = Array.newInstance(invocationMethod.getParameterTypes()[i - 1].getComponentType(), varArgCount);
                        for (int j = 0; j < varArgCount; j++) {
                            Array.set(varArgs, j, values[i-1+j]);
                        }
                    } else {
                        varArgs = Arrays.copyOfRange(values, i - 1, values.length);
                    }
                    newValues[i - 1] = varArgs;
                    values = newValues;
                }
            }
            Object result = null;
            ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
            try {
                if (this.classLoader != null) {
                    Thread.currentThread().setContextClassLoader(this.classLoader);
                }
                result = invocationMethod.invoke(functionTarget, values);
            } finally {
                Thread.currentThread().setContextClassLoader(originalCL);
            }
            if (context != null && getDeterministic().ordinal() <= Determinism.USER_DETERMINISTIC.ordinal()) {
                context.setDeterminismLevel(getDeterministic());
            }
            return importValue(result, getReturnType(), context);
        } catch(ArithmeticException e) {
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30384, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30384, getFullName()));
        } catch(InvocationTargetException e) {
             if (e.getTargetException() instanceof BlockedException) {
                 throw (BlockedException)e.getTargetException();
             }
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30384, e.getTargetException(), QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30384, getFullName()));
        } catch(IllegalAccessException e) {
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30385, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30385, method.toString()));
        } catch (TransformationException e) {
             throw new FunctionExecutionException(e);
        }
    }

    private void checkMethod() throws FunctionExecutionException {
        // If descriptor is missing invokable method, find this VM's descriptor
        // give name and types from fd
        if(invocationMethod == null) {
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30382, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30382, getFullName()));
        }
    }

    public static Object importValue(Object result, Class<?> expectedType, CommandContext context)
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
        result = DataTypeManager.convertToRuntimeType(result, expectedType != DataTypeManager.DefaultDataClasses.OBJECT);
        if (expectedType.isArray() && result instanceof ArrayImpl) {
            return result;
        }
        result = DataTypeManager.transformValue(result, expectedType);
        if (result != null && expectedType == DataTypeManager.DefaultDataClasses.STRING) {
            String s = (String)result;
            if (s.length() > DataTypeManager.MAX_STRING_LENGTH) {
                if (context != null) {
                    TeiidProcessingException warning = new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31274, s.length(), DataTypeManager.MAX_STRING_LENGTH));
                    warning.setStackTrace(SourceWarning.EMPTY_STACK_TRACE);
                    context.addWarning(warning);
                }
                return s.substring(0, DataTypeManager.MAX_STRING_LENGTH);
            }
        }
        return result;
    }

    public boolean isSystemFunction(String name) {
        return this.getName().equalsIgnoreCase(name) && CoreConstants.SYSTEM_MODEL.equals(this.getSchema());
    }

}
