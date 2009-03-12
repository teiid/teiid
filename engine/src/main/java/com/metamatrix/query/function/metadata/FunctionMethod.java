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

package com.metamatrix.query.function.metadata;

import java.io.Serializable;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * <p>This class represents information about a particular function signature.  
 * Function signatures are unique with respect to their name, # of arguments, 
 * and type of arguments. Return type and argument names are not uniqueness
 * factors.  This class makes no attempt to validate the data put into it, 
 * particularly with respect to null values.  The 
 * {@link FunctionMetadataValidator} can be used to validate this object.</p>
 *
 * Attributes:
 * <UL>
 * <LI>name - Name of the function</LI>
 * <LI>description - Description of the function</LI>
 * <LI>category - Function category containing this function</LI>
 * <LI>pushdown - Determine whether this function can, cannot, or must be pushed down to a source</LI>
 * <LI>invocationClass - Class containing method implementing this function</LI>
 * <LI>invocationMethod - Method implementing this function</LI>
 * <LI>inputParameters - 0 or more input parameters</LI>
 * <LI>outputParameter - 1 output parameter</LI>
 * </UL>
 *
 * @see FunctionParameter
 * @see FunctionMetadataValidator
 * @see FunctionCategoryConstants
 */
public class FunctionMethod implements Serializable {

    public static final int CAN_PUSHDOWN = 0;
    public static final int CANNOT_PUSHDOWN = 1;
    public static final int MUST_PUSHDOWN = 2;
    public static final int SYNTHETIC = 3;
    
    /*
     * always -> normal deterministic functions
     * server lifetime -> lookup (however lookup values can be flushed at any time)
     * session -> env, user
     * command -> command payload
     * never -> rand, etc.
     * 
     * Anything at a session level and above is treated as deterministic.
     * This is not quite correct for lookup or env.  Only in extremely rare
     * circumstances would that be a problem.
     * 
     * For now the commandPayload function is treated as a special case, like lookup, in
     * that it is considered deterministic, but will be delayed in its evaluation until
     * processing time.
     */
    public static final int DETERMINISTIC = 0;
    public static final int SERVER_DETERMINISTIC = 1;
    public static final int SESSION_DETERMINISTIC = 2;
    public static final int COMMAND_DETERMINISTIC = 3;
    public static final int NONDETERMINISTIC = 4;
    

    private String name;
    private String description;
    private String category;
    private int pushdown = CAN_PUSHDOWN;
    private String invocationClass;
    private String invocationMethod;
    private boolean nullDependent;
    
    private int deterministic = DETERMINISTIC;
        
    private FunctionParameter[] inputParameters;
    private FunctionParameter outputParameter;
    
    /**
     * Construct a new empty function method.
     */
    public FunctionMethod() {
    }

    /**
     * Construct a function method with default pushdown and null dependent attributes.
     * @param name Function name
     * @param description Function description
     * @param category Function category
     * @param invocationClass Invocation class
     * @param invocationMethod Invocation method
     * @param inputParams Input parameters
     * @param outputParam Output parameter (return parameter)
     */
    public FunctionMethod(String name, String description, String category, 
        String invocationClass, String invocationMethod, 
        FunctionParameter[] inputParams, FunctionParameter outputParam) {
        
        this(name, description, category, invocationClass, invocationMethod, inputParams, outputParam, DETERMINISTIC);
    }
    
    /**
     * Construct a function method with default pushdown and null dependent attributes.
     * @param name Function name
     * @param description Function description
     * @param category Function category
     * @param invocationClass Invocation class
     * @param invocationMethod Invocation method
     * @param inputParams Input parameters
     * @param outputParam Output parameter (return parameter)
     */
    public FunctionMethod(String name, String description, String category, 
        String invocationClass, String invocationMethod, 
        FunctionParameter[] inputParams, FunctionParameter outputParam, int deterministic) {
        
        this(name, description, category, CAN_PUSHDOWN, invocationClass, invocationMethod, inputParams, outputParam, false, deterministic);
    }

    /**
     * Construct a function method with all parameters assuming null dependent and non-deterministic.
     * @param name Function name
     * @param description Function description
     * @param category Function category
     * @param invocationClass Invocation class
     * @param invocationMethod Invocation method
     * @param inputParams Input parameters
     * @param outputParam Output parameter (return parameter)
     */
    public FunctionMethod(String name, String description, String category, 
        int pushdown, String invocationClass, String invocationMethod, 
        FunctionParameter[] inputParams, FunctionParameter outputParam) {
        
        this(name, description, category, pushdown, invocationClass, invocationMethod, inputParams, outputParam, true, NONDETERMINISTIC);
    }
    
    /**
     * Construct a function method with all parameters.
     * @param name Function name
     * @param description Function description
     * @param category Function category
     * @param invocationClass Invocation class
     * @param invocationMethod Invocation method
     * @param inputParams Input parameters
     * @param outputParam Output parameter (return parameter)
     */
    public FunctionMethod(String name,
                          String description,
                          String category,
                          int pushdown,
                          String invocationClass,
                          String invocationMethod,
                          FunctionParameter[] inputParams,
                          FunctionParameter outputParam,
                          boolean nullDependent,
                          int deterministic) {
        
        setName(name);
        setDescription(description);
        setCategory(category);
        setPushdown(pushdown);
        setInvocationClass(invocationClass);
        setInvocationMethod(invocationMethod);
        setInputParameters(inputParams);
        setOutputParameter(outputParam); 
        setNullDependent(nullDependent);
        setDeterministic(deterministic);
    }
    
    /**
     * Return name of method
     * @return Name
     */
    public String getName() {
        return this.name;
    }
    
    /**
     * Set name of method
     * @param name Name
     */
    public void setName(String name) { 
        this.name = name;
    }
    
    /**
     * Get description of method
     * @return Description
     */
    public String getDescription() { 
        return this.description;
    }        
    
    /**
     * Set description of method
     * @param description Description
     */
    public void setDescription(String description) { 
        this.description = description;
    }

    /**
     * Get category of method
     * @return Category
     * @see FunctionCategoryConstants
     */
    public String getCategory() { 
        return this.category;
    }        
    
    /**
     * Set category of method
     * @param category Category
     * @see FunctionCategoryConstants
     */
    public void setCategory(String category) { 
        this.category = category;
    }
    
    /**
     * Get pushdown property of method
     * @return One of the FunctionMethod constants for pushdown
     */
    public int getPushdown() {
        return pushdown;
    }

    /**
     * Set pushdown property of method
     * @param pushdown One of the FunctionMethod constants for pushdown
     */
    public void setPushdown(int pushdown) {
        this.pushdown = pushdown;
    }
    
    /**
     * Get invocation class name
     * @return Invocation class name
     */
    public String getInvocationClass() { 
        return this.invocationClass;
    }        
    
    /**
     * Set invocation class name
     * @param invocationClass Invocation class name
     */
    public void setInvocationClass(String invocationClass) { 
        this.invocationClass = invocationClass;
    }
    
    /**
     * Get invocation method name
     * @return Invocation method name
     */
    public String getInvocationMethod() { 
        return this.invocationMethod;
    }        
    
    /**
     * Set invocation method name
     * @param invocationMethod Invocation method name
     */
    public void setInvocationMethod(String invocationMethod) { 
        this.invocationMethod = invocationMethod;
    }
    
    /**
     * Get a count of the input parameters.
     * @return Number of input parameters
     */
    public int getInputParameterCount() {
        if(this.inputParameters == null) { 
            return 0;
        }
        return this.inputParameters.length;
    }
    
    /**
     * Get input parameters
     * @return Array of input parameters, may be null if 0 parameters
     */
    public FunctionParameter[] getInputParameters() { 
        return this.inputParameters;
    }
    
    /**
     * Set input parameters.
     * @param params Input parameters
     */
    public void setInputParameters(FunctionParameter[] params) { 
        this.inputParameters = params;
    }
    
    /**
     * Get ouput parameter.
     * @return Output parameter or return argument
     */
    public FunctionParameter getOutputParameter() { 
        return this.outputParameter;
    }
    
    /**
     * Set ouput parameter.
     * @param param Output Parameter
     */
    public void setOutputParameter(FunctionParameter param) { 
        this.outputParameter = param;
    }
    
    /**
     * Get hash code for this object.  The hash code is based on the name 
     * and input parameters.  <B>WARNING: Changing the name or input parameters
     * will change the hash code.</B>  If this occurs after the object has been
     * placed in a HashSet or HashMap, the object will be lost!!!!  In that 
     * case, the object must be added to the hashed collection again.
     * @return Hash code, based on name and input parameters
     */
    public int hashCode() { 
        int hash = HashCodeUtil.hashCode(0, name);
        if(inputParameters != null) { 
            hash = HashCodeUtil.hashCode(hash, inputParameters.length);
            
            // Base hash only on first input parameter type, not all, for performance
            if(inputParameters.length > 0 && inputParameters[0] != null) {
                hash = HashCodeUtil.hashCode(hash, inputParameters[0].getType());
            }    
        }             
        return hash;
    }
    
    /**
     * Compare other object for equality.  This object is equal to another 
     * FunctionMethod if 1) Name of function matches (case-insensitive), 
     * 2) number of input parameters matches and 3) types of input parameters
     * match.
     * @return True if object equals this object according to conditions
     */
    public boolean equals(Object obj) {
        if(obj == this) { 
            return true;
        } else if(obj == null) { 
            return false;
        } else if(obj instanceof FunctionMethod) { 
            FunctionMethod other = (FunctionMethod) obj;

            // Compare # of parameters - do this first as it is much faster than name compare
            if(getInputParameterCount() != other.getInputParameterCount()) {
                return false;
            }

            // Compare function names - case insensitive
            if(other.getName() == null || this.getName() == null) { 
                return false;
            }
            if(! other.getName().equalsIgnoreCase(this.getName()) ) {
                return false;
            }
            
            // Compare types of parameters
            FunctionParameter[] thisInputs = this.getInputParameters();
            if(thisInputs != null && thisInputs.length > 0) { 
                // If thisInputs is not null and >0 and other parameter
                // count matched this parameter count, otherInputs MUST be 
                // non null to have more than one parameter - so we don't 
                // need to check it here.
                FunctionParameter[] otherInputs = other.getInputParameters();
                
                for(int i=0; i<thisInputs.length; i++) { 
                    boolean paramMatch = compareWithNull(thisInputs[i], otherInputs[i]);
                    if(! paramMatch) { 
                        return false;
                    }    
                }   
            }    
            
            // Found no discrepancies, must be equal
            return true;            
        } else {
            // Can't compare object of different type
            return false;
        }    
    }    
    
    /**
     * Compare two objects that may or may not be null and consider null==null
     * as true.
     * @param o1 Object 1
     * @param o2 Object 2
     * @return True if o1 and o2 are null or if they both aren't and they are equal
     */
    private boolean compareWithNull(Object o1, Object o2) {
        if(o1 == null) { 
            if(o2 == null) { 
                return true;
            }
            return false;
        }
        if(o2 == null) { 
            return false;
        }
        return o1.equals(o2);
    }
    
    /**
     * Return string version for debugging purposes
     * @return String representation of function method
     */ 
    public String toString() { 
        StringBuffer str = new StringBuffer();
        if(name != null) { 
            str.append(name);
        } else {
            str.append("<unknown>"); //$NON-NLS-1$
        } 
        
        // Print parameters
        str.append("("); //$NON-NLS-1$
        if(inputParameters != null) { 
            for(int i=0; i<inputParameters.length; i++) {
                if(inputParameters[i] != null) { 
                    str.append(inputParameters[i].toString());                   
                } else {
                    str.append("<unknown>"); //$NON-NLS-1$
                }
                
                if(i < (inputParameters.length-1)) { 
                    str.append(", "); //$NON-NLS-1$
                }
            }    
        }                                
        str.append(") : "); //$NON-NLS-1$
        
        // Print return type
        if(outputParameter != null) { 
            str.append(outputParameter.toString());
        } else {
            str.append("<unknown>"); //$NON-NLS-1$
        }
        
        return str.toString();
    }

    /**
     * Returns true if the function can produce a non-null output from a null parameter
     */
    public boolean isNullDependent() {
        return this.nullDependent;
    }
    
    public void setNullDependent(boolean nullSafe) {
        this.nullDependent = nullSafe;
    }

    public int getDeterministic() {
        return this.deterministic;
    }

    public void setDeterministic(int deterministic) {
        this.deterministic = deterministic;
    }
    
    public boolean isVarArgs() {
    	if (this.inputParameters != null && this.inputParameters.length > 0) {
    		return inputParameters[inputParameters.length - 1].isVarArg();
    	}
    	return false;
    }
     
}
