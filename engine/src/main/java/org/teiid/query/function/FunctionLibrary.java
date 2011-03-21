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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.teiid.api.exception.query.InvalidFunctionException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;



/**
 * The function library is the primary way for the system to find out what
 * functions are available, resolve function signatures, and invoke system
 * and user-defined functions.
 */
public class FunctionLibrary {

	// Special type conversion functions
	public static final String CONVERT = "convert"; //$NON-NLS-1$
	public static final String CAST = "cast"; //$NON-NLS-1$

    // Special lookup function
    public static final String LOOKUP = "lookup"; //$NON-NLS-1$

    // Special user function
    public static final String USER = "user"; //$NON-NLS-1$
    // Special environment variable lookup function
    public static final String ENV = "env"; //$NON-NLS-1$
    public static final String SESSION_ID = "session_id"; //$NON-NLS-1$
    
    // Special pseudo-functions only for XML queries
    public static final String CONTEXT = "context"; //$NON-NLS-1$
    public static final String ROWLIMIT = "rowlimit"; //$NON-NLS-1$
    public static final String ROWLIMITEXCEPTION = "rowlimitexception"; //$NON-NLS-1$
    
    // Misc.
    public static final String DECODESTRING = "decodestring"; //$NON-NLS-1$
    public static final String DECODEINTEGER = "decodeinteger"; //$NON-NLS-1$
    public static final String COMMAND_PAYLOAD = "commandpayload"; //$NON-NLS-1$
    
    public static final String CONCAT = "CONCAT"; //$NON-NLS-1$
    public static final String CONCAT2 = "CONCAT2"; //$NON-NLS-1$
    public static final String CONCAT_OPERATOR = "||"; //$NON-NLS-1$
    public static final String SUBSTRING = "substring"; //$NON-NLS-1$
    public static final String NVL = "NVL"; //$NON-NLS-1$
    public static final String IFNULL = "IFNULL"; //$NON-NLS-1$
    
    public static final String FROM_UNIXTIME = "from_unixtime"; //$NON-NLS-1$
    public static final String TIMESTAMPADD = "timestampadd"; //$NON-NLS-1$
    
    public static final String PARSETIME = "parsetime"; //$NON-NLS-1$
    public static final String PARSEDATE = "parsedate"; //$NON-NLS-1$
    public static final String FORMATTIME = "formattime"; //$NON-NLS-1$
    public static final String FORMATDATE = "formatdate"; //$NON-NLS-1$
    
    public static final String NULLIF = "nullif"; //$NON-NLS-1$
    public static final String COALESCE = "coalesce"; //$NON-NLS-1$

    public static final String SPACE = "space"; //$NON-NLS-1$
	
    // Function tree for system functions (never reloaded)
    private FunctionTree systemFunctions;

    // Function tree for user-defined functions
    private FunctionTree[] userFunctions;

	/**
	 * Construct the function library.  This should be called only once by the
	 * FunctionLibraryManager.
	 */
	public FunctionLibrary(FunctionTree systemFuncs, FunctionTree... userFuncs) {
        systemFunctions = systemFuncs;
       	userFunctions = userFuncs;
	}

    /**
     * Get all function categories, sorted in alphabetical order
     * @return List of function category names, sorted in alphabetical order
     */
    public List<String> getFunctionCategories() {
        // Remove category duplicates
        TreeSet<String> categories = new TreeSet<String>();
        categories.addAll( systemFunctions.getCategories() );
        if (this.userFunctions != null) {
	        for (FunctionTree tree: this.userFunctions) {
	        	categories.addAll(tree.getCategories());
	        }
        }

        ArrayList<String> categoryList = new ArrayList<String>(categories);
        return categoryList;
    }

    /**
     * Get all function forms in a category, sorted by name, then # of args, then names of args.
     * @param category Category name
     * @return List of {@link FunctionForm}s in a category
     */
    public List<FunctionForm> getFunctionForms(String category) {
        List<FunctionForm> forms = new ArrayList<FunctionForm>();
        forms.addAll(systemFunctions.getFunctionForms(category));
        if (this.userFunctions != null) {
	        for (FunctionTree tree: this.userFunctions) {
	        	forms.addAll(tree.getFunctionForms(category));
	        }
        }

        // Sort alphabetically
        Collections.sort(forms);
        return forms;
    }

    /**
     * Find function form based on function name and # of arguments.
     * @param name Function name
     * @param numArgs Number of arguments
     * @return Corresponding form or null if not found
     */
    public FunctionForm findFunctionForm(String name, int numArgs) {
        FunctionForm form = systemFunctions.findFunctionForm(name, numArgs);
        if(form == null && this.userFunctions != null) {
        	for (FunctionTree tree: this.userFunctions) {
        		form = tree.findFunctionForm(name, numArgs);
        		if (form != null) {
        			break;
        		}
        	}
        }
        return form;
    }

	/**
	 * Find a function descriptor given a name and the types of the arguments.
	 * This method matches based on case-insensitive function name and
     * an exact match of the number and types of parameter arguments.
     * @param name Name of the function to resolve
     * @param types Array of classes representing the types
     * @return Descriptor if found, null if not found
	 */
	public FunctionDescriptor findFunction(String name, Class<?>[] types) {
        // First look in system functions
        FunctionDescriptor descriptor = systemFunctions.getFunction(name, types);

        // If that fails, check the user defined functions
        if(descriptor == null && this.userFunctions != null) {
        	for (FunctionTree tree: this.userFunctions) {
        		descriptor = tree.getFunction(name, types);
        		if (descriptor != null) {
        			break;
        		}
        	}
        }

        return descriptor;
	}

	/**
	 * Get the conversions that are needed to call the named function with arguments
	 * of the given type.  In the case of an exact match, the list will contain all nulls.
	 * In other cases the list will contain one or more non-null values where the value
	 * is a conversion function that can be used to convert to the proper types for
	 * executing the function.
     * @param name Name of function
	 * @param returnType
	 * @param types Existing types passed to the function
     * @return Null if no conversion could be found, otherwise an array of conversions
     * to apply to each argument.  The list should match 1-to-1 with the parameters.
     * Parameters that do not need a conversion are null; parameters that do are
     * FunctionDescriptors.
	 * @throws QueryResolverException 
	 */
	public FunctionDescriptor[] determineNecessaryConversions(String name, Class<?> returnType, Class<?>[] types, boolean hasUnknownType) {
		// Check for no args - no conversion necessary
		if(types.length == 0) {
			return new FunctionDescriptor[0];
		}

		// Construct results array
		FunctionDescriptor[] results = null;

        //First find existing functions with same name and same number of parameters
        final Collection<FunctionMethod> functionMethods = new LinkedList<FunctionMethod>();
        functionMethods.addAll( this.systemFunctions.findFunctionMethods(name, types.length) );
        if (this.userFunctions != null) {
	        for (FunctionTree tree: this.userFunctions) {
	        	functionMethods.addAll( tree.findFunctionMethods(name, types.length) );
	        }
        }
        
        //Score each match, reject any where types can not be converted implicitly       
        //Score of current method (lower score means better match with less converts
        //Current best score (lower score is best.  Higher score results in more implicit conversions
        int bestScore = Integer.MAX_VALUE;
        boolean ambiguous = false;
                
        for (FunctionMethod nextMethod : functionMethods) {
            int currentScore = 0; 
            final List<FunctionParameter> methodTypes = nextMethod.getInputParameters();
            //Holder for current signature with converts where required
            FunctionDescriptor[] currentSignature = new FunctionDescriptor[types.length];
            
            //Iterate over the parameters adding conversions where required or failing when
            //no implicit conversion is possible
            int i = 0;
            for(; i < types.length; i++) {
            	//treat all varags as the same type
                final String tmpTypeName = methodTypes.get(Math.min(i, methodTypes.size() - 1)).getType();
                Class<?> targetType = DataTypeManager.getDataTypeClass(tmpTypeName);

                Class<?> sourceType = types[i];
                if (sourceType == null) {
                    FunctionDescriptor fd = findTypedConversionFunction(DataTypeManager.DefaultDataClasses.NULL, targetType);
                    currentSignature[i] = fd;
                    currentScore++;
                    continue;
                }
                
				try {
					FunctionDescriptor fd = getConvertFunctionDescriptor(sourceType, targetType);
					if (fd != null) {
		                currentScore++;
		                currentSignature[i] = fd;
					}
				} catch (InvalidFunctionException e) {
					break;
				}
            }
            
            //If the method is valid match and it is the current best score, capture those values as current best match
            if (i != types.length || currentScore > bestScore) {
                continue;
            }
            
            if (hasUnknownType) {
            	if (returnType != null) {
            		try {
						FunctionDescriptor fd = getConvertFunctionDescriptor(DataTypeManager.getDataTypeClass(nextMethod.getOutputParameter().getType()), returnType);
						if (fd != null) {
							currentScore++;
						}
					} catch (InvalidFunctionException e) {
						//there still may be a common type, but use any other valid conversion over this one
						currentScore += (types.length + 1);
					}
            	}
                ambiguous = currentScore == bestScore;
            }
            
            if (currentScore < bestScore) {

                if (currentScore == 0) {
                    //this must be an exact match
                    return currentSignature;
                }    
                
                bestScore = currentScore;
                results = currentSignature;
            }            
        }
        
        if (ambiguous) {
            return null;
        }
        
		return results;
	}
	
	private FunctionDescriptor getConvertFunctionDescriptor(Class<?> sourceType, Class<?> targetType) throws InvalidFunctionException {
		final String sourceTypeName = DataTypeManager.getDataTypeName(sourceType);
        final String targetTypeName = DataTypeManager.getDataTypeName(targetType);
        //If exact match no conversion necessary
        if(sourceTypeName.equals(targetTypeName)) {
            return null;
        }
        //Else see if an implicit conversion is possible.
        if(!DataTypeManager.isImplicitConversion(sourceTypeName, targetTypeName)){
            throw new InvalidFunctionException();
        }
        //Else no conversion is available and the current method is not a valid match
        final FunctionDescriptor fd = findTypedConversionFunction(sourceType, targetType);
        if(fd == null) {
        	throw new InvalidFunctionException();
        }
        return fd;
	}

    /**
     * Find conversion function and set return type to proper type.   
     * @param sourceType The source type class
     * @param targetType The target type class
     * @return A CONVERT function descriptor or null if not possible
     */
    public FunctionDescriptor findTypedConversionFunction(Class<?> sourceType, Class<?> targetType) {
        FunctionDescriptor fd = findFunction(CONVERT, new Class[] {sourceType, DataTypeManager.DefaultDataClasses.STRING});
        if (fd != null) {
            return copyFunctionChangeReturnType(fd, targetType);
        }
        return null;
    }

	/**
	 * Return a copy of the given FunctionDescriptor with the sepcified return type.
	 * @param fd FunctionDescriptor to be copied.
	 * @param returnType The return type to apply to the copied FunctionDescriptor.
	 * @return The copy of FunctionDescriptor.
	 */
    public FunctionDescriptor copyFunctionChangeReturnType(FunctionDescriptor fd, Class<?> returnType) {
        if(fd != null) {
        	FunctionDescriptor fdImpl = fd;
            FunctionDescriptor copy = (FunctionDescriptor)fdImpl.clone();
            copy.setReturnType(returnType);
            return copy;
        }
        return fd;
    }
    
    public static boolean isConvert(Function function) {
        Expression[] args = function.getArgs();
        String funcName = function.getName();
        
        return args.length == 2 && (funcName.equalsIgnoreCase(FunctionLibrary.CONVERT) || funcName.equalsIgnoreCase(FunctionLibrary.CAST));
    }
}
