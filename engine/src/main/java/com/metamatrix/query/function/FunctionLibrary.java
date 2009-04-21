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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.api.exception.query.InvalidFunctionException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.function.metadata.FunctionParameter;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;


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
    
    // Special xpathvalue function
    public static final String XPATHVALUE = "xpathvalue"; //$NON-NLS-1$

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
    
    public static final String NULLIF = "nullif"; //$NON-NLS-1$
    public static final String COALESCE = "coalesce"; //$NON-NLS-1$

    public static final String SPACE = "space"; //$NON-NLS-1$
    
    // Function tree for system functions (never reloaded)
    private FunctionTree systemFunctions;

    // Function tree for user-defined functions (reloadable)
    private FunctionTree userFunctions;

	/**
	 * Construct the function library.  This should be called only once by the
	 * FunctionLibraryManager.
	 */
	FunctionLibrary() {
        // Put empty trees here to avoid null checks throughout the class
        systemFunctions = new FunctionTree(Collections.EMPTY_LIST);
        userFunctions = new FunctionTree(Collections.EMPTY_LIST);
	}

    /**
     * Get all function categories, sorted in alphabetical order
     * @return List of function category names, sorted in alphabetical order
     */
    public List getFunctionCategories() {
        // Remove category duplicates
        HashSet categories = new HashSet();
        categories.addAll( systemFunctions.getCategories() );
        categories.addAll( userFunctions.getCategories() );

        // Sort alphabetically
        ArrayList categoryList = new ArrayList(categories);
        Collections.sort(categoryList);
        return categoryList;
    }

    /**
     * Get all function forms in a category, sorted by name, then # of args, then names of args.
     * @param category Category name
     * @return List of {@link FunctionForm}s in a category
     */
    public List getFunctionForms(String category) {
        List forms = new ArrayList();
        forms.addAll(systemFunctions.getFunctionForms(category));
        forms.addAll(userFunctions.getFunctionForms(category));

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
        if(form == null) {
            form = userFunctions.findFunctionForm(name, numArgs);
        }
        return form;
    }

    /**
     * Add the system functions to the library.  This should be done
     * exactly once by the FunctionLibraryManager.
     * @param source System metadata source
     */
    void setSystemFunctions(FunctionMetadataSource source) {
        this.systemFunctions = new FunctionTree(source);
    }

    /**
     * Replace the existing set of reloadable functions with a new set.  This
     * is called by the FunctionLibraryManager every time it reloads the
     * reloadable sources.
     * @param sources Collection of {@link FunctionMetadataSource} objects
     */
    void replaceReloadableFunctions(Collection sources) {
        // Build new function tree
        FunctionTree reloadedFunctions = new FunctionTree(sources);

        // Switch to new user-defined functions - this is not synchronized
        // because it is merely an object reference change.  There is no
        // way for other code to get part of the old tree and part of the new
        // tree - they will get either one or the other.  The old tree is
        // dropped.
        this.userFunctions = reloadedFunctions;
    }

	/**
	 * Find a function descriptor given a name and the types of the arguments.
	 * This method matches based on case-insensitive function name and
     * an exact match of the number and types of parameter arguments.
     * @param name Name of the function to resolve
     * @param types Array of classes representing the types
     * @return Descriptor if found, null if not found
	 */
	public FunctionDescriptor findFunction(String name, Class[] types) {
        // First look in system functions
        FunctionDescriptor descriptor = systemFunctions.getFunction(name, types);

        // If that fails, check the user defined functions
        if(descriptor == null) {
            descriptor = userFunctions.getFunction(name, types);
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
        functionMethods.addAll( this.systemFunctions.findFunctionMethods(name , types.length) );
        functionMethods.addAll( this.userFunctions.findFunctionMethods(name , types.length) );
        
        //Score each match, reject any where types can not be converted implicitly       
        //Score of current method (lower score means better match with less converts
        //Current best score (lower score is best.  Higher score results in more implicit conversions
        int bestScore = Integer.MAX_VALUE;
        boolean ambiguous = false;
                
        for (FunctionMethod nextMethod : functionMethods) {
            int currentScore = 0; 
            final FunctionParameter[] methodTypes = nextMethod.getInputParameters();
            //Holder for current signature with converts where required
            FunctionDescriptor[] currentSignature = new FunctionDescriptor[types.length];
            
            //Iterate over the parameters adding conversions where required or failing when
            //no implicit conversion is possible
            int i = 0;
            for(; i < types.length; i++) {
            	//treat all varags as the same type
                final String tmpTypeName = methodTypes[Math.min(i, methodTypes.length - 1)].getType();
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
    public FunctionDescriptor findTypedConversionFunction(Class sourceType, Class targetType) {
        FunctionDescriptor fd = findFunction(CONVERT, new Class[] {sourceType, DataTypeManager.DefaultDataClasses.STRING});
        if (fd != null) {
            return copyFunctionChangeReturnType(fd, targetType);
        }
        return null;
    }

	/**
	 * Invoke the function described in the function descriptor, using the
	 * values provided.  Return the result of the function.
	 * @param fd Function descriptor describing the name and types of the arguments
	 * @param values Values that should match 1-to-1 with the types described in the
	 * function descriptor
	 * @return Result of invoking the function
	 */
	public Object invokeFunction(FunctionDescriptor fd, Object[] values)
		throws InvalidFunctionException, FunctionExecutionException {

        if(fd == null) {
            throw new InvalidFunctionException(ErrorMessageKeys.FUNCTION_0001, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0001, fd));
        }
        
        if (!fd.isNullDependent()) {
        	for (int i = 0; i < values.length; i++) {
				if (values[i] == null) {
					return null;
				}
			}
        }

        // If descriptor is missing invokable method, find this VM's descriptor
        // give name and types from fd
        Method method = fd.getInvocationMethod();
        if(method == null) {
            FunctionDescriptor localDescriptor = findFunction(fd.getName(), fd.getTypes());
            if(localDescriptor == null) {
                throw new InvalidFunctionException(ErrorMessageKeys.FUNCTION_0001, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0001, fd));
            }

            // Get local invocation method, which should never be null
            method = localDescriptor.getInvocationMethod();
            if (method == null){
                throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0002, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0002, localDescriptor.getName()));
            }
        }
        
        if (fd.getDeterministic() >= FunctionMethod.SESSION_DETERMINISTIC && values.length > 0 && values[0] instanceof CommandContext) {
        	CommandContext cc = (CommandContext)values[0];
        	cc.setSessionFunctionEvaluated(true);
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
            result = DataTypeManager.convertToRuntimeType(result);
            result = DataTypeManager.transformValue(result, fd.getReturnType());
            return result;
        } catch(InvocationTargetException e) {
            throw new FunctionExecutionException(e.getTargetException(), ErrorMessageKeys.FUNCTION_0003, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0003, fd.getName()));
        } catch(IllegalAccessException e) {
            throw new FunctionExecutionException(e, ErrorMessageKeys.FUNCTION_0004, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0004, method.toString()));
        } catch (TransformationException e) {
        	throw new FunctionExecutionException(e, e.getMessage());
		}
	}

	/**
	 * Return a copy of the given FunctionDescriptor with the sepcified return type.
	 * @param fd FunctionDescriptor to be copied.
	 * @param returnType The return type to apply to the copied FunctionDescriptor.
	 * @return The copy of FunctionDescriptor.
	 */
    public FunctionDescriptor copyFunctionChangeReturnType(FunctionDescriptor fd, Class returnType) {
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
        String funcName = function.getName().toLowerCase();
        
        return args.length == 2 && (funcName.equalsIgnoreCase(FunctionLibrary.CONVERT) || funcName.equalsIgnoreCase(FunctionLibrary.CAST));
    }
}
