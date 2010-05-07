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

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.function.metadata.FunctionCategoryConstants;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.function.metadata.FunctionParameter;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * Data structure used to store function signature information. There are multiple uses
 * of this signature information so there are multiple data structures within the FunctionTree
 * for handling each.  One type of information is the function metadata required by users of
 * this class for data driving GUIs or function selection.  The other type of information is that
 * needed to quickly find and/or invoke the functions at execution time.  In general all methods
 * are concerned with function metadata EXCEPT {@link #getFunction} which is used to find a function
 * for execution.
 */
public class FunctionTree {

    // Constant used to look up the special descriptor key in a node map
    private static final Integer DESCRIPTOR_KEY = new Integer(-1);

    private Map<String, Set<String>> categories = new HashMap<String, Set<String>>();

    private Map<String, List<FunctionMethod>> functionsByName = new HashMap<String, List<FunctionMethod>>();
    
    private Set<FunctionMethod> allFunctions = new HashSet<FunctionMethod>();

	/**
	 * Function lookup and invocation use: Function name (uppercase) to Map (recursive tree)
	 */
    private Map treeRoot = new HashMap();

    /**
     * Construct a new tree with the given source of function metadata.
     * @param source The metadata source
     */
    public FunctionTree(FunctionMetadataSource source) {
        // Load data structures
        addSource(source);
    }

    /**
     * Construct a new tree with the given collection of sources.
     * @param sources The collection of function metadata sources ({@link com.metamatrix.query.function.FunctionMetadataSource})
     */
    FunctionTree(Collection sources) {
        // Load data structures
        addSources(sources);
    }

    /**
     * Add a collection of functions to the data structures.
     * @param sources The function metadata sources ({@link com.metamatrix.query.function.FunctionMetadataSource})
     */
    private void addSources(Collection sources) {
        if(sources == null) {
            return;
        }

        Iterator sourceIter = sources.iterator();
        while(sourceIter.hasNext()) {
            Object sourceObj = sourceIter.next();
            if(sourceObj instanceof FunctionMetadataSource) {
                addSource((FunctionMetadataSource) sourceObj);
            } else {
                Assertion.failed(QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0044, sourceObj.getClass().getName()));
            }
        }
    }

    /**
     * Add all functions from a metadata source to the data structures.
     * @param source The source of the functions
     */
    private void addSource(FunctionMetadataSource source) {
        if(source == null) {
            return;
        }

        Collection functions = source.getFunctionMethods();
        if(functions != null) {
            Iterator functionIter = functions.iterator();
            while(functionIter.hasNext()) {
                Object functionObj = functionIter.next();
                if(! (functionObj instanceof FunctionMethod)) {
                    Assertion.failed(QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0045, functionObj.getClass().getName()));
                }
                FunctionMethod method = (FunctionMethod) functionObj;

				if (!containsIndistinguishableFunction(method)){
                    // Store method metadata for retrieval
                    addMetadata(method);

                    // Add to tree
                    addFunction(source, method);
				} else {
                    LogManager.logWarning(LogConstants.CTX_FUNCTION_TREE, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0046, new Object[]{method}));
				}
            }
        }
    }

	// ---------------------- FUNCTION SELECTION USE METHODS ----------------------

	/*
	 * Per defect 4612 -
	 * Because of the fix for defect 4264, it is possible in the modeler to
	 * define two functions with different implementations, but having the
	 * same name (using the "alias") and the same parameter types, making the
	 * two FunctionMethod objects indistinguishable by their equals method.
	 * This method will check if any indistinguishable functions are already
	 * present in this FunctionTree.  If so, it will be logged and any
	 * newer indistinguishable functions will just not be added.
	 */
	private boolean containsIndistinguishableFunction(FunctionMethod method){
        return allFunctions.contains(method);
	}

    /**
     * Store the method in the function metadata.
     * @param method The function metadata for a particular method signature
     */
    private void addMetadata(FunctionMethod method) {
    	String categoryKey = method.getCategory();
    	if (categoryKey == null) {
    		method.setCategory(FunctionCategoryConstants.MISCELLANEOUS);
    		categoryKey = FunctionCategoryConstants.MISCELLANEOUS;
    	}
    	categoryKey = categoryKey.toUpperCase();
        String nameKey = method.getName().toUpperCase();

        // Look up function map (create if necessary)
        Set<String> functions = categories.get(categoryKey);
        if (functions == null) {
            functions = new HashSet<String>();
            categories.put(categoryKey, functions);
        }

        // Look up function in function map
        functions.add(nameKey);

        // Add method to list by function name
        List<FunctionMethod> knownMethods = functionsByName.get(nameKey);
        if(knownMethods == null) {
            knownMethods = new ArrayList<FunctionMethod>();
            functionsByName.put(nameKey, knownMethods);
        }
        knownMethods.add(method);
        allFunctions.add(method);
    }

    /**
     * Get collection of category names.
     * @return Category names
     */
    Collection<String> getCategories() {
        return categories.keySet();
    }

    /**
     * Get collection of function forms in a category
     * @param category Category to get (case-insensitive)
     * @return Collection of {@link FunctionForm}s
     */
    Collection<FunctionForm> getFunctionForms(String category) {
        Set<FunctionForm> functionForms = new HashSet<FunctionForm>();

        Set<String> functions = categories.get(category.toUpperCase());
        if(functions != null) {
        	for (String functionName : functions) {
        		for (FunctionMethod functionMethod : this.functionsByName.get(functionName)) {
                    functionForms.add(new FunctionForm(functionMethod));
                }
            }
        }

        return functionForms;
    }

    /**
     * Find function form based on function name and # of arguments.
     * @param name Function name, case insensitive
     * @param args Number of arguments
     * @return Corresponding form or null if not found
     */
    FunctionForm findFunctionForm(String name, int args) {
    	List<FunctionMethod> results = findFunctionMethods(name, args);
    	if (results.size() > 0) {
    		return new FunctionForm(results.get(0));
    	}
    	return null;
    }
    
    /**
     * Find all function methods with the given name and arg length
     * @param name Function name, case insensitive
     * @param args Number of arguments
     * @return Corresponding form or null if not found
     */
    List<FunctionMethod> findFunctionMethods(String name, int args) {
        final List<FunctionMethod> allMatches = new ArrayList<FunctionMethod>();
        List<FunctionMethod> methods = functionsByName.get(name.toUpperCase());
        if(methods == null || methods.size() == 0) {
            return allMatches;
        }

        for (FunctionMethod functionMethod : methods) {
            if(functionMethod.getInputParameterCount() == args || functionMethod.isVarArgs() && args >= functionMethod.getInputParameterCount() - 1) {
                allMatches.add(functionMethod);
            }
        }

        return allMatches;
    }    

	// ---------------------- FUNCTION INVOCATION USE METHODS ----------------------

    /**
     * Store the method for function resolution and invocation.
     * @param source The function metadata source, which knows how to obtain the invocation class
     * @param method The function metadata for a particular method signature
     */
    private void addFunction(FunctionMetadataSource source, FunctionMethod method) {
        // Get method name
        String methodName = method.getName();

        // Get input types for path
        FunctionParameter[] inputParams = method.getInputParameters();
        List<Class> inputTypes = new LinkedList<Class>();
        if(inputParams != null) {
            for(int i=0; i<inputParams.length; i++) {
                String typeName = inputParams[i].getType();
                inputTypes.add(DataTypeManager.getDataTypeClass(typeName));
            }
        }
        Class[] types = inputTypes.toArray(new Class[inputTypes.size()]);

        if (method.isVarArgs()) {
        	inputTypes.set(inputTypes.size() - 1, Array.newInstance(inputTypes.get(inputTypes.size() - 1), 0).getClass());
        }

        // Get return type
        FunctionParameter outputParam = method.getOutputParameter();
        Class outputType = null;
        if(outputParam != null) {
            outputType = DataTypeManager.getDataTypeClass(outputParam.getType());
        }

        Method invocationMethod = null;
        boolean requiresContext = false;
        // Defect 20007 - Ignore the invocation method if pushdown is not required.
        if (method.getPushdown() == FunctionMethod.CAN_PUSHDOWN || method.getPushdown() == FunctionMethod.CANNOT_PUSHDOWN) {
            try {
                Class methodClass = source.getInvocationClass(method.getInvocationClass());
                ReflectionHelper helper = new ReflectionHelper(methodClass);
                try {
                	invocationMethod = helper.findBestMethodWithSignature(method.getInvocationMethod(), inputTypes);
                } catch (NoSuchMethodException e) {
                    inputTypes.add(0, CommandContext.class);
                	invocationMethod = helper.findBestMethodWithSignature(method.getInvocationMethod(), inputTypes);
                	requiresContext = true;
                }
            } catch (ClassNotFoundException e) {
              // Failed to load class, so can't load method - this will fail at invocation time.
              // We don't fail here because this situation can occur in the modeler, which does
              // not have the function jar files.  The modeler never invokes, so this isn't a
              // problem.
            } catch (Exception e) {                
                throw new MetaMatrixRuntimeException(e, ErrorMessageKeys.FUNCTION_0047, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0047, new Object[]{method.getInvocationClass(), invocationMethod, inputTypes}));
            } 
            if(invocationMethod != null && !FunctionTree.isValidMethod(invocationMethod)) {
            	throw new MetaMatrixRuntimeException(ErrorMessageKeys.FUNCTION_0047, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0047, new Object[]{method.getInvocationClass(), invocationMethod, inputTypes}));
            }
        } else {
            inputTypes.add(0, CommandContext.class);
        }

        FunctionDescriptor descriptor = new FunctionDescriptor(method.getName(), method.getPushdown(), types, outputType, invocationMethod, requiresContext, method.isNullDependent(), method.getDeterministic());
        // Store this path in the function tree
        Map node = treeRoot;
        Object[] path = buildPath(methodName, types);
        for(int pathIndex = 0; pathIndex < path.length; pathIndex++) {
            Object pathPart = path[pathIndex];
            Map children = (Map) node.get(pathPart);
            if(children == null) {
                children = new HashMap();
                node.put(pathPart, children);
            }
            if (method.isVarArgs() && pathIndex == path.length - 1) {
        		node.put(DESCRIPTOR_KEY, descriptor);
            }
            node = children;
        }

        if (method.isVarArgs()) {
        	node.put(types[types.length - 1], node);
        }
        // Store the leaf descriptor in the tree
        node.put(DESCRIPTOR_KEY, descriptor);
    }
    
	/**
	 * Validate a method looked up by reflection.  The method should have a non-void return type
	 * and be a public static method.
	 * @param method Method to validate
	 * @return True if valid
	 */
	static boolean isValidMethod(Method method) {
		// Check return type is non void
		Class methodReturn = method.getReturnType();
		if(methodReturn.equals(Void.TYPE)) {
		    return false;
		}

		// Check that method is public
		int modifiers = method.getModifiers();
		if(! Modifier.isPublic(modifiers)) {
		    return false;
		}

		// Check that method is static
		if(! Modifier.isStatic(modifiers)) {
		    return false;
		}

		return true;
	}

    /**
     * Look up a function descriptor by signature in the tree.  If none is
     * found, null is returned.
     * @param name Name of the function, case is not important
     * @param argTypes Types of each argument in the function
     * @return Descriptor which can be used to invoke the function
     */
    FunctionDescriptor getFunction(String name, Class[] argTypes) {
        // Build search path
        Object[] path = buildPath(name, argTypes);

        // Walk path in tree
        Map node = treeRoot;
        for(int i=0; i<path.length; i++) {
        	node = (Map)node.get(path[i]);
        	if (node == null) {
        		return null;
            }
        }

        // Look for key at the end
        if(node.containsKey(DESCRIPTOR_KEY)) {
            // This is the end - return descriptor
            return (FunctionDescriptor) node.get(DESCRIPTOR_KEY);
        }
        // No descriptor at this location in tree
        return null;
    }

    /**
     * Build the path in the function storage tree.  The path for a function consists
     * of it's name (uppercased) and each of the argument classes.
     * @param name Name of function
     * @param argTypes Types of each arguments
     * @return Path in function storage tree
     */
    private Object[] buildPath(String name, Class[] argTypes) {
        Object[] path = new Object[argTypes.length + 1];
        path[0] = name.toUpperCase();
        System.arraycopy(argTypes, 0, path, 1, argTypes.length);
        return path;
    }
}
