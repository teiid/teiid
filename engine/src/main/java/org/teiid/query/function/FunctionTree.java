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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.teiid.UserDefinedAggregate;
import org.teiid.core.CoreConstants;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.MetadataException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.util.CommandContext;


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
    private static final Integer DESCRIPTOR_KEY = -1;

    private Map<String, Set<String>> categories = new HashMap<String, Set<String>>();

    private Map<String, List<FunctionMethod>> functionsByName = new TreeMap<String, List<FunctionMethod>>(String.CASE_INSENSITIVE_ORDER);
    
    private Set<FunctionMethod> allFunctions = new HashSet<FunctionMethod>();

	/**
	 * Function lookup and invocation use: Function name (uppercase) to Map (recursive tree)
	 */
    private Map<String, Map<Object, Object>> treeRoot = new TreeMap<String, Map<Object, Object>>(String.CASE_INSENSITIVE_ORDER);
    private boolean validateClass;
    
    /**
     * Construct a new tree with the given source of function metadata.
     * @param source The metadata source
     */
    public FunctionTree(String name, FunctionMetadataSource source) {
    	this(name, source, false);
    }
    
    /**
     * Construct a new tree with the given source of function metadata.
     * @param source The metadata source
     */
    public FunctionTree(String name, FunctionMetadataSource source, boolean validateClass) {
        // Load data structures
    	this.validateClass = validateClass;
    	boolean system = CoreConstants.SYSTEM_MODEL.equalsIgnoreCase(name) || CoreConstants.SYSTEM_ADMIN_MODEL.equalsIgnoreCase(name);
        Collection<FunctionMethod> functions = source.getFunctionMethods();
    	for (FunctionMethod method : functions) {
			if (!containsIndistinguishableFunction(method)){
                // Add to tree
                addFunction(name, source, method, system);
			} else if (!CoreConstants.SYSTEM_MODEL.equalsIgnoreCase(name)) {
                LogManager.logWarning(LogConstants.CTX_FUNCTION_TREE, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30011, new Object[]{method}));
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
        List<FunctionMethod> methods = functionsByName.get(name);
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
    public FunctionDescriptor addFunction(String schema, FunctionMetadataSource source, FunctionMethod method, boolean system) {
    	String categoryKey = method.getCategory();
    	if (categoryKey == null) {
    		method.setCategory(FunctionCategoryConstants.MISCELLANEOUS);
    		categoryKey = FunctionCategoryConstants.MISCELLANEOUS;
    	}
    	categoryKey = categoryKey.toUpperCase();
        
        // Look up function map (create if necessary)
        Set<String> functions = categories.get(categoryKey);
        if (functions == null) {
            functions = new HashSet<String>();
            categories.put(categoryKey, functions);
        }
        
        // Get method name
        String methodName = schema + AbstractMetadataRecord.NAME_DELIM_CHAR + method.getName();

        // Get input types for path
        List<FunctionParameter> inputParams = method.getInputParameters();
        Class<?>[] types = null;
        if(inputParams != null) {
        	types = new Class<?>[inputParams.size()];
            for(int i=0; i<inputParams.size(); i++) {
                String typeName = inputParams.get(i).getType();
                Class<?> clazz = DataTypeManager.getDataTypeClass(typeName);
                types[i] = clazz;
            }
        } else {
        	types = new Class<?>[0];
        }

        FunctionDescriptor descriptor = createFunctionDescriptor(source, method, types, system);
        descriptor.setSchema(schema);
        // Store this path in the function tree
        
        int index = -1;
        while(true) {
            String nameKey = methodName.toUpperCase();

	        // Look up function in function map
	        functions.add(nameKey);
	
	        // Add method to list by function name
	        List<FunctionMethod> knownMethods = functionsByName.get(nameKey);
	        if(knownMethods == null) {
	            knownMethods = new ArrayList<FunctionMethod>();
	            functionsByName.put(nameKey, knownMethods);
	        }
	        knownMethods.add(method);

        	Map<Object, Object> node = treeRoot.get(methodName);
        	if (node == null) {
        		node = new HashMap<Object, Object>(2);
        		treeRoot.put(methodName, node);
        	}
	        for(int pathIndex = 0; pathIndex < types.length; pathIndex++) {
	            Class<?> pathPart = types[pathIndex];
	            Map<Object, Object> children = (Map<Object, Object>) node.get(pathPart);
	            if(children == null) {
	                children = new HashMap<Object, Object>(2);
	                node.put(pathPart, children);
	            }
	            if (method.isVarArgs() && pathIndex == types.length - 1) {
	        		node.put(DESCRIPTOR_KEY, descriptor);
	                Map<Object, Object> alternate = new HashMap<Object, Object>(2);
	                alternate.put(DESCRIPTOR_KEY, descriptor);
	                node.put(DataTypeManager.getArrayType(pathPart), alternate);
	            }
	            node = children;
	        }
	
	        if (method.isVarArgs()) {
	        	node.put(types[types.length - 1], node);
	        }
	        // Store the leaf descriptor in the tree
	        node.put(DESCRIPTOR_KEY, descriptor);
	        
	        index = methodName.indexOf(AbstractMetadataRecord.NAME_DELIM_CHAR, index+1);
	        if (index == -1) {
	        	break;
	        }
	        methodName = methodName.substring(index+1);
        }
        
        allFunctions.add(method);
        return descriptor;
    }

	private FunctionDescriptor createFunctionDescriptor(
			FunctionMetadataSource source, FunctionMethod method,
			Class<?>[] types, boolean system) {
		// Get return type
        FunctionParameter outputParam = method.getOutputParameter();
        Class<?> outputType = null;
        if(outputParam != null) {
            outputType = DataTypeManager.getDataTypeClass(outputParam.getType());
        }
        List<Class<?>> inputTypes = new ArrayList<Class<?>>(Arrays.asList(types));
        boolean hasWrappedArg = false;
        if (!system) {
	        for (int i = 0; i < types.length; i++) {
	        	if (types[i] == DataTypeManager.DefaultDataClasses.VARBINARY) {
	        		hasWrappedArg = true;
	        		inputTypes.set(i, byte[].class);
	        	}
	        }
        }
        if (method.isVarArgs()) {
        	inputTypes.set(inputTypes.size() - 1, DataTypeManager.getArrayType(inputTypes.get(inputTypes.size() - 1)));
        }

        Method invocationMethod = method.getMethod();
        boolean requiresContext = false;
        // Defect 20007 - Ignore the invocation method if pushdown is not required.
        if (validateClass && (method.getPushdown() == PushDown.CAN_PUSHDOWN || method.getPushdown() == PushDown.CANNOT_PUSHDOWN)) {
        	if (invocationMethod == null) {
	        	if (method.getInvocationClass() == null || method.getInvocationMethod() == null) {
	        		throw new MetadataException(QueryPlugin.Event.TEIID31123, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31123, method.getName()));
	        	}
	            try {
	                Class<?> methodClass = source.getInvocationClass(method.getInvocationClass());
	                ReflectionHelper helper = new ReflectionHelper(methodClass);
	                try {
	                	invocationMethod = helper.findBestMethodWithSignature(method.getInvocationMethod(), inputTypes);
	                } catch (NoSuchMethodException e) {
	                    inputTypes.add(0, CommandContext.class);
	                	invocationMethod = helper.findBestMethodWithSignature(method.getInvocationMethod(), inputTypes);
	                	requiresContext = true;
	                }
	            } catch (ClassNotFoundException e) {
	            	 throw new MetadataException(QueryPlugin.Event.TEIID30387, e,QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30387, method.getName(), method.getInvocationClass()));
	            } catch (NoSuchMethodException e) {
	            	 throw new MetadataException(QueryPlugin.Event.TEIID30388, e,QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30388, method, method.getInvocationClass(), method.getInvocationMethod()));
	            } catch (Exception e) {                
	                 throw new MetadataException(QueryPlugin.Event.TEIID30389, e,QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30389, method, method.getInvocationClass(), method.getInvocationMethod()));
	            } 
        	} else {
        		requiresContext = (invocationMethod.getParameterTypes().length > 0 && org.teiid.CommandContext.class.isAssignableFrom(invocationMethod.getParameterTypes()[0]));
        	}
            if (invocationMethod != null) {
            	// Check return type is non void
        		Class<?> methodReturn = invocationMethod.getReturnType();
        		if(method.getAggregateAttributes() == null && methodReturn.equals(Void.TYPE)) {
        			 throw new MetadataException(QueryPlugin.Event.TEIID30390, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30390, method.getName(), invocationMethod));
        		}

        		// Check that method is public
        		int modifiers = invocationMethod.getModifiers();
        		if(! Modifier.isPublic(modifiers)) {
        			 throw new MetadataException(QueryPlugin.Event.TEIID30391, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30391, method.getName(), invocationMethod));
        		}

        		// Check that method is static
        		if(! Modifier.isStatic(modifiers)) {
        			if (method.getAggregateAttributes() == null) {
        				throw new MetadataException(QueryPlugin.Event.TEIID30392, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30392, method.getName(), invocationMethod));
        			}
        		} else if (method.getAggregateAttributes() != null) {
        			throw new MetadataException(QueryPlugin.Event.TEIID30600, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30600, method.getName(), invocationMethod));
        		}
        		
        		if (method.getAggregateAttributes() != null && !(UserDefinedAggregate.class.isAssignableFrom(invocationMethod.getDeclaringClass()))) {
    				throw new MetadataException(QueryPlugin.Event.TEIID30601, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30601, method.getName(), method.getInvocationClass(), UserDefinedAggregate.class.getName()));
        		}
	            method.setMethod(invocationMethod);
            }
        }

        FunctionDescriptor result = new FunctionDescriptor(method, types, outputType, invocationMethod, requiresContext);
        if (method.getAggregateAttributes() != null && (method.getPushdown() == PushDown.CAN_PUSHDOWN || method.getPushdown() == PushDown.CANNOT_PUSHDOWN)) {
        	result.newInstance();
        }
        result.setHasWrappedArgs(hasWrappedArg);
        return result;
	}
    
    /**
     * Look up a function descriptor by signature in the tree.  If none is
     * found, null is returned.
     * @param name Name of the function, case is not important
     * @param argTypes Types of each argument in the function
     * @return Descriptor which can be used to invoke the function
     */
    FunctionDescriptor getFunction(String name, Class<?>[] argTypes) {
        // Walk path in tree
        Map<Object, Object> node = treeRoot.get(name);
        if (node == null) {
        	return null;
        }
        for(int i=0; i<argTypes.length; i++) {
        	Map<Object, Object> nextNode = (Map<Object, Object>)node.get(argTypes[i]);
        	if (nextNode == null) {
        		if (argTypes[i].isArray()) {
        			//array types are not yet considered in the function typing logic
        			nextNode = (Map<Object, Object>) node.get(DataTypeManager.DefaultDataClasses.OBJECT);
        		}
        		if (nextNode == null) {
        			return null;
        		}
            }
        	node = nextNode;
        }

        // Look for key at the end
        if(node.containsKey(DESCRIPTOR_KEY)) {
            // This is the end - return descriptor
            return (FunctionDescriptor) node.get(DESCRIPTOR_KEY);
        }
        // No descriptor at this location in tree
        return null;
    }

}
