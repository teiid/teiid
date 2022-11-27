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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.api.exception.query.InvalidFunctionException;
import org.teiid.core.CoreConstants;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.metadata.AggregateAttributes;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.translator.SourceSystemFunctions;



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
    public static final String SYS_PROP = "sys_prop"; //$NON-NLS-1$
    public static final String ENV_VAR = "env_var"; //$NON-NLS-1$
    public static final String SESSION_ID = "session_id"; //$NON-NLS-1$

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
    public static final String ARRAY_GET = "array_get"; //$NON-NLS-1$
    public static final String JSONARRAY = "jsonarray"; //$NON-NLS-1$

    public static final String MVSTATUS = "mvstatus"; //$NON-NLS-1$

    public static final Set<String> INTERNAL_SCHEMAS = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);

    static {
        INTERNAL_SCHEMAS.add(CoreConstants.SYSTEM_MODEL);
        INTERNAL_SCHEMAS.add(CoreConstants.SYSTEM_ADMIN_MODEL);
        INTERNAL_SCHEMAS.add(CoreConstants.ODBC_MODEL);
    }

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

    public FunctionTree[] getUserFunctions() {
        return userFunctions;
    }

    public FunctionTree getSystemFunctions() {
        return systemFunctions;
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
     * Get all function in a category.
     * @param category Category name
     * @return List of {@link FunctionMethod}s in a category
     */
    public List<FunctionMethod> getFunctionsInCategory(String category) {
        List<FunctionMethod> forms = new ArrayList<FunctionMethod>();
        forms.addAll(systemFunctions.getFunctionsInCategory(category));
        if (this.userFunctions != null) {
            for (FunctionTree tree: this.userFunctions) {
                forms.addAll(tree.getFunctionsInCategory(category));
            }
        }
        return forms;
    }

    /**
     * Find function form based on function name and # of arguments.
     * @param name Function name
     * @param numArgs Number of arguments
     * @return Corresponding form or null if not found
     */
    public boolean hasFunctionMethod(String name, int numArgs) {
        List<FunctionMethod> methods = systemFunctions.findFunctionMethods(name, numArgs);
        if (!methods.isEmpty()) {
            return true;
        }
        if(this.userFunctions != null) {
            for (FunctionTree tree: this.userFunctions) {
                methods = tree.findFunctionMethods(name, numArgs);
                if (!methods.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
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
     * Find a function descriptor given a name and the types of the arguments.
     * This method matches based on case-insensitive function name and
     * an exact match of the number and types of parameter arguments.
     * @param name Name of the function to resolve
     * @param types Array of classes representing the types
     * @return Descriptor if found, null if not found
     */
    public List<FunctionDescriptor> findAllFunctions(String name, Class<?>[] types) {
        // First look in system functions
        FunctionDescriptor descriptor = systemFunctions.getFunction(name, types);

        // If that fails, check the user defined functions
        if(descriptor == null && this.userFunctions != null) {
            List<FunctionDescriptor> result = new LinkedList<FunctionDescriptor>();
            for (FunctionTree tree: this.userFunctions) {
                descriptor = tree.getFunction(name, types);
                if (descriptor != null) {
                    //pushdown function takes presedence
                    //TODO: there may be multiple translators contributing functions with the same name / types
                    //need "conformed" logic so that the right pushdown can occur
                    if (CoreConstants.SYSTEM_MODEL.equals(descriptor.getSchema())) {
                        return Arrays.asList(descriptor);
                    }
                    result.add(descriptor);
                }
            }
            return result;
        }
        if (descriptor != null) {
            return Arrays.asList(descriptor);
        }
        return Collections.emptyList();
    }

    public static class ConversionResult {
        public ConversionResult(FunctionMethod method) {
            this.method = method;
        }
        public FunctionMethod method;
        public boolean needsConverion;
    }

    /**
     * Get the conversions that are needed to call the named function with arguments
     * of the given type.  In the case of an exact match, the list will contain all nulls.
     * In other cases the list will contain one or more non-null values where the value
     * is a conversion function that can be used to convert to the proper types for
     * executing the function.
     * @param name Name of function
     * @param returnType
     * @param args
     * @param types Existing types passed to the function
     * @throws InvalidFunctionException
     */
    public ConversionResult determineNecessaryConversions(String name, Class<?> returnType, Expression[] args, Class<?>[] types, boolean hasUnknownType) throws InvalidFunctionException {
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
        FunctionMethod result = null;
        boolean isSystem = false;
        boolean narrowing = false;

        outer: for (FunctionMethod nextMethod : functionMethods) {
            int currentScore = 0;
            boolean nextNarrowing = false;
            final List<FunctionParameter> methodTypes = nextMethod.getInputParameters();
            //Holder for current signature with converts where required

            //Iterate over the parameters adding conversions where required or failing when
            //no implicit conversion is possible
            for(int i = 0; i < types.length; i++) {
                final String tmpTypeName = methodTypes.get(Math.min(i, methodTypes.size() - 1)).getRuntimeType();
                Class<?> targetType = DataTypeManager.getDataTypeClass(tmpTypeName);

                Class<?> sourceType = types[i];
                if (sourceType == null) {
                    currentScore++;
                    continue;
                }
                if (sourceType.isArray()&& targetType.isArray()
                        && sourceType.getComponentType().equals(targetType.getComponentType())) {
                    currentScore++;
                    continue;
                }
                if (sourceType.isArray()) {
                    if (isVarArgArrayParam(nextMethod, types, i, targetType)) {
                        //vararg array parameter
                        continue;
                    }
                    //treat the array as object type until proper type handling is added
                    sourceType = DataTypeManager.DefaultDataClasses.OBJECT;
                }
                try {
                    Transform t = getConvertFunctionDescriptor(sourceType, targetType);
                    if (t != null) {
                        if (t.isExplicit()) {
                            if (!(args[i] instanceof Constant) || ResolverUtil.convertConstant(DataTypeManager.getDataTypeName(sourceType), tmpTypeName, (Constant)args[i]) == null) {
                                continue outer;
                            }
                            nextNarrowing = true;
                            currentScore++;
                        } else {
                            currentScore++;
                        }
                    }
                } catch (InvalidFunctionException e) {
                    continue outer;
                }
            }

            //If the method is valid match and it is the current best score, capture those values as current best match
            if (currentScore > bestScore) {
                continue;
            }

            if (hasUnknownType) {
                if (returnType != null) {
                    try {
                        Transform t = getConvertFunctionDescriptor(DataTypeManager.getDataTypeClass(nextMethod.getOutputParameter().getRuntimeType()), returnType);
                        if (t != null) {
                            if (t.isExplicit()) {
                                //there still may be a common type, but use any other valid conversion over this one
                                currentScore += types.length + 1;
                                nextNarrowing = true;
                            } else {
                                currentScore++;
                            }
                        }
                    } catch (InvalidFunctionException e) {
                        //there still may be a common type, but use any other valid conversion over this one
                        currentScore += (types.length * types.length);
                    }
                }
            }

            if (nextNarrowing && result != null && !narrowing) {
                continue;
            }

            boolean useNext = false;

            if (!nextNarrowing && narrowing) {
                useNext = true;
            }

            boolean isSystemNext = nextMethod.getParent() == null || INTERNAL_SCHEMAS.contains(nextMethod.getParent().getName());
            if ((isSystem && isSystemNext) || (!isSystem && !isSystemNext && result != null)) {
                int partCount = partCount(result.getName());
                int nextPartCount = partCount(nextMethod.getName());
                if (partCount < nextPartCount) {
                    //the current is more specific
                    //this makes us more consistent with the table resolving logic
                    continue outer;
                }
                if (nextPartCount < partCount) {
                    useNext = true;
                }
            } else if (isSystemNext) {
                useNext = true;
            }

            if (currentScore == bestScore && !useNext) {
                ambiguous = true;
                boolean useCurrent = false;
                List<FunctionParameter> bestParams = result.getInputParameters();
                for (int j = 0; j < types.length; j++) {
                    String t1 = bestParams.get(Math.min(j, bestParams.size() - 1)).getRuntimeType();
                    String t2 = methodTypes.get((Math.min(j, methodTypes.size() - 1))).getRuntimeType();

                    if (types[j] == null || t1.equals(t2)) {
                        continue;
                    }

                    String commonType = ResolverUtil.getCommonRuntimeType(new String[] {t1, t2});

                    if (commonType == null) {
                        continue outer; //still ambiguous
                    }

                    if (commonType.equals(t1)) {
                        if (!useCurrent) {
                            useNext = true;
                        }
                    } else if (commonType.equals(t2)) {
                        if (!useNext) {
                            useCurrent = true;
                        }
                    } else {
                        continue outer;
                    }
                }
                if (useCurrent) {
                    ambiguous = false; //prefer narrower
                } else {
                    String sysName = result.getProperty(FunctionMethod.SYSTEM_NAME, false);
                    String sysNameOther = nextMethod.getProperty(FunctionMethod.SYSTEM_NAME, false);
                    if (sysName != null && sysName.equalsIgnoreCase(sysNameOther)) {
                        ambiguous = false;
                    }
                }
            }

            if (currentScore < bestScore || useNext) {
                ambiguous = false;
                if (currentScore == 0 && isSystemNext) {
                    return new ConversionResult(nextMethod);
                }

                bestScore = currentScore;
                result = nextMethod;
                isSystem = isSystemNext;
                narrowing = nextNarrowing;
            }
        }

        if (ambiguous) {
            throw GENERIC_EXCEPTION;
        }

        ConversionResult cr = new ConversionResult(result);
        if (result != null) {
            cr.needsConverion = (bestScore != 0);
        }
        return cr;
    }

    private int partCount(String name) {
        int result = 0;
        int index = 0;
        while (true) {
            index = name.indexOf('.', index+1);
            if (index > 0) {
                result++;
            } else {
                break;
            }
        }
        return result;
    }

    public FunctionDescriptor[] getConverts(FunctionMethod method, Class<?>[] types) {
        final List<FunctionParameter> methodTypes = method.getInputParameters();
        FunctionDescriptor[] result = new FunctionDescriptor[types.length];
        for(int i = 0; i < types.length; i++) {
            //treat all varags as the same type
            final String tmpTypeName = methodTypes.get(Math.min(i, methodTypes.size() - 1)).getRuntimeType();
            Class<?> targetType = DataTypeManager.getDataTypeClass(tmpTypeName);

            Class<?> sourceType = types[i];
            if (sourceType == null) {
                result[i] = findTypedConversionFunction(DataTypeManager.DefaultDataClasses.NULL, targetType);
            } else if (sourceType != targetType){
                if (isVarArgArrayParam(method, types, i, targetType)) {
                    //vararg array parameter
                    continue;
                }
                result[i] = findTypedConversionFunction(sourceType, targetType);
            }
        }
        return result;
    }

    public boolean isVarArgArrayParam(FunctionMethod method, Class<?>[] types,
            int i, Class<?> targetType) {
        return i == types.length - 1 && method.isVarArgs() && i == method.getInputParameterCount() - 1
                && types[i].isArray() && targetType.isAssignableFrom(types[i].getComponentType());
    }

    private static final InvalidFunctionException GENERIC_EXCEPTION = new InvalidFunctionException(QueryPlugin.Event.TEIID30419);

    private Transform getConvertFunctionDescriptor(Class<?> sourceType, Class<?> targetType) throws InvalidFunctionException {
        //If exact match no conversion necessary
        if(sourceType.equals(targetType)) {
            return null;
        }
        Transform result = DataTypeManager.getTransform(sourceType, targetType);
        //Else see if an implicit conversion is possible.
        if(result == null){
             throw GENERIC_EXCEPTION;
        }
        return result;
    }

    /**
     * Find conversion function and set return type to proper type.
     * @param sourceType The source type class
     * @param targetType The target type class
     * @return A CONVERT function descriptor or null if not possible
     */
    public FunctionDescriptor findTypedConversionFunction(Class<?> sourceType, Class<?> targetType) {
        //TODO: should array to string be prohibited?
        FunctionDescriptor fd = findFunction(CONVERT, new Class[] {sourceType, DataTypeManager.DefaultDataClasses.STRING});
        if (fd != null) {
            return copyFunctionChangeReturnType(fd, targetType);
        }
        return null;
    }

    /**
     * Return a copy of the given FunctionDescriptor with the specified return type.
     * @param fd FunctionDescriptor to be copied.
     * @param returnType The return type to apply to the copied FunctionDescriptor.
     * @return The copy of FunctionDescriptor.
     */
    public FunctionDescriptor copyFunctionChangeReturnType(FunctionDescriptor fd, Class<?> returnType) {
        if(fd != null) {
            FunctionDescriptor fdImpl = fd;
            FunctionDescriptor copy = fdImpl.clone();
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

    /**
     * Return a list of the most general forms of built-in aggregate functions.
     * <br>count(*) - is not included
     * <br>textagg - is not included due to its non standard syntax
     *
     * @param includeAnalytic - true to include analytic functions that must be windowed
     * @return
     */
    public List<FunctionMethod> getBuiltInAggregateFunctions(boolean includeAnalytic) {
        ArrayList<FunctionMethod> result = new ArrayList<FunctionMethod>();
        if (this.systemFunctions != null) {
            FunctionDescriptor stExtent = this.systemFunctions.getFunction(SourceSystemFunctions.ST_EXTENT, new Class[] {DataTypeManager.DefaultDataClasses.GEOMETRY});
            result.add(stExtent.getMethod());
        }
        for (Type type : AggregateSymbol.Type.values()) {
            AggregateAttributes aa = new AggregateAttributes();
            if (type.isAnalytical()) {
                if (!includeAnalytic) {
                    continue;
                }
                aa.setAnalytic(true);
                aa.setAllowsDistinct(false);
                aa.setAllowsOrderBy(false);
            }
            String returnType = null;
            String[] argTypes = null;
            aa.setAllowsDistinct(true);
            switch (type) {
            case TEXTAGG:
            case USER_DEFINED:
                continue;
            case DENSE_RANK:
            case RANK:
            case ROW_NUMBER:
                aa.setAllowsDistinct(false);
                returnType = AggregateSymbol.LONG_RANKS?DataTypeManager.DefaultDataTypes.LONG:DataTypeManager.DefaultDataTypes.INTEGER;
                argTypes = new String[] {};
                break;
            case ANY:
            case SOME:
            case EVERY:
                returnType = DataTypeManager.DefaultDataTypes.BOOLEAN;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.BOOLEAN};
                break;
            case COUNT_BIG:
                returnType = DataTypeManager.DefaultDataTypes.LONG;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.OBJECT};
                break;
            case COUNT:
                returnType = DataTypeManager.DefaultDataTypes.INTEGER;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.OBJECT};
                break;
            case MAX:
            case MIN:
            case AVG:
            case SUM:
                returnType = DataTypeManager.DefaultDataTypes.OBJECT;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.OBJECT};
                break;
            case STDDEV_POP:
            case STDDEV_SAMP:
            case VAR_POP:
            case VAR_SAMP:
                returnType = DataTypeManager.DefaultDataTypes.DOUBLE;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.DOUBLE};
                break;
            case STRING_AGG:
                returnType = DataTypeManager.DefaultDataTypes.OBJECT;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.OBJECT};
                aa.setAllowsOrderBy(true);
                break;
            case ARRAY_AGG:
                returnType = DataTypeManager.DefaultDataTypes.OBJECT;
                argTypes = new String[] {DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.OBJECT))};
                aa.setAllowsOrderBy(true);
                aa.setAllowsDistinct(false);
                break;
            case JSONARRAY_AGG:
                returnType = DataTypeManager.DefaultDataTypes.JSON;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.OBJECT};
                aa.setAllowsOrderBy(true);
                aa.setAllowsDistinct(false);
                break;
            case XMLAGG:
                returnType = DataTypeManager.DefaultDataTypes.XML;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.XML};
                aa.setAllowsOrderBy(true);
                aa.setAllowsDistinct(false);
                break;
            case FIRST_VALUE:
            case LAST_VALUE:
                returnType = DataTypeManager.DefaultDataTypes.OBJECT;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.OBJECT};
                break;
            case LEAD:
            case LAG:
                returnType = DataTypeManager.DefaultDataTypes.OBJECT;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.OBJECT, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.OBJECT};
                break;
            case NTILE:
                returnType = DataTypeManager.DefaultDataTypes.INTEGER;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.INTEGER};
                break;
            case PERCENT_RANK:
                returnType = DataTypeManager.DefaultDataTypes.DOUBLE;
                argTypes = new String[] {};
                break;
            case CUME_DIST:
                returnType = DataTypeManager.DefaultDataTypes.DOUBLE;
                argTypes = new String[] {};
                break;
            case NTH_VALUE:
                returnType = DataTypeManager.DefaultDataTypes.OBJECT;
                argTypes = new String[] {DataTypeManager.DefaultDataTypes.OBJECT, DataTypeManager.DefaultDataTypes.INTEGER};
                break;
            }
            FunctionMethod fm = FunctionMethod.createFunctionMethod(type.name(), type.name(), FunctionCategoryConstants.AGGREGATE, returnType, argTypes);
            fm.setAggregateAttributes(aa);
            result.add(fm);
        }
        return result;
    }

    public boolean userFunctionExists(String resourceName) {
        String[] parts = resourceName.split("\\.", 2); //$NON-NLS-1$
        if (parts.length < 0) {
            return false;
        }
        for (FunctionTree tree : userFunctions) {
            if (parts[0].equalsIgnoreCase(tree.getSchemaName())) {
                return tree.hasFunctionWithName(parts[1]);
            }
        }
        return false;
    }
}
