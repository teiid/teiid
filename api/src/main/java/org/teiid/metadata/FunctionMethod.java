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

package org.teiid.metadata;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;


/**
 * <p>This class represents information about a particular function signature.
 * Function signatures are unique with respect to their name, # of arguments,
 * and type of arguments. Return type and argument names are not uniqueness
 * factors.  This class makes no attempt to validate the data put into it,
 * particularly with respect to null values.
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
 * <LI>determinism - specifies whether the function is deterministic or not. Various levels are provided
 * <LI>nullOnNull - Specifies whether the function is called if any of the input arguments is null. The result is the null value.
 * </UL>
 *
 * @see FunctionParameter
 */
public class FunctionMethod extends AbstractMetadataRecord {

    public static final String SYSTEM_NAME = AbstractMetadataRecord.RELATIONAL_PREFIX + "system-name"; //$NON-NLS-1$
    public static final String VIRTUAL_FUNCTION = AbstractMetadataRecord.RELATIONAL_PREFIX + "virtual-function"; //$NON-NLS-1$

    private static final long serialVersionUID = -8039086494296455152L;

    private static final String NOT_ALLOWED = "NOT_ALLOWED"; //$NON-NLS-1$
    private static final String ALLOWED = "ALLOWED"; //$NON-NLS-1$
    private static final String REQUIRED = "REQUIRED"; //$NON-NLS-1$

    /**
     * Function Pushdown
     * CAN_PUSHDOWN = If the source supports the function, then it will be pushed down. Must supply the Java impl
     * CANNOT_PUSHDOWN = It will not be pushed down, evaluated in Teiid. Must supply the Java impl
     * MUST_PUSHDOWN = Function must be pushed to source, no need to supply Java impl.
     * SYNTHETIC = system functions that will be rewritten
     */
    public enum PushDown {CAN_PUSHDOWN, CANNOT_PUSHDOWN, MUST_PUSHDOWN, SYNTHETIC};

    /**
     * DETERMINISTIC - normal deterministic functions
     * vdb - lookup (however lookup values can be flushed at any time), current_database
     * session - env, user
     * command - command payload
     * never - rand, etc.
     *
     * Anything at a session level and above is treated as deterministic.
     * This is not quite correct for lookup or env.  Only in extremely rare
     * circumstances would that be a problem.
     *
     * For now the commandPayload function is treated as a special case, like lookup, in
     * that it is considered deterministic, but will be delayed in its evaluation until
     * processing time.
     */
    public enum Determinism{
        NONDETERMINISTIC,
        INSTRUCTION_DETERMINISTIC, //specifically for the now function which is not fully non-deterministic TEIID-5290
        COMMAND_DETERMINISTIC,
        SESSION_DETERMINISTIC,
        USER_DETERMINISTIC,
        VDB_DETERMINISTIC,
        DETERMINISTIC;
    }

    private String description;
    private String category;
    private PushDown pushdown = PushDown.CAN_PUSHDOWN;
    private String invocationClass;
    private String invocationMethod;
    private boolean nullOnNull;
    private AggregateAttributes aggregateAttributes;
    private transient Method method;

    private Determinism determinism = Determinism.DETERMINISTIC;

    protected List<FunctionParameter> inParameters = new ArrayList<FunctionParameter>(2);
    private FunctionParameter outputParameter;
    private Schema parent;

    public FunctionMethod() {
    }

    public FunctionMethod(String name, String description, String category, FunctionParameter[] inputParams, FunctionParameter outputParam) {
        this(name, description, category, PushDown.MUST_PUSHDOWN, null, null, inputParams!=null?Arrays.asList(inputParams):null, outputParam, true, Determinism.DETERMINISTIC);
    }

    public FunctionMethod(String name,
                          String description,
                          String category,
                          PushDown pushdown,
                          String invocationClass,
                          String invocationMethod,
                          List<FunctionParameter> inputParams,
                          FunctionParameter outputParam,
                          boolean nullOnNull,
                          Determinism deterministic) {

        setName(name);
        setDescription(description);
        setCategory(category);
        setPushdown(pushdown);
        setInvocationClass(invocationClass);
        setInvocationMethod(invocationMethod);
        setInputParameters(inputParams);
        setOutputParameter(outputParam);
        setNullOnNull(nullOnNull);
        setDeterminism(deterministic);
    }

    public FunctionMethod(String name, String description, String category,
        String invocationClass, String invocationMethod,
        FunctionParameter[] inputParams, FunctionParameter outputParam) {
        this(name, description, category, PushDown.CAN_PUSHDOWN, invocationClass, invocationMethod, inputParams!=null?Arrays.asList(inputParams):null, outputParam, true, Determinism.DETERMINISTIC);
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
     */
    public String getCategory() {
        return this.category;
    }

    /**
     * Set category of method
     * @param category Category
     */
    public void setCategory(String category) {
        this.category = category;
    }

    /**
     * Get pushdown property of method
     * @return One of the FunctionMethod constants for pushdown
     */
    public PushDown getPushdown() {
        return pushdown;
    }

    /**
     * Set pushdown property of method
     * @param pushdown One of the FunctionMethod constants for pushdown
     */
    public void setPushdown(PushDown pushdown) {
        this.pushdown = pushdown;
    }

    public void setPushDown(String pushdown) {
        if (pushdown != null) {
            if (pushdown.equals(REQUIRED)) {
                this.pushdown = PushDown.MUST_PUSHDOWN;
            } else if (pushdown.equals(ALLOWED)) {
                this.pushdown = PushDown.CAN_PUSHDOWN;
            } else if (pushdown.equals(NOT_ALLOWED)) {
                this.pushdown = PushDown.CANNOT_PUSHDOWN;
            }
        } else {
            this.pushdown = PushDown.CAN_PUSHDOWN;
        }
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
        if(this.inParameters == null) {
            return 0;
        }
        return this.inParameters.size();
    }

    /**
     * Get input parameters
     * @return Array of input parameters, may be null if 0 parameters
     */

    public List<FunctionParameter> getInputParameters() {
        return this.inParameters;
    }

    /**
     * Set input parameters.
     * @param params Input parameters
     */
    public void setInputParameters(List<FunctionParameter> params) {
        this.inParameters.clear();
        if (params != null) {
            for (FunctionParameter param : params) {
                param.setParent(this);
                this.inParameters.add(param);
            }
        }
    }

    /**
     * Get output/return parameter.
     * @return Output parameter or return argument
     */
    public FunctionParameter getOutputParameter() {
        return this.outputParameter;
    }

    /**
     * Set output/return parameter.
     * @param param Output Parameter
     */
    public void setOutputParameter(FunctionParameter param) {
        if (param != null) {
            param.setName(FunctionParameter.OUTPUT_PARAMETER_NAME);
            param.setParent(this);
        }
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
        int hash = HashCodeUtil.hashCode(0, super.getName().toUpperCase());
        if(inParameters != null) {
            hash = HashCodeUtil.hashCode(hash, inParameters.hashCode());
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
        }
        if(obj instanceof FunctionMethod) {
            FunctionMethod other = (FunctionMethod) obj;

            // Compare # of parameters - do this first as it is much faster than name compare
            if(getInputParameterCount() != other.getInputParameterCount()) {
                return false;
            }

            if (!EquivalenceUtil.areEqual(this.parent, other.getParent())) {
                return false;
            }

            if(! other.getName().equalsIgnoreCase(this.getName()) ) {
                return false;
            }

            // Compare types of parameters
            List<FunctionParameter> thisInputs = this.getInputParameters();
            if(thisInputs != null && thisInputs.size() > 0) {
                // If thisInputs is not null and >0 and other parameter
                // count matched this parameter count, otherInputs MUST be
                // non null to have more than one parameter - so we don't
                // need to check it here.
                List<FunctionParameter> otherInputs = other.getInputParameters();

                for(int i=0; i<thisInputs.size(); i++) {
                    boolean paramMatch = EquivalenceUtil.areEqual(thisInputs.get(i), otherInputs.get(i));
                    if(! paramMatch) {
                        return false;
                    }
                }
            }

            // Found no discrepancies, must be equal
            return true;
        }
        return false;
    }

    /**
     * Return string version for debugging purposes
     * @return String representation of function method
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        if(getName() != null) {
            str.append(getName());
        } else {
            str.append("<unknown>"); //$NON-NLS-1$
        }

        // Print parameters
        str.append("("); //$NON-NLS-1$
        if(inParameters != null) {
            for(int i=0; i<inParameters.size(); i++) {
                if(inParameters.get(i) != null) {
                    str.append(inParameters.get(i).toString());
                } else {
                    str.append("<unknown>"); //$NON-NLS-1$
                }

                if(i < (inParameters.size()-1)) {
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
     * Returns true if the function returns null on any null input
     */
    public boolean isNullOnNull() {
        return this.nullOnNull;
    }

    public void setNullOnNull(boolean nullOnNull) {
        this.nullOnNull = nullOnNull;
    }

    public Determinism getDeterminism() {
        return this.determinism;
    }

    public void setDeterministicBoolean(boolean deterministic) {
        this.determinism = deterministic ? Determinism.DETERMINISTIC : Determinism.NONDETERMINISTIC;
    }

    public void setDeterminism(Determinism determinism) {
        this.determinism = determinism;
    }

    public boolean isVarArgs() {
        if (this.inParameters != null && this.inParameters.size() > 0) {
            return inParameters.get(inParameters.size() - 1).isVarArg();
        }
        return false;
    }

    /**
     *
     * @param varargs
     * @return true if the value was successfully set.
     */
    public boolean setVarArgs(boolean varargs) {
        if (this.inParameters != null && this.inParameters.size() > 0) {
            inParameters.get(inParameters.size() - 1).setVarArg(varargs);
            return true;
        }
        return false;
    }

    public void setParent(Schema parent) {
        this.parent = parent;
    }

    @Override
    public Schema getParent() {
        return parent;
    }

    /**
     * Gets the {@link AggregateAttributes} for this function if it
     * represents an aggregate function.  Must be null for non-aggregates.
     * @return
     */
    public AggregateAttributes getAggregateAttributes() {
        return aggregateAttributes;
    }

    public void setAggregateAttributes(AggregateAttributes aggregateAttributes) {
        this.aggregateAttributes = aggregateAttributes;
    }

    public static FunctionMethod createFunctionMethod(String name, String description, String category,
            String returnType, String... paramTypes) {
        FunctionParameter[] params = new FunctionParameter[paramTypes.length];
        for (int i = 0; i < paramTypes.length; i++) {
            params[i] = new FunctionParameter("param" + (i+1), paramTypes[i]); //$NON-NLS-1$
        }
        FunctionMethod method = new FunctionMethod(name, description, category, params, new FunctionParameter("result", returnType)); //$NON-NLS-1$
        method.setNameInSource(name);
        return method;
    }

    public static void convertExtensionMetadata(Procedure procedureRecord,
            FunctionMethod function) {
        String deterministic = procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "deterministic", true); //$NON-NLS-1$
        boolean nullOnNull = Boolean.valueOf(procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "null-on-null", true)); //$NON-NLS-1$
        String varargs = procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "varargs", true); //$NON-NLS-1$
        String javaClass = procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "java-class", true); //$NON-NLS-1$
        String javaMethod = procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "java-method", true); //$NON-NLS-1$
        if (function.getInvocationClass() == null) {
            function.setInvocationClass(javaClass);
        }
        if (function.getInvocationMethod() == null) {
            function.setInvocationMethod(javaMethod);
        }
        if (!procedureRecord.getParameters().isEmpty()) {
            function.setProperties(procedureRecord.getProperties());
        }
        boolean aggregate = Boolean.valueOf(procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "aggregate", true)); //$NON-NLS-1$
        if (deterministic != null) {
            function.setDeterminism(Boolean.valueOf(deterministic)?Determinism.DETERMINISTIC:Determinism.NONDETERMINISTIC);
        }
        function.setNullOnNull(nullOnNull);
        if (varargs != null && !function.getInputParameters().isEmpty()) {
            function.getInputParameters().get(function.getInputParameterCount() - 1).setVarArg(Boolean.valueOf(varargs));
        }
        if (aggregate) {
            boolean analytic = Boolean.valueOf(procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "analytic", true)); //$NON-NLS-1$
            boolean allowsOrderBy = Boolean.valueOf(procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "allows-orderby", true)); //$NON-NLS-1$
            boolean usesDistinctRows = Boolean.valueOf(procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "uses-distinct-rows", true)); //$NON-NLS-1$
            boolean allowsDistinct = Boolean.valueOf(procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "allows-distinct", true)); //$NON-NLS-1$
            boolean decomposable = Boolean.valueOf(procedureRecord.getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "decomposable", true)); //$NON-NLS-1$
            AggregateAttributes aa = new AggregateAttributes();
            aa.setAnalytic(analytic);
            aa.setAllowsOrderBy(allowsOrderBy);
            aa.setUsesDistinctRows(usesDistinctRows);
            aa.setAllowsDistinct(allowsDistinct);
            aa.setDecomposable(decomposable);
            function.setAggregateAttributes(aa);
        }
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    @Override
    public String getIdentifier() {
        return this.getUUID();
    }

}
