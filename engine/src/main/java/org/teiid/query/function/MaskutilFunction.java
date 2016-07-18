package org.teiid.query.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.function.source.SecuritySystemFunctions;
import org.teiid.query.function.source.XMLSystemFunctions;

public class MaskutilFunction extends UDFSource implements FunctionCategoryConstants {

	private static final String FUNCTION_CLASS = Maskutil.class.getName();
	private static final String XML_FUNCTION_CLASS = XMLSystemFunctions.class.getName();
	private static final String SECURITY_FUNCTION_CLASS = SecuritySystemFunctions.class.getName();

	public MaskutilFunction(boolean allowEnvFunction) {
		super(new ArrayList<FunctionMethod>());
		// TODO Auto-generated constructor stub
		addRandom();
		addHash();
		addDigit();
	}

	private void addRandom() {
		functions.add(new FunctionMethod("random", "testUDF description", STRING, FUNCTION_CLASS, "toRandomValue",
				new FunctionParameter[] {
						new FunctionParameter("sourceValue", DataTypeManager.DefaultDataTypes.STRING, "String") }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "testUDF return data")));
	}

	private void addHash() {
		functions.add(new FunctionMethod("hash", "testUDF description", STRING, FUNCTION_CLASS, "toHashValue",
				new FunctionParameter[] {
						new FunctionParameter("sourceValue", DataTypeManager.DefaultDataTypes.STRING, "String") }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "testUDF return data")));
	}

	private void addDigit() {
		functions.add(new FunctionMethod("digit", "testUDF description", STRING, FUNCTION_CLASS, "toDigitValue",
				new FunctionParameter[] {
						new FunctionParameter("sourceValue", DataTypeManager.DefaultDataTypes.STRING, "String") }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "testUDF return data")));
	}

	 
	/**
     * Get all function signatures for this metadata source.
     * @return Unordered collection of {@link FunctionMethod}s
     */
    @Override
	public Collection<org.teiid.metadata.FunctionMethod> getFunctionMethods() {
        return this.functions;
	}
    
    public static FunctionMethod createSyntheticMethod(String name, String description, String category, 
            String invocationClass, String invocationMethod, FunctionParameter[] inputParams, 
            FunctionParameter outputParam) {
    	return new FunctionMethod(name, description, category, PushDown.SYNTHETIC, invocationClass, invocationMethod, inputParams!=null?Arrays.asList(inputParams):null, outputParam, false,Determinism.NONDETERMINISTIC);
    }

}
