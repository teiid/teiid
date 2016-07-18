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
	 
	public MaskutilFunction(boolean allowEnvFunction) {
		super(new ArrayList<FunctionMethod>());
		// TODO Auto-generated constructor stub
		addRandom();
		addHash();
		addDigit();
	}

	private void addRandom() {
		functions.add(new FunctionMethod("random", "randomize the string", STRING, FUNCTION_CLASS, "toRandomValue",
				new FunctionParameter[] {
						new FunctionParameter("sourceValue", DataTypeManager.DefaultDataTypes.STRING, "String") }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "The string after randomize")));
	}

	private void addHash() {
		functions.add(new FunctionMethod("hash", "Get the hash code of string", STRING, FUNCTION_CLASS, "toHashValue",
				new FunctionParameter[] {
						new FunctionParameter("sourceValue", DataTypeManager.DefaultDataTypes.STRING, "String") }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING,"The hashcode of string")));
	}

	private void addDigit() {
		functions.add(new FunctionMethod("digit", "Get the digit characters of the string", STRING, FUNCTION_CLASS, "toDigitValue",
				new FunctionParameter[] {
						new FunctionParameter("sourceValue", DataTypeManager.DefaultDataTypes.STRING, "String") }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "digit characters of the string")));
	}

	 
	/**
     * Get all function signatures for this metadata source.
     * @return Unordered collection of {@link FunctionMethod}s
     */
    @Override
	public Collection<org.teiid.metadata.FunctionMethod> getFunctionMethods() {
        return this.functions;
	}
    

}
