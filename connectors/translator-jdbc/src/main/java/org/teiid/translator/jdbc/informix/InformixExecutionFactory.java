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

/*
 */
package org.teiid.translator.jdbc.informix;

import java.util.ArrayList;
import java.util.List;

import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;


@Translator(name="informix", description="A translator for Informix Database")
public class InformixExecutionFactory extends JDBCExecutionFactory {

	private ConvertModifier convertModifier;

	@Override
	public void start() throws TranslatorException {
		super.start();
		
        convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
        convertModifier.addTypeMapping("smallint", FunctionModifier.BYTE, FunctionModifier.SHORT); //$NON-NLS-1$
        convertModifier.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("int8", FunctionModifier.LONG); //$NON-NLS-1$
        convertModifier.addTypeMapping("decimal(32,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convertModifier.addTypeMapping("smallfloat", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("float", FunctionModifier.DOUBLE); //$NON-NLS-1$
        convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
    	convertModifier.addTypeMapping("datetime", FunctionModifier.TIME, FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	convertModifier.addTypeMapping("varchar(255)", FunctionModifier.STRING); //$NON-NLS-1$
    	convertModifier.addTypeMapping("varchar(1)", FunctionModifier.CHAR); //$NON-NLS-1$
    	convertModifier.addTypeMapping("byte", FunctionModifier.VARBINARY); //$NON-NLS-1$
    	convertModifier.addTypeMapping("blob", FunctionModifier.BLOB); //$NON-NLS-1$
    	convertModifier.addTypeMapping("clob", FunctionModifier.CLOB); //$NON-NLS-1$

    	convertModifier.setWideningNumericImplicit(true);
    	registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
    }

	@Override
    public List getSupportedFunctions() {
        List supportedFunctons = new ArrayList();
        supportedFunctons.addAll(super.getSupportedFunctions());
        supportedFunctons.add("CAST"); //$NON-NLS-1$
        supportedFunctons.add("CONVERT"); //$NON-NLS-1$
        return supportedFunctons;
    }
	
	@Override
	public boolean supportsConvert(int fromType, int toType) {
		if (!super.supportsConvert(fromType, toType)) {
    		return false;
    	}
    	if (convertModifier.hasTypeMapping(toType)) {
    		return true;
    	}
    	return false;
	}
	
	@Override
	public boolean hasTimeType() {
		return false;
	}
}
