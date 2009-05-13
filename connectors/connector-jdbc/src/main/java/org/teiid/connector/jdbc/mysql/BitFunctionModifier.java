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

package org.teiid.connector.jdbc.mysql;

import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.translator.AliasModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;

public class BitFunctionModifier extends AliasModifier {
	private ILanguageFactory langFactory;

	public BitFunctionModifier(String alias, ILanguageFactory langFactory) {
		super(alias);
		this.langFactory = langFactory;
	}
	
	/**
	 * Wrap the renamed function in a convert back to integer
	 */
	@Override
	public IExpression modify(IFunction function) {
		return langFactory.createFunction(SourceSystemFunctions.CONVERT, 
				new IExpression[] {super.modify(function), langFactory.createLiteral(MySQLConvertModifier.SIGNED_INTEGER, TypeFacility.RUNTIME_TYPES.STRING)}, TypeFacility.RUNTIME_TYPES.INTEGER);
	}	

}
