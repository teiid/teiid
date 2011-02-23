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
package org.teiid.translator.jdbc.ingres;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Limit;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.FunctionModifier;

@Translator(name="ingres93", description="A translator for Ingres 9.3 or later Database")
public class Ingres93ExecutionFactory extends IngresExecutionFactory {
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		convert.addTypeMapping("ansidate", FunctionModifier.DATE); //$NON-NLS-1$
		convert.addTypeMapping("timestamp(9) with time zone", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<?> translateLimit(Limit limit, ExecutionContext context) {
		if (limit.getRowOffset() > 0) {
	        return Arrays.asList("OFFSET ", limit.getRowOffset(), " FETCH FIRST ", limit.getRowLimit(), " ROWS ONLY"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}
		return super.translateLimit(limit, context);
	}
	
	@Override
	public boolean supportsRowOffset() {
		return true;
	}
	
}
