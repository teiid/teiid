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
package org.teiid.translator.jdbc.ucanaccess;


import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.hsql.HsqlExecutionFactory;

@Translator(name="ucanaccess", description="A translator for read/write Microsoft Access Database")
public class UCanAccessExecutionFactory extends HsqlExecutionFactory {
	
	public static final String UCANACCESS = "ucanaccess"; //$NON-NLS-1$
	
	public UCanAccessExecutionFactory() {
		setSupportsOrderBy(true);
		setMaxInCriteriaSize(JDBCExecutionFactory.DEFAULT_MAX_IN_CRITERIA);
		setMaxDependentInPredicates(10);
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
		
		addPushDownFunction(UCANACCESS, "DCount", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(UCANACCESS, "DSum", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(UCANACCESS, "DMax", TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(UCANACCESS, "DMin", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(UCANACCESS, "DAvg", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(UCANACCESS, "DFirst", TypeFacility.RUNTIME_NAMES.OBJECT, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
		addPushDownFunction(UCANACCESS, "DLast", TypeFacility.RUNTIME_NAMES.OBJECT, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
	}
	
    @Override
    public boolean supportsDependentJoins() {
    	return false;
    }
}
