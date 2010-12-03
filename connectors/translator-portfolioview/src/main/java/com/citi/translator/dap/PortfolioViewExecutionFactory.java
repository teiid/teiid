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

package com.citi.translator.dap;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Call;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;

@Translator(name="portfolioview", description="A translator for creating a portfolio view")
public class PortfolioViewExecutionFactory extends ExecutionFactory<ConnectionFactory, Connection> {
	
	private String portfolioName;

	@TranslatorProperty(display="portfolio name", description="Portfolio View name")
	public String getPortfolioName() {
		return this.portfolioName;
	}

	public void setPortfolioName(String portfolio) {
		this.portfolioName = portfolio;
	}
	
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection connection)
    		throws TranslatorException {
    	throw new TranslatorException("This is view model, no executions allowed"); //$NON-NLS-1$
    }
    
	@Override
    public final List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory,	Connection conn) throws TranslatorException {
		Table t = createView(metadataFactory, "pg_attribute"); //$NON-NLS-1$ 

		metadataFactory.addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		metadataFactory.addPrimaryKey("pk_pg_attr", Arrays.asList("oid"), t); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "select * from foo"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
	}
	
	
	private Table createView(MetadataFactory metadataFactory, String name) throws TranslatorException {
		Table t = metadataFactory.addTable(name);
		t.setSupportsUpdate(false);
		t.setVirtual(true);
		t.setTableType(Type.Table);
		return t;
	}
}
