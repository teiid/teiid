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
package org.teiid.translator.jpa;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;

@Translator(name="jpa2", description="A translator for JPA2 based entities")
public class JPA2ExecutionFactory extends ExecutionFactory<EntityManagerFactory, EntityManager> {
	private Map<String, FunctionModifier> functionModifiers = new TreeMap<String, FunctionModifier>(String.CASE_INSENSITIVE_ORDER);
	
	public JPA2ExecutionFactory() {
		setSupportsNativeQueries(true);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		setSupportsInnerJoins(true);
		setSupportsOrderBy(true);
		setSupportsSelectDistinct(true);
		setSupportedJoinCriteria(SupportedJoinCriteria.KEY);
		setSupportsOuterJoins(true);
		
		registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("current_date")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("current_time")); //$NON-NLS-1$
	}
	
	@Override
	public EntityManager getConnection(EntityManagerFactory factory, ExecutionContext executionContext) throws TranslatorException {
		if (factory == null) {
			return null;
		}
		return factory.createEntityManager();
	}

	@Override
	public void closeConnection(EntityManager connection, EntityManagerFactory factory) {
		connection.close();
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, EntityManager connection) throws TranslatorException {
		return new JPQLQueryExecution(this, command, executionContext, metadata, connection);
	}

	@Override
	public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, EntityManager connection) throws TranslatorException {
		return super.createProcedureExecution(command, executionContext, metadata, connection);
	}

	@Override
	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, EntityManager connection) throws TranslatorException {
		return new JPQLUpdateExecution(command, executionContext, metadata, connection);
	}
	
	@Override
	public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, EntityManager connection) throws TranslatorException {
		 return new JPQLDirectQueryExecution(arguments, command, executionContext, metadata, connection);
	}	
	
	
	@Override
	public void getMetadata(MetadataFactory mf, EntityManager em) throws TranslatorException {
		JPAMetadataProcessor metadataProcessor = new JPAMetadataProcessor();
		PropertiesUtils.setBeanProperties(metadataProcessor, mf.getImportProperties(), "importer"); //$NON-NLS-1$
		metadataProcessor.getMetadata(mf, em);
	}


	@Override
	public boolean supportsSelectExpression() {
		return true;
	}

	@Override
	public boolean supportsAliasedTable() {
		return true;
	}

	@Override
	public boolean supportsInlineViews() {
		return true;
	}

	@Override
	public boolean supportsCompareCriteriaEquals() {
		return true;
	}

	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}

	@Override
	public boolean supportsLikeCriteria() {
		return true;
	}

	@Override
	public boolean supportsInCriteria() {
		return true;
	}

	@Override
	public boolean supportsInCriteriaSubquery() {
		return false;
	}

	@Override
	public boolean supportsIsNullCriteria() {
		return true;
	}

	@Override
	public boolean supportsOrCriteria() {
		return true;
	}

	@Override
	public boolean supportsNotCriteria() {
		return true;
	}

	@Override
	public boolean supportsExistsCriteria() {
		return true;
	}

	@Override
	public boolean supportsGroupBy() {
		return true;
	}

	@Override
	public boolean supportsHaving() {
		return true;
	}

	@Override
	public boolean supportsAggregatesSum() {
		return true;
	}

	@Override
	public boolean supportsAggregatesAvg() {
		return true;
	}

	@Override
	public boolean supportsAggregatesMin() {
		return true;
	}

	@Override
	public boolean supportsAggregatesMax() {
		return true;
	}

	@Override
	public boolean supportsAggregatesCount() {
		return true;
	}

	@Override
	public boolean supportsAggregatesCountStar() {
		return true;
	}

	@Override
	public boolean supportsAggregatesDistinct() {
		return true;
	}

	@Override
	public boolean supportsScalarSubqueries() {
		return true;
	}
	
	@Override
	public boolean useAnsiJoin() {
		return true;
	}

	@Override
	public boolean supportsDependentJoins() {
		return true;
	}

	@Override
    public boolean supportsSelfJoins() {
    	return true;
    }	

	@Override
	public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(getDefaultSupportedFunctions());

        // String functions
        supportedFunctions.add(SourceSystemFunctions.CONCAT); 
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING); 
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.LCASE); 
        supportedFunctions.add(SourceSystemFunctions.UCASE); 
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);

        // airthamatic functions
        supportedFunctions.add(SourceSystemFunctions.ABS); 
        supportedFunctions.add(SourceSystemFunctions.SQRT); 
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);
        
        return supportedFunctions;
	}

    /**
     * Return a map of function name to FunctionModifier.
     * @return Map of function name to FunctionModifier.
     */
    public Map<String, FunctionModifier> getFunctionModifiers() {
    	return functionModifiers;
    }
    
    /**
     * Add the {@link FunctionModifier} to the set of known modifiers.
     * @param name
     * @param modifier
     */
    public void registerFunctionModifier(String name, FunctionModifier modifier) {
    	this.functionModifiers.put(name, modifier);
    }	
	
	
	public List<String> getDefaultSupportedFunctions(){
		return Arrays.asList(new String[] { "+", "-", "*", "/" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
}
