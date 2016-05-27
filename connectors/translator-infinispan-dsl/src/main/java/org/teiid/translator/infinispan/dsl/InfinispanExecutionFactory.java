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

package org.teiid.translator.infinispan.dsl;

import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.infinispan.dsl.metadata.AnnotationMetadataProcessor;
import org.teiid.translator.infinispan.dsl.metadata.ProtobufMetadataProcessor;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;



/**
 * InfinispanExecutionFactory is the translator that will be use to translate  a remote Infinispan cache and issue queries
 * using DSL to query the cache.  
 * 
 * @author vhalbert
 * 
 * @since 8.7
 *
 */
@Translator(name = "infinispan-cache-dsl", description = "The Infinispan Translator Using DSL to Query Cache")
public class InfinispanExecutionFactory extends ObjectExecutionFactory {

	// max available without having to try to override 
	// BooleanQuery.setMaxClauseCount(), and
	// infinispan doesn't currently support that option.
	// https://issues.jboss.org/browse/ISPN-6728
	public static final int MAX_SET_SIZE = 1024;
	
	private boolean supportsCompareCriteriaOrdered = false;
	
	public InfinispanExecutionFactory() {
		super();
		setSourceRequiredForMetadata(true);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(5);

		setSupportsOrderBy(true);
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(true);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(true);
		
		setSupportedJoinCriteria(SupportedJoinCriteria.KEY);
	}

	@Override
	public int getMaxFromGroups() {
		return 2;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectExecutionFactory#createDirectExecution(java.util.List, org.teiid.language.Command, org.teiid.translator.ExecutionContext, org.teiid.metadata.RuntimeMetadata, org.teiid.translator.object.ObjectConnection)
	 */
	@Override
	public ProcedureExecution createDirectExecution(List<Argument> arguments,
			Command command, ExecutionContext executionContext,
			RuntimeMetadata metadata, ObjectConnection connection)
			throws TranslatorException {
		return super.createDirectExecution(arguments, command, executionContext,
				metadata, connection);
	}
    

	@Override
    public boolean supportsAliasedTable() {
        return true;
    }

	@Override
    public boolean supportsInCriteria() {
		return Boolean.TRUE.booleanValue();
	}

	/**
	 * Discusses issue with trying to support IS NULL and IS NOT NULL;
	 * @return boolean
	 */
	 // "https://issues.jboss.org/browse/TEIID-3573"
	@Override
    public boolean supportsIsNullCriteria() {
		return Boolean.FALSE.booleanValue();
	}
	
	@Override
	public boolean supportsOrCriteria() {
		return Boolean.TRUE.booleanValue();
	}

	@Override
    public boolean supportsCompareCriteriaEquals() {
		return Boolean.TRUE.booleanValue();
	}

	@TranslatorProperty(display="CompareCriteriaOrdered", description="If true, translator can support comparison criteria with the operator '=>' or '<=' ",advanced=true)
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return supportsCompareCriteriaOrdered;
	}
	
	public boolean setSupportsCompareCriteriaOrdered(boolean supports) {
		return supportsCompareCriteriaOrdered = supports;
	}
	
	@Override
	public boolean supportsLikeCriteria() {
		return Boolean.TRUE.booleanValue();
	}	

	@Override
	public boolean supportsLikeCriteriaEscapeCharacter() {
		return Boolean.TRUE.booleanValue();
	}	
	
	/**
	 * Discusses issue with trying to support NOT;
	 */
	// https://issues.jboss.org/browse/TEIID-3573

	@Override
	public boolean supportsNotCriteria() {
		return Boolean.FALSE.booleanValue();
	}

	@Override
    public MetadataProcessor<ObjectConnection> getMetadataProcessor(){
		if (this.supportsSearchabilityUsingAnnotations()) {
			return new AnnotationMetadataProcessor(true);
		}
	    return new ProtobufMetadataProcessor();
	}

}
