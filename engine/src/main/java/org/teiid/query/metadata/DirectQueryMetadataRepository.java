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
package org.teiid.query.metadata;

import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

/**
 * This Metadata repository adds the "native" procedure to all the execution factories that support them. 
 */
public class DirectQueryMetadataRepository extends MetadataRepository {
	
	@Override
	public void loadMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory) throws TranslatorException {

		if (executionFactory != null && executionFactory.supportsDirectQueryProcedure()) {
			Procedure p = factory.addProcedure(executionFactory.getDirectQueryProcedureName());
			p.setAnnotation("Invokes translator with a native query that returns results in array of values"); //$NON-NLS-1$

			ProcedureParameter param = factory.addProcedureParameter("request", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
			param.setAnnotation("The native query to execute"); //$NON-NLS-1$
			param.setNullType(NullType.No_Nulls);

			param = factory.addProcedureParameter("variable", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p); //$NON-NLS-1$
			param.setAnnotation("Any number of varaibles; usage will vary by translator"); //$NON-NLS-1$
			param.setNullType(NullType.Nullable);
			param.setVarArg(true);
			
			factory.addProcedureResultSetColumn("tuple", TypeFacility.RUNTIME_NAMES.OBJECT, p); //$NON-NLS-1$		
		}
	}	
}
