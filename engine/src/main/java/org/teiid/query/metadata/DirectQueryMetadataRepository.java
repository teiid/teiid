/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.query.metadata;

import org.teiid.core.types.DataTypeManager;
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
public class DirectQueryMetadataRepository implements MetadataRepository {

    @Override
    public void loadMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory) throws TranslatorException {

        if (executionFactory != null && executionFactory.supportsDirectQueryProcedure() && factory.getSchema().getProcedure(executionFactory.getDirectQueryProcedureName()) == null) {
            Procedure p = factory.addProcedure(executionFactory.getDirectQueryProcedureName());
            p.setAnnotation("Invokes translator with a native query that returns results in an array of values"); //$NON-NLS-1$

            ProcedureParameter param = factory.addProcedureParameter("request", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
            param.setAnnotation("The native query to execute"); //$NON-NLS-1$
            param.setNullType(NullType.No_Nulls);

            param = factory.addProcedureParameter("variable", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p); //$NON-NLS-1$
            param.setAnnotation("Any number of varaibles; usage will vary by translator"); //$NON-NLS-1$
            param.setNullType(NullType.Nullable);
            param.setVarArg(true);

            factory.addProcedureResultSetColumn("tuple", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(TypeFacility.RUNTIME_TYPES.OBJECT)), p); //$NON-NLS-1$
        }
    }
}
