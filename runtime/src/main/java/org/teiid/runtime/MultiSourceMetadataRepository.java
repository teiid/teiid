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

package org.teiid.runtime;

import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class MultiSourceMetadataRepository implements MetadataRepository<Object, Object> {

    private String multiSourceColumnName;

    public MultiSourceMetadataRepository(String multiSourceColumnName) {
        this.multiSourceColumnName = multiSourceColumnName;
    }

    @Override
    public void loadMetadata(MetadataFactory factory,
            ExecutionFactory<Object, Object> executionFactory, Object connectionFactory)
            throws TranslatorException {
        Schema s = factory.getSchema();
        for (Table t : s.getTables().values()) {
            if (!t.isPhysical()) {
                continue;
            }
            Column c = t.getColumnByName(multiSourceColumnName);
            if (c == null) {
                c = factory.addColumn(multiSourceColumnName, DataTypeManager.DefaultDataTypes.STRING, t);
                MultiSourceMetadataWrapper.setMultiSourceElementMetadata(c);
            }
        }
        outer: for (Procedure p : s.getProcedures().values()) {
            if (p.isVirtual()) {
                continue;
            }
            for (ProcedureParameter pp : p.getParameters()) {
                if (multiSourceColumnName.equalsIgnoreCase(pp.getName())) {
                    continue outer;
                }
            }
            ProcedureParameter pp = factory.addProcedureParameter(multiSourceColumnName, DataTypeManager.DefaultDataTypes.STRING, Type.In, p);
            pp.setNullType(NullType.Nullable);
        }
    }

}
