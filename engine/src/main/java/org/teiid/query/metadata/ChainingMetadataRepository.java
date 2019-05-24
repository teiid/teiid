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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.TableStats;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class ChainingMetadataRepository implements MetadataRepository<Object, Object> {

    final ArrayList<MetadataRepository<Object, Object>> repositories;

    @SuppressWarnings("unchecked")
    public ChainingMetadataRepository(List<? extends MetadataRepository> repos) {
        this.repositories = new ArrayList<MetadataRepository<Object,Object>>((Collection<? extends MetadataRepository<Object, Object>>)repos);
    }

    @Override
    public void loadMetadata(MetadataFactory factory,
            ExecutionFactory<Object, Object> executionFactory,
            Object connectionFactory) throws TranslatorException {
        for (MetadataRepository<Object, Object> repo : repositories) {
            repo.loadMetadata(factory, executionFactory, connectionFactory);
        }
    }

    @Override
    public void setColumnStats(String vdbName, String vdbVersion, Column column,
            ColumnStats columnStats) {
        for (MetadataRepository<Object, Object> repo : repositories) {
            repo.setColumnStats(vdbName, vdbVersion, column, columnStats);
        }
    }

    @Override
    public void setInsteadOfTriggerDefinition(String vdbName, String vdbVersion,
            Table table, TriggerEvent triggerOperation, String triggerDefinition) {
        for (MetadataRepository<Object, Object> repo : repositories) {
            repo.setInsteadOfTriggerDefinition(vdbName, vdbVersion, table, triggerOperation, triggerDefinition);
        }
    }

    @Override
    public void setInsteadOfTriggerEnabled(String vdbName, String vdbVersion,
            Table table, TriggerEvent triggerOperation, boolean enabled) {
        for (MetadataRepository<Object, Object> repo : repositories) {
            repo.setInsteadOfTriggerEnabled(vdbName, vdbVersion, table, triggerOperation, enabled);
        }
    }

    @Override
    public void setProcedureDefinition(String vdbName, String vdbVersion,
            Procedure procedure, String procedureDefinition) {
        for (MetadataRepository<Object, Object> repo : repositories) {
            repo.setProcedureDefinition(vdbName, vdbVersion, procedure, procedureDefinition);
        }
    }

    @Override
    public void setProperty(String vdbName, String vdbVersion,
            AbstractMetadataRecord record, String name, String value) {
        for (MetadataRepository<Object, Object> repo : repositories) {
            repo.setProperty(vdbName, vdbVersion, record, name, value);
        }
    }

    @Override
    public void setTableStats(String vdbName, String vdbVersion, Table table,
            TableStats tableStats) {
        for (MetadataRepository<Object, Object> repo : repositories) {
            repo.setTableStats(vdbName, vdbVersion, table, tableStats);
        }
    }

    @Override
    public void setViewDefinition(String vdbName, String vdbVersion, Table table,
            String viewDefinition) {
        for (MetadataRepository<Object, Object> repo : repositories) {
            repo.setViewDefinition(vdbName, vdbVersion, table, viewDefinition);
        }
    }

}
