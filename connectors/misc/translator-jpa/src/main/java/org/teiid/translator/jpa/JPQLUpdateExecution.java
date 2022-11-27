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
package org.teiid.translator.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.teiid.core.TeiidException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class JPQLUpdateExecution extends JPQLBaseExecution implements UpdateExecution {
    private Command command;
    private int[] results;

    public JPQLUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, EntityManager em) {
        super(executionContext, metadata, em);
        this.command = command;
    }

    @Override
    public void execute() throws TranslatorException {
        if (this.command instanceof Insert) {
            this.results = new int[1];
            Object entity = handleInsert((Insert)this.command);
            this.enityManager.merge(entity);
            this.results[0] = 1;
        }
        else {
            // update or delete
            this.results = new int[1];
            this.results[0] = executeUpdate(this.command);
        }
    }

    private int executeUpdate(Command cmd) throws TranslatorException {
        String jpql = JPQLUpdateQueryVisitor.getJPQLString(cmd);
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "JPA Source-Query:", jpql); //$NON-NLS-1$
        Query query = this.enityManager.createQuery(jpql);
        return query.executeUpdate();
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        return this.results;
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    private Object handleInsert(Insert insert) throws TranslatorException {
        try {
            String entityClassName = insert.getTable().getMetadataObject().getProperty(JPAMetadataProcessor.ENTITYCLASS, false);
            Object entity = ReflectionHelper.create(entityClassName, null, this.executionContext.getCommandContext().getVDBClassLoader());

            List<ColumnReference> columns = insert.getColumns();
            List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
            if(columns.size() != values.size()) {
                throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14007));
            }
            for(int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i).getMetadataObject();
                Object value = values.get(i);

                // do not add the derived columns
                String name = column.getProperty(JPAMetadataProcessor.KEY_ASSOSIATED_WITH_FOREIGN_TABLE, false);
                if (name == null) {
                    if(value instanceof Literal) {
                        Literal literalValue = (Literal)value;
                        PropertiesUtils.setBeanProperty(entity, column.getName(), literalValue.getValue());
                    } else {
                        PropertiesUtils.setBeanProperty(entity, column.getName(), value);
                    }
                }
            }
            return entity;
        } catch (TeiidException e) {
            throw new TranslatorException(e);
        }
    }
}
