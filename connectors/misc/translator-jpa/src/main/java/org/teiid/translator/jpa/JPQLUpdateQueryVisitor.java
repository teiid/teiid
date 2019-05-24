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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.translator.TranslatorException;

public class JPQLUpdateQueryVisitor extends HierarchyVisitor {
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    protected AtomicInteger aliasCounter = new AtomicInteger(0);
    protected HashMap<String, String> correlatedName = new HashMap<String, String>();

    public JPQLUpdateQueryVisitor() {
        super(false);
    }

    public static String getJPQLString(Command obj)  throws TranslatorException {
        JPQLUpdateQueryVisitor visitor = new JPQLUpdateQueryVisitor();

        visitor.visitNode(obj);

        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        return visitor.convertToQuery(obj);
    }

    private String convertToQuery(Command obj) {
        JPQLUpdateStringVisitor visitor = new JPQLUpdateStringVisitor(this);
        visitor.visitNode(obj);
        return visitor.toString();
    }

    @Override
    public void visit(NamedTable obj) {
        if (obj.getCorrelationName() == null) {
            String aliasName = "ql_"+this.aliasCounter.getAndIncrement(); //$NON-NLS-1$
            this.correlatedName.put(obj.getMetadataObject().getName(), aliasName);
            obj.setCorrelationName(aliasName);
        }
        else {
            this.correlatedName.put(obj.getMetadataObject().getName(), obj.getCorrelationName());
        }
    }

    static class JPQLUpdateStringVisitor extends SQLStringVisitor {
        private JPQLUpdateQueryVisitor visitor;

        public JPQLUpdateStringVisitor(JPQLUpdateQueryVisitor visitor) {
            this.visitor = visitor;
        }

        @Override
        public void visit(ColumnReference column) {
            AbstractMetadataRecord record = column.getMetadataObject();
            if (record != null) {
                String name = record.getProperty(JPAMetadataProcessor.KEY_ASSOSIATED_WITH_FOREIGN_TABLE, false);
                if (name == null) {
                    buffer.append(this.visitor.correlatedName.get(column.getTable().getMetadataObject().getName())).append(Tokens.DOT).append(column.getMetadataObject().getName());
                }
                else {
                    buffer.append(this.visitor.correlatedName.get(name)).append(Tokens.DOT).append(column.getMetadataObject().getName());
                }
            }
            else {
                buffer.append(column.getName());
            }
        }
    }
}
