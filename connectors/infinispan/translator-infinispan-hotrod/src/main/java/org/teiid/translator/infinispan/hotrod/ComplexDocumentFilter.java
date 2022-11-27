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
package org.teiid.translator.infinispan.hotrod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.infinispan.api.DocumentFilter;
import org.teiid.infinispan.api.MarshallerBuilder;
import org.teiid.infinispan.api.ProtobufMetadataProcessor;
import org.teiid.language.NamedTable;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.translator.TranslatorException;

public class ComplexDocumentFilter implements DocumentFilter {

    private NamedTable parentTable;
    private NamedTable childTable;
    private Criteria criteria;
    private RuntimeMetadata metadata;
    private Map<ElementSymbol, Integer> elementMap = new HashMap<>();
    private Action action;
    private String childName;

    public ComplexDocumentFilter(NamedTable parentTable, NamedTable childTable, RuntimeMetadata metadata, String filter,
            Action action) throws TranslatorException {
        this.parentTable = parentTable;
        this.childTable = childTable;
        this.metadata = metadata;
        this.action = action;
        this.childName = ProtobufMetadataProcessor.getMessageName(childTable.getMetadataObject());

        int i = 0;
        for (Column column : parentTable.getMetadataObject().getColumns()) {
            GroupSymbol gs = new GroupSymbol(parentTable.getCorrelationName());
            gs.setMetadataID(parentTable.getMetadataObject());
            elementMap.put(new ElementSymbol(column.getName(), gs), i++);
        }

        for (Column column : childTable.getMetadataObject().getColumns()) {
            GroupSymbol gs = new GroupSymbol(childTable.getCorrelationName());
            gs.setMetadataID(childTable.getMetadataObject());
            elementMap.put(new ElementSymbol(column.getName(), gs), i++);
        }
        try {
            this.criteria = QueryParser.getQueryParser().parseCriteria(filter);
        } catch (QueryParserException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public boolean matches(Map<String, Object> parentProperties, Map<String, Object> childProperties)
            throws TranslatorException {
        try {
            List<Object> tuple = new ArrayList<>();
            int i = 0;
            for (Column column : parentTable.getMetadataObject().getColumns()) {
                tuple.add(i++, parentProperties.get(MarshallerBuilder.getDocumentAttributeName(column, false, metadata)));
            }

            for (Column column : childTable.getMetadataObject().getColumns()) {
                if (ProtobufMetadataProcessor.isPseudo(column)) {
                    Column parentColumn = IckleConversionVisitor.normalizePseudoColumn(column, this.metadata);
                    tuple.add(i++, parentProperties.get(MarshallerBuilder.getDocumentAttributeName(parentColumn, false, metadata)));
                } else {
                    tuple.add(i++, childProperties.get(MarshallerBuilder.getDocumentAttributeName(column, true, metadata)));
                }
            }
            org.teiid.query.util.CommandContext cc = new org.teiid.query.util.CommandContext();
            final Evaluator evaluator = new Evaluator(elementMap, null, cc);
            return evaluator.evaluate(criteria, tuple);
        } catch (ExpressionEvaluationException e) {
            throw new TranslatorException(e);
        } catch (BlockedException e) {
            throw new TranslatorException(e);
        } catch (TeiidComponentException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public Action action() {
        return this.action;
    }

    @Override
    public String getChildName() {
        return this.childName;
    }

}
