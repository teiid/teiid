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
package org.teiid.translator.infinispan.hotrod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
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

    public ComplexDocumentFilter(NamedTable parentTable, NamedTable childTable, RuntimeMetadata metadata, String filter,
            Action action) throws TranslatorException {
        this.parentTable = parentTable;
        this.childTable = childTable;
        this.metadata = metadata;
        this.action = action;

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
                tuple.add(i++, childProperties.get(MarshallerBuilder.getDocumentAttributeName(column, true, metadata)));
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
}
