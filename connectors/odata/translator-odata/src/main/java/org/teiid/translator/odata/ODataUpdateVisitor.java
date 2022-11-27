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
package org.teiid.translator.odata;

import java.util.ArrayList;
import java.util.List;

import org.odata4j.core.OCollection;
import org.odata4j.core.OCollections;
import org.odata4j.core.OObject;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.core.OSimpleObjects;
import org.odata4j.edm.EdmCollectionType;
import org.odata4j.edm.EdmProperty.CollectionKind;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.teiid.language.Array;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ODataUpdateVisitor extends ODataSQLVisitor {
    protected ODataExecutionFactory executionFactory;
    protected RuntimeMetadata metadata;
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private String method = "POST"; //$NON-NLS-1$
    private Table entity;
    private List<OProperty<?>> payload;
    private String uri;

    public ODataUpdateVisitor(ODataExecutionFactory executionFactory, RuntimeMetadata metadata) {
        super(executionFactory, metadata);
    }

    @Override
    public void visit(Insert obj) {
        this.method = "POST"; //$NON-NLS-1$
        this.entity = obj.getTable().getMetadataObject();
        this.uri = this.entity.getName();

        final List<OProperty<?>> props = new ArrayList<OProperty<?>>();
        int elementCount = obj.getColumns().size();
        for (int i = 0; i < elementCount; i++) {
            Column column = obj.getColumns().get(i).getMetadataObject();
            List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();
            OProperty<?> property = readProperty(column, values.get(i));
            props.add(property);
        }
        this.payload = props;
    }

    @Override
    public void visit(Update obj) {
        this.method = "PUT"; //$NON-NLS-1$
        this.entity = obj.getTable().getMetadataObject();
        visitNode(obj.getTable());

        // only pk are allowed, no other criteria not allowed
        obj.setWhere(buildEntityKey(obj.getWhere()));

        // this will build with entity keys
        this.uri = getEnitityURL();

        if (this.uri.indexOf('(') == -1) {
            this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17011, this.filter.toString())));
        }

        if (this.filter.length() > 0) {
            this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17009, this.filter.toString())));
        }

        final List<OProperty<?>> props = new ArrayList<OProperty<?>>();
        int elementCount = obj.getChanges().size();
        for (int i = 0; i < elementCount; i++) {
            Column column = obj.getChanges().get(i).getSymbol().getMetadataObject();
            OProperty<?> property = readProperty(column, obj.getChanges().get(i).getValue());
            props.add(property);
        }
        this.payload = props;
    }

    private OProperty<?> readProperty(Column column, Object value) {
        if (value instanceof Array) {
            EdmType componentType = ODataTypeManager.odataType(column.getRuntimeType());
            if (componentType instanceof EdmCollectionType) {
                componentType = ((EdmCollectionType)componentType).getItemType();
            }
            OCollection.Builder<OObject> b = OCollections.newBuilder(componentType);
            List<Expression> values = ((Array)value).getExpressions();
            for (int i = 0; i < values.size(); i++) {
                Literal literal = (Literal)values.get(i);
                b.add(OSimpleObjects.create((EdmSimpleType<?>)componentType, literal.getValue()));
            }
            return OProperties.collection(column.getName(),
                    new EdmCollectionType(CollectionKind.Collection,
                            componentType), b.build());
        } else {
            Literal literal = (Literal)value;
            return OProperties.simple(column.getName(), literal.getValue());
        }
    }

    @Override
    public void visit(Delete obj) {
        this.method = "DELETE"; //$NON-NLS-1$
        this.entity = obj.getTable().getMetadataObject();
        visitNode(obj.getTable());

        // only pk are allowed, no other criteria not allowed
        obj.setWhere(buildEntityKey(obj.getWhere()));

        // this will build with entity keys
        this.uri = getEnitityURL();
        if (this.uri.indexOf('(') == -1) {
            this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17011, this.filter.toString())));
        }

        if (this.filter.length() > 0) {
            this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17009, this.filter.toString())));
        }
    }

    public Table getTable() {
        return this.entity;
    }

    @Override
    public String buildURL() {
        return this.uri;
    }

    public String getMethod() {
        return this.method;
    }

    public List<OProperty<?>> getPayload() {
        return this.payload;
    }
}
