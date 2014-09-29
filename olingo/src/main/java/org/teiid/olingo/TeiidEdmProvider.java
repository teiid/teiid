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
package org.teiid.olingo;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.olingo.commons.api.ODataException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.server.api.edm.provider.Action;
import org.apache.olingo.server.api.edm.provider.ActionImport;
import org.apache.olingo.server.api.edm.provider.AliasInfo;
import org.apache.olingo.server.api.edm.provider.ComplexType;
import org.apache.olingo.server.api.edm.provider.EdmProvider;
import org.apache.olingo.server.api.edm.provider.EntityContainer;
import org.apache.olingo.server.api.edm.provider.EntityContainerInfo;
import org.apache.olingo.server.api.edm.provider.EntitySet;
import org.apache.olingo.server.api.edm.provider.EntityType;
import org.apache.olingo.server.api.edm.provider.EnumType;
import org.apache.olingo.server.api.edm.provider.Function;
import org.apache.olingo.server.api.edm.provider.FunctionImport;
import org.apache.olingo.server.api.edm.provider.Reference;
import org.apache.olingo.server.api.edm.provider.Reference.Include;
import org.apache.olingo.server.api.edm.provider.Schema;
import org.apache.olingo.server.api.edm.provider.Singleton;
import org.apache.olingo.server.api.edm.provider.Term;
import org.apache.olingo.server.api.edm.provider.TypeDefinition;
import org.teiid.metadata.MetadataStore;

public class TeiidEdmProvider extends EdmProvider {

    private final Schema edmSchema;
    private final MetadataStore metadataStore;

    public TeiidEdmProvider(MetadataStore metadataStore, Schema edmSchema) {
        this.edmSchema = edmSchema;
        this.metadataStore = metadataStore;
    }

    @Override
    public EnumType getEnumType(FullQualifiedName enumTypeName)
            throws ODataException {
        return super.getEnumType(enumTypeName);
    }

    @Override
    public TypeDefinition getTypeDefinition(FullQualifiedName typeDefinitionName)
            throws ODataException {
        return super.getTypeDefinition(typeDefinitionName);
    }

    @Override
    public List<Function> getFunctions(FullQualifiedName fqn)
            throws ODataException {
        return edmSchema.getFunctions();
    }

    @Override
    public Term getTerm(FullQualifiedName termName) throws ODataException {
        return super.getTerm(termName);
    }

    @Override
    public EntitySet getEntitySet(FullQualifiedName fqn, String entitySetName)
            throws ODataException {
        EntityContainer ec = this.edmSchema.getEntityContainer();
        if (ec.getEntitySets() != null) {
            for (EntitySet es : ec.getEntitySets()) {
                if (es.getName().equals(entitySetName)) {
                    return es;
                }
            }
        }
        return null;
    }

    @Override
    public Singleton getSingleton(FullQualifiedName fqn, String singletonName)
            throws ODataException {
        EntityContainer ec = this.edmSchema.getEntityContainer();
        if (ec.getSingletons() != null) {
            for (Singleton es : ec.getSingletons()) {
                if (es.getName().equals(singletonName)) {
                    return es;
                }
            }
        }
        return null;
    }

    @Override
    public ActionImport getActionImport(FullQualifiedName fqn,
            String actionImportName) throws ODataException {
        EntityContainer ec = edmSchema.getEntityContainer();
        if (ec.getActionImports() != null) {
            for (ActionImport es : ec.getActionImports()) {
                if (es.getName().equals(actionImportName)) {
                    return es;
                }
            }
        }
        return null;
    }

    @Override
    public FunctionImport getFunctionImport(FullQualifiedName fqn,
            String functionImportName) throws ODataException {
        EntityContainer ec = this.edmSchema.getEntityContainer();
        if (ec.getFunctionImports() != null) {
            for (FunctionImport es : ec.getFunctionImports()) {
                if (es.getName().equals(functionImportName)) {
                    return es;
                }
            }
        }
        return null;
    }

    @Override
    public EntityContainerInfo getEntityContainerInfo(FullQualifiedName fqn)
            throws ODataException {
        EntityContainerInfo info = new EntityContainerInfo();
        info.setContainerName(new FullQualifiedName(this.edmSchema
                .getNamespace(), this.edmSchema.getNamespace()));
        return info;
    }

    @Override
    public List<AliasInfo> getAliasInfos() throws ODataException {
        return super.getAliasInfos();
    }

    @Override
    public EntityContainer getEntityContainer() throws ODataException {
        return edmSchema.getEntityContainer();
    }

    @Override
    public List<Schema> getSchemas() throws ODataException {
        return Arrays.asList(this.edmSchema);
    }

    @Override
    public EntityType getEntityType(final FullQualifiedName fqn)
            throws ODataException {
        if (this.edmSchema.getEntityTypes() != null) {
            for (EntityType type : this.edmSchema.getEntityTypes()) {
                if (type.getName().equals(fqn.getName())) {
                    return type;
                }
            }
        }
        return null;
    }

    @Override
    public ComplexType getComplexType(final FullQualifiedName fqn)
            throws ODataException {
        if (this.edmSchema.getComplexTypes() != null) {
            for (ComplexType type : this.edmSchema.getComplexTypes()) {
                if (type.getName().equals(fqn.getName())) {
                    return type;
                }
            }
        }
        return null;
    }

    @Override
    public List<Action> getActions(final FullQualifiedName fqn)
            throws ODataException {
        return this.edmSchema.getActions();
    }

    @Override
    public List<Reference> getReferences() throws ODataException {
        List<Reference> references = new ArrayList<Reference>();
        try {
            for (org.teiid.metadata.Schema teiidSchema : metadataStore
                    .getSchemaList()) {
                Reference ref = new Reference();
                // TODO: this needs to be proper service URI based in coming URL
                ref.setUri(new URI("http//:teiid.org/" + teiidSchema.getName()));
                Include include = new Include(teiidSchema.getName());
                include.setAlias(teiidSchema.getName());
                ref.setIncludes(Arrays.asList(include));

                references.add(ref);
            }
        } catch (URISyntaxException e) {
            // TODO:What needs to be done?
        }
        return references;
    }
}
