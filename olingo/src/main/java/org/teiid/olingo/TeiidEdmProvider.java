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

import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.olingo.server.api.edm.provider.Schema;
import org.apache.olingo.server.api.edm.provider.Singleton;
import org.apache.olingo.server.api.edm.provider.Term;
import org.apache.olingo.server.api.edm.provider.TypeDefinition;

public class TeiidEdmProvider extends EdmProvider {
	
	Collection<Schema> edmSchemas;

	public TeiidEdmProvider(Collection<Schema> edmSchemas) {
		this.edmSchemas = edmSchemas;
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
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				return schema.getFunctions();
			}
		}
		return null;
	}

	@Override
	public Term getTerm(FullQualifiedName termName) throws ODataException {
		return super.getTerm(termName);
	}

	@Override
	public EntitySet getEntitySet(FullQualifiedName fqn, String entitySetName) throws ODataException {
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				EntityContainer ec = schema.getEntityContainer();
				for (EntitySet es : ec.getEntitySets()) {
					if (es.getName().equals(entitySetName)) {
						return es;
					}
				}
			}
		}
		return null;
	}

	@Override
	public Singleton getSingleton(FullQualifiedName fqn,
			String singletonName) throws ODataException {
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				EntityContainer ec = schema.getEntityContainer();
				for (Singleton es : ec.getSingletons()) {
					if (es.getName().equals(singletonName)) {
						return es;
					}
				}
			}
		}
		return null;
	}

	@Override
	public ActionImport getActionImport(FullQualifiedName fqn,
			String actionImportName) throws ODataException {
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				EntityContainer ec = schema.getEntityContainer();
				for (ActionImport es : ec.getActionImports()) {
					if (es.getName().equals(actionImportName)) {
						return es;
					}
				}
			}
		}
		return null;
	}

	@Override
	public FunctionImport getFunctionImport(FullQualifiedName fqn,
			String functionImportName) throws ODataException {
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				EntityContainer ec = schema.getEntityContainer();
				for (FunctionImport es : ec.getFunctionImports()) {
					if (es.getName().equals(functionImportName)) {
						return es;
					}
				}
			}
		}
		return null;
	}

	@Override
	public EntityContainerInfo getEntityContainerInfo(FullQualifiedName fqn) throws ODataException {
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				EntityContainer ec = schema.getEntityContainer();
				EntityContainerInfo info = new EntityContainerInfo();
				info.setContainerName(new FullQualifiedName(schema.getNamespace(), fqn.getName()));
				return info;
			}
		}
		return null;
	}

	@Override
	public List<AliasInfo> getAliasInfos() throws ODataException {
		return super.getAliasInfos();
	}

	@Override
	public EntityContainer getEntityContainer() throws ODataException {
		throw new ODataException("bad method");
	}

	public List<Schema> getSchemas() throws ODataException {
		return new ArrayList<Schema>(this.edmSchemas);
	}

	public EntityType getEntityType(final FullQualifiedName fqn)
			throws ODataException {
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				for (EntityType type : schema.getEntityTypes()) {
					if (type.getName().equals(fqn.getName())) {
						return type;
					}
				}
			}
		}
		return null;
	}

	public ComplexType getComplexType(final FullQualifiedName fqn)
			throws ODataException {
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				for (ComplexType type : schema.getComplexTypes()) {
					if (type.getName().equals(fqn.getName())) {
						return type;
					}
				}
			}
		}
		return null;
	}
	
	public List<Action> getActions(final FullQualifiedName fqn)
			throws ODataException {
		for (Schema schema : this.edmSchemas) {
			if (schema.getNamespace().equals(fqn.getNamespace())) {
				return schema.getActions();
			}
		}
		return null;
	}	
}
