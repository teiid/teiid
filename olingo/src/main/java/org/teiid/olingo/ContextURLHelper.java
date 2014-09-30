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

import java.util.List;

import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceSingleton;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;

public class ContextURLHelper extends DefaultODataResourceURLHierarchyVisitor {
    private String entitySet;
    private String navPath;
    private boolean singleEntity;

    public String buildURL(UriInfo info) {
        visit(info);
        StringBuilder sb = new StringBuilder();
        sb.append(this.entitySet);
        if (!this.singleEntity && this.navPath != null) {
            sb.append(this.navPath);
        }
        return sb.toString();
    }

    @Override
    public void visit(UriResourceEntitySet info) {
        this.entitySet = info.getEntitySet().getName();
        if (info.getKeyPredicates() != null && info.getKeyPredicates().size() > 0) {
            this.navPath = buildEntityKey(info.getKeyPredicates());
            this.singleEntity = true;
        }
    }

    private String buildEntityKey(List<UriParameter> keys) {
        if (keys.size() == 1) {
            UriParameter key = keys.get(0);
            Literal literal = (Literal)key.getExpression();
            return "("+literal.getText()+")"; //$NON-NLS-1$ //$NON-NLS-2$
        }

        // complex (multi-keyed)
        StringBuilder sb = new StringBuilder();
        sb.append("("); //$NON-NLS-1$
        for (int i = 0; i < keys.size(); i++) {
            UriParameter key = keys.get(i);
            if (i > 0) {
                sb.append(","); //$NON-NLS-1$
            }
            sb.append(key.getName());
            sb.append("="); //$NON-NLS-1$
            Literal literal = (Literal)key.getExpression();
            sb.append(literal.getText());
        }
        sb.append(")"); //$NON-NLS-1$
        return sb.toString();
    }

    @Override
    public void visit(UriResourceNavigation info) {
        // typically navigation only happens in $entity-id situations,
        EdmNavigationProperty property = info.getProperty();
        String navigationName = property.getName();
        this.entitySet = this.entitySet +this.navPath+"/"+navigationName; //$NON-NLS-1$
        this.navPath = null;
        this.singleEntity = false;

        List<UriParameter> keys = info.getKeyPredicates();
        if (keys != null && keys.size() > 0) {
            this.navPath = buildEntityKey(keys);
            this.singleEntity = true;
        }
    }

    @Override
    public void visit(UriResourceSingleton info) {
        this.entitySet = info.getSingleton().getName();
    }

    @Override
    public void visit(UriResourcePrimitiveProperty info) {
        if (info.isCollection()) {
            this.entitySet = "Collection("+info.getProperty().getType().toString()+")"; //$NON-NLS-1$ //$NON-NLS-2$
        }
        this.entitySet = info.getProperty().getType().toString();
        this.navPath = null;
    }

    /*
    @Override
    public void visit(SelectOption option) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < option.getSelectItems().size(); i++) {
            SelectItem si = option.getSelectItems().get(i);
            if (i > 0) {
                sb.append(","); //$NON-NLS-1$
            }
            UriResource resource = ResourcePropertyCollector.getUriResource(si.getResourcePath());
            if (resource.getKind() == UriResourceKind.primitiveProperty) {
                UriResourcePrimitiveProperty primitiveProp = (UriResourcePrimitiveProperty)resource;
                sb.append(primitiveProp.getProperty().getName());
            }
        }
        this.navPath = sb.toString();
    }
    */
}
