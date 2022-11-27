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

package org.teiid.query.sql.symbol;

import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class XMLForest implements Expression {

    private List<DerivedColumn> args;
    private XMLNamespaces namespaces;

    public XMLForest(List<DerivedColumn> args) {
        this.args = args;
    }

    public XMLNamespaces getNamespaces() {
        return namespaces;
    }

    public void setNamespaces(XMLNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    public List<DerivedColumn> getArgs() {
        return args;
    }

    @Override
    public Class<?> getType() {
        return DataTypeManager.DefaultDataClasses.XML;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public XMLForest clone() {
        XMLForest clone = new XMLForest(LanguageObject.Util.deepClone(args, DerivedColumn.class));
        if (namespaces != null) {
            clone.namespaces = namespaces.clone();
        }
        return clone;
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(args.hashCode());
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof XMLForest)) {
            return false;
        }
        XMLForest other = (XMLForest)obj;
        return args.equals(other.args) && EquivalenceUtil.areEqual(namespaces, other.namespaces);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
