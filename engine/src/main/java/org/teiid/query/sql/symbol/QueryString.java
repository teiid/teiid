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

import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

/**
 * Represents query string name value pairs
 */
public class QueryString implements Expression {

    private static final long serialVersionUID = -3348922701950966494L;
    private List<DerivedColumn> args;
    private Expression path;

    public QueryString(Expression path, List<DerivedColumn> args) {
        this.args = args;
        this.path = path;
    }

    public List<DerivedColumn> getArgs() {
        return args;
    }

    @Override
    public QueryString clone() {
        QueryString clone = new QueryString((Expression)path.clone(), LanguageObject.Util.deepClone(args, DerivedColumn.class));
        return clone;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof QueryString)) {
            return false;
        }
        QueryString other = (QueryString)obj;
        return path.equals(other.path) && args.equals(other.args);
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public Expression getPath() {
        return path;
    }

    public void setPath(Expression path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    @Override
    public Class<?> getType() {
        return DefaultDataClasses.STRING;
    }

}
