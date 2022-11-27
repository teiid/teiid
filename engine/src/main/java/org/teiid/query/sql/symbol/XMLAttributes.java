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

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents XMLATTRIBUTES name value pairs
 */
public class XMLAttributes implements LanguageObject {

    private static final long serialVersionUID = -3348922701950966494L;
    private List<DerivedColumn> args;

    public XMLAttributes(List<DerivedColumn> args) {
        this.args = args;
    }

    public List<DerivedColumn> getArgs() {
        return args;
    }

    @Override
    public XMLAttributes clone() {
        XMLAttributes clone = new XMLAttributes(LanguageObject.Util.deepClone(args, DerivedColumn.class));
        return clone;
    }

    @Override
    public int hashCode() {
        return args.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof XMLAttributes)) {
            return false;
        }
        XMLAttributes other = (XMLAttributes)obj;
        return args.equals(other.args);
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
