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

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.PredicateCriteria;

public class XMLExists extends PredicateCriteria {

    private XMLQuery xmlQuery;

    public XMLExists(XMLQuery xmlQuery) {
        this.xmlQuery = xmlQuery;
    }

    public XMLQuery getXmlQuery() {
        return xmlQuery;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Object clone() {
        return new XMLExists(xmlQuery.clone());
    }

    @Override
    public int hashCode() {
        return xmlQuery.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof XMLExists)) {
            return false;
        }
        return xmlQuery.equals(((XMLExists)obj).getXmlQuery());
    }

}
