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

package org.teiid.query.sql.lang;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;



public class Limit implements LanguageObject {

    public static String NON_STRICT = "NON_STRICT"; //$NON-NLS-1$

    private Expression offset;
    private Expression rowLimit;
    private boolean implicit;
    private boolean strict = true;

    public Limit(Expression offset, Expression rowLimit) {
        this.offset = offset;
        this.rowLimit = rowLimit;
    }

    private Limit() {

    }

    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    public boolean isStrict() {
        return strict;
    }

    public boolean isImplicit() {
        return implicit;
    }

    public void setImplicit(boolean implicit) {
        this.implicit = implicit;
    }

    public Expression getOffset() {
        return offset;
    }

    public void setOffset(Expression offset) {
        this.offset = offset;
    }

    public Expression getRowLimit() {
        return rowLimit;
    }

    public void setRowLimit(Expression rowLimit ) {
        this.rowLimit = rowLimit;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public int hashCode() {
        int h = HashCodeUtil.hashCode(0, offset);
        return HashCodeUtil.hashCode(h, rowLimit);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Limit)) {
            return false;
        }
        Limit other = (Limit)o;
        if (!EquivalenceUtil.areEqual(this.offset, other.offset)) {
            return false;
        }
        return EquivalenceUtil.areEqual(this.rowLimit, other.rowLimit);
    }

    public Limit clone() {
        Limit clone = new Limit();
        clone.implicit = this.implicit;
        clone.strict = this.strict;
        if (this.rowLimit != null) {
            clone.setRowLimit((Expression) this.rowLimit.clone());
        }
        if (this.offset != null) {
            clone.setOffset((Expression) this.offset.clone());
        }
        return clone;
    }

    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
}
