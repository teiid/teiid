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

import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.util.Assertion;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This class represents a reference (positional from the user query, or
 * to an element from another scope).  This reference may resolve to many different values
 * during evaluation.  For any particular bound value, it is treated as a constant.
 */
public class Reference implements Expression, ContextReference {

    public interface Constraint {
        public void validate(Object value) throws QueryValidatorException;
    }

    private boolean positional;
    private boolean optional;

    private int refIndex;
    private Class<?> type;

    private ElementSymbol expression;

    private Constraint constraint;

    /**
     * Constructor for a positional Reference.
     */
    public Reference(int refIndex) {
        this.refIndex = refIndex;
        this.positional = true;
    }

    public Constraint getConstraint() {
        return constraint;
    }

    public void setConstraint(Constraint constraint) {
        this.constraint = constraint;
    }

    /**
     * Constructor for an element Reference.
     */
    public Reference(ElementSymbol expression) {
        this.expression = expression;
        this.positional = false;
    }

    private Reference(Reference ref) {
        this.refIndex = ref.refIndex;
        this.positional = ref.positional;
        this.type = ref.type;
        if (ref.expression != null) {
            this.expression = ref.expression.clone();
        }
        this.constraint = ref.constraint;
        this.optional = ref.optional;
    }

    public int getIndex() {
        return this.refIndex;
    }

    @Override
    public String getContextSymbol() {
        return "$param/pos" + this.refIndex; //$NON-NLS-1$
    }

    public ElementSymbol getExpression() {
        return this.expression;
    }

    public Class<?> getType() {
        if (this.isPositional() && this.expression == null) {
            return type;
        }
        return expression.getType();
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        return new Reference(this);
    }

    /**
     * Compare this constant to another constant for equality.
     * @param obj Other object
     * @return True if constants are equal
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(!(obj instanceof Reference)) {
            return false;
        }
        Reference other = (Reference) obj;

        if (this.positional != other.positional) {
            return false;
        }

        if (this.positional) {
            return other.getIndex() == getIndex();
        }

        // Compare based on name
        return this.expression.equals(other.expression);
    }

    public void setType(Class<?> type) {
        Assertion.assertTrue(this.positional);
        this.type = type;
    }

    /**
     * Define hash code to be that of the underlying object to make it stable.
     * @return Hash code, based on value
     */
    public int hashCode() {
        if (this.isPositional()) {
            return getIndex();
        }
        return this.expression.hashCode();
    }

    /**
     * Return a String representation of this object using SQLStringVisitor.
     * @return String representation using SQLStringVisitor
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    public boolean isCorrelated() {
        if (this.isPositional()) {
            return false;
        }
        //metadata hack
        if (this.expression.getGroupSymbol() == null || !(this.expression.getGroupSymbol().getMetadataID() instanceof TempMetadataID)) {
            return true;
        }

        TempMetadataID tid = (TempMetadataID)this.expression.getGroupSymbol().getMetadataID();
        return !tid.isScalarGroup();
    }

    public boolean isPositional() {
        return this.positional;
    }

    public void setExpression(ElementSymbol expression) {
        assert this.expression != null && !this.positional;
        this.expression = expression;
    }

    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }

}
