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

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

/**
 * This class represents a join type.
 * Outer joins will put nulls on the outer side for non-matches.  "Cross" joins
 * have no criteria and are a cross product of all rows in each table.  To use
 * a JoinType, you should not (and cannot) construct the object - rather you
 * should use the provided static constants.
 */
public class JoinType implements LanguageObject {
    // Constants defining join type - users will construct these

    /** Represents an inner join:  a INNER JOIN b */
    public static final JoinType JOIN_INNER         = new JoinType(0, false);

    /** Represents a right outer join:  a RIGHT OUTER JOIN b */
    public static final JoinType JOIN_RIGHT_OUTER     = new JoinType(1, true);

    /** Represents a left outer join:  a LEFT OUTER JOIN b */
    public static final JoinType JOIN_LEFT_OUTER     = new JoinType(2, true);

    /** Represents a full outer join:  a FULL OUTER JOIN b */
    public static final JoinType JOIN_FULL_OUTER     = new JoinType(3, true);

    /** Represents a cross join:  a CROSS JOIN b */
    public static final JoinType JOIN_CROSS         = new JoinType(4, false);

    /** Represents a union join:  a UNION JOIN b - not used after rewrite */
    public static final JoinType JOIN_UNION         = new JoinType(5, true);

    /** internal SEMI Join type */
    public static final JoinType JOIN_SEMI          = new JoinType(6, false);

    /** internal ANTI SEMI Join type */
    public static final JoinType JOIN_ANTI_SEMI          = new JoinType(7, true);

    private int type;
    private boolean outer;

    /**
     * Construct a join type object.  This is private and is only called by
     * the static constant objects in this class.
     * @param type Type code for object
     */
    private JoinType(int type, boolean outer) {
        this.type = type;
        this.outer = outer;
    }

    /**
     * Used only for comparison during equals, not by users of this class
     * @return Type code for object
     */
    int getTypeCode() {
        return this.type;
    }

    /**
      * To switch directions from left to right or right to left.  Joins
      * that are not LEFT OUTER or RIGHT OUTER are returned unchanged.
      * @return New JoinType constant for the reverse join type
     */
    public JoinType getReverseType() {
        if(this.equals(JOIN_RIGHT_OUTER)) {
            return JOIN_LEFT_OUTER;
        } else if(this.equals(JOIN_LEFT_OUTER)) {
            return JOIN_RIGHT_OUTER;
        }
        return this;
    }

    /**
     * Check if this join type is an outer join.
     * @return True if left/right/full outer, false if inner/cross
     */
    public boolean isOuter() {
        return outer;
    }

    /**
     * Override Object.equals() to compare objects
     * @param other Other object
     * @return True if equal
     */
    public boolean equals(Object other) {
        if(this == other) {
            return true;
        }
        if(! (other instanceof JoinType)) {
            return false;
        }

        return ((JoinType)other).getTypeCode() == this.type;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code for this type
     * @return Hash code
     */
    public int hashCode() {
        return this.type;
    }

    /**
      * Class is immutable, so clone can just return the same class
      * @return Same object - these objects are immutable
     */
    public Object clone() {
        return this;
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
