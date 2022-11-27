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

/*
 */
package org.teiid.query.sql.lang;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Rpresent INTO clause in SELECT ... INTO ... clause, which is used to create
 * temporary table.
 */
public class Into implements LanguageObject {
    private GroupSymbol group;

    /**
     * Construct default object
     */
    public Into() {
    }

    /**
     * Construct object with specified group
     * @param group Group being held
     */
    public Into(GroupSymbol group) {
        this.group = group;
    }

    /**
     * Set the group held by the clause
     * @param group Group to hold
     */
    public void setGroup(GroupSymbol group) {
        this.group = group;
    }

    /**
     * Get group held by clause
     * @return Group held by clause
     */
    public GroupSymbol getGroup() {
        return this.group;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Check whether objects are equal
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(! (obj instanceof Into)) {
            return false;
        }

        return EquivalenceUtil.areEqual(getGroup(), ((Into)obj).getGroup());
    }

    /**
     * Get hash code of object
     * @return Hash code
     */
    public int hashCode() {
        if(this.group == null) {
            return 0;
        }
        return this.group.hashCode();
    }

    /**
     * Get deep clone of object
     * @return Deep copy of the object
     */
    public Object clone() {
        GroupSymbol copyGroup = null;
        if(this.group != null) {
            copyGroup = (GroupSymbol) this.group.clone();
        }
        return new Into(copyGroup);
    }


    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
