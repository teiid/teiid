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

package org.teiid.query.sql.proc;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.visitor.SQLStringVisitor;

/**
 * <p> This class represents the a statement in the stored procedure language.
 * The subclasses of this class represent specific statements like an
 * <code>IfStatement</code>, <code>AssignmentStatement</code> etc.
 */
public abstract class Statement implements LanguageObject {

    public interface Labeled {
        String getLabel();
        void setLabel(String label);
    }

    /**
     * Represents an unknown type of statement
     */
    public static final int TYPE_UNKNOWN = 0;

    /**
     * Represents a IF statement
     */
    public static final int TYPE_IF = 1;

    /**
     * Represents a SQL COMMAND statement
     */
    public static final int TYPE_COMMAND = 2;

    /**
     * Represents a DECLARE statement
     */
    public static final int TYPE_DECLARE = 3;

    /**
     * Represents a ERROR statement
     */
    public static final int TYPE_ERROR = 4;

    /**
     * Represents a ASSIGNMENT statement
     */
    public static final int TYPE_ASSIGNMENT = 5;

    /**
     * Represents a LOOP statement
     */
    public static final int TYPE_LOOP = 6;

    /**
     * Represents a WHILE statement
     */
    public static final int TYPE_WHILE = 7;

    /**
     * Represents a CONTINUE statement
     */
    public static final int TYPE_CONTINUE = 8;

    /**
     * Represents a BREAK statement
     */
    public static final int TYPE_BREAK = 9;

    public static final int TYPE_UPDATE = 10;

    public static final int TYPE_COMPOUND = 11;

    public static final int TYPE_LEAVE = 12;

    public static final int TYPE_RETURN = 13;

    /**
     * Return type of statement to make it easier to build switch statements by statement type.
     * @return Type from TYPE constants
     */
    public abstract int getType();

    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================

    /**
     * Deep clone statement to produce a new identical statement.
     * @return Deep clone
     */
    public abstract Object clone();

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
}
