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

package org.teiid.query.sql.proc;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.visitor.SQLStringVisitor;

/**
 * <p> This class represents the a statement in the stored procedure language.
 * The subclasses of this class represent specific statements like an
 * <code>IfStatement</code>, <code>AssignmentStatement</code> etc.</p>
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
