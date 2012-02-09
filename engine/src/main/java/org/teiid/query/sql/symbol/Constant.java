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

package org.teiid.query.sql.symbol;

import java.math.BigDecimal;
import java.util.List;

import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This class represents a literal value in a SQL string.  The Constant object has a value
 * and a type for that value.  In many cases, the type can be derived from the type of the
 * value, but that is not true if the value is null.  In that case, the type is unknown
 * and is set to the null type until the type is resolved at a later point.
 */
public class Constant implements Expression, Comparable<Constant> {

	public static final Constant NULL_CONSTANT = new Constant(null);
	
	private Object value;
	private Class<?> type;
	private boolean multiValued;
	private boolean bindEligible;

	/**
	 * Construct a typed constant.  The specified value is not verified to be a value
	 * of the specified type.  If this is not true, stuff probably won't work later on.
	 *
     * @param value Constant value, may be null
	 * @param type Type for the constant, should never be null
	 */
	public Constant(Object value, Class<?> type) {
        // Set value
        this.value = DataTypeManager.convertToRuntimeType(value);

        // Check that type is valid, then set it
        if(type == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0014")); //$NON-NLS-1$
        }
        if(! DataTypeManager.getAllDataTypeClasses().contains(type)) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0015", type.getName())); //$NON-NLS-1$
        }
        assert value == null || type.isAssignableFrom(value.getClass()) : "Invalid value for specified type."; //$NON-NLS-1$
        this.type = type;
	}

	/**
	 * Construct a constant with a value, which may be null.  The data type
     * is determined automatically from the type of the value.
	 * @param value Constant value, may be null
	 */
	public Constant(Object value) {
		this.value = DataTypeManager.convertToRuntimeType(value);
		if (this.value == null) {
			this.type = DataTypeManager.DefaultDataClasses.NULL;
		} else if (DataTypeManager.getAllDataTypeClasses().contains(this.value.getClass())) {
			this.type = this.value.getClass();
		} else {
			this.type = DataTypeManager.DefaultDataClasses.OBJECT; 
		}
	}

	/**
	 * Get type of constant, if known
	 * @return Java class name of type
	 */
	public Class<?> getType() {
		return this.type;
	}

	/**
	 * Get value of constant
	 * @return Constant value
	 */
	public Object getValue() {
		return this.value;
	}

	/**
	 * Return true if the constant is null.
	 * @return True if value is null
	 */
	public boolean isNull() {
		return value==null;
	}

	/**
	 * Return true if expression has been fully resolved.  Typically the QueryResolver component
	 * will handle resolution of an expression.
	 * @return True if resolved
	 */
	public boolean isResolved() {
		return true;
	}
	
    public void setMultiValued(List<?> value) {
		this.multiValued = true;
		this.value = value;
	}
    
    public boolean isMultiValued() {
		return multiValued;
	}

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
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

		if(!(obj instanceof Constant)) {
			return false;
		}
		Constant other = (Constant) obj;

		// Check null values first
		if(other.isNull()) {
			if (this.isNull()) {
				return true;
			}
			return false;
		}
		
		if (this.isNull()) {
			return false;
		}

		// Check type - types are never null
		if(! other.getType().equals(this.getType())) {
			return false;
		}
		
		if (this.value instanceof BigDecimal) {
			if (this.value == other.value) {
				return true;
			}
			if (!(other.value instanceof BigDecimal)) {
				return false;
			}
			return ((BigDecimal)this.value).compareTo((BigDecimal)other.value) == 0;
		}

        return multiValued == other.multiValued && other.getValue().equals(this.getValue());
	}

	/**
	 * Define hash code to be that of the underlying object to make it stable.
	 * @return Hash code, based on value
	 */
	public int hashCode() {
		if(this.value != null) {
			if (this.value instanceof BigDecimal) {
				BigDecimal bd = (BigDecimal)this.value;
				int xsign = bd.signum();
		        if (xsign == 0)
		            return 0;
		        bd = bd.stripTrailingZeros();
		        return bd.hashCode();
			}
			return this.value.hashCode();
		}
		return 0;
	}

	/**
	 * Return a shallow copy of this object - value is NOT cloned!
	 * @return Shallow copy of object
	 */
	public Object clone() {
        Constant copy =  new Constant(getValue(), getType());
        copy.multiValued = multiValued;
        copy.bindEligible = bindEligible;
        return copy;
	}

	/**
	 * Return a String representation of this object using SQLStringVisitor.
	 * @return String representation using SQLStringVisitor
	 */
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}

	@Override
	public int compareTo(Constant o) {
		if (isNull()) {
			if (o.isNull()) {
				return 0;
			}
			return -1;
		}
		if (o.isNull()) {
			return 1;
		}
		return compare((Comparable<?>)this.value, (Comparable<?>)o.getValue());
	}
	
	/**
	 * Compare the given non-null values
	 * @param o1
	 * @param o2
	 * @return
	 */
	public final static int compare(Comparable o1, Comparable o2) {
		if (DataTypeManager.PAD_SPACE) {
			if (o1 instanceof String) {
				CharSequence s1 = (CharSequence)o1;
				CharSequence s2 = (CharSequence)o2;
				return comparePadded(s1, s2);
			} else if (o1 instanceof ClobType) {
				CharSequence s1 = ((ClobType)o1).getCharSequence();
				CharSequence s2 = ((ClobType)o2).getCharSequence();
				return comparePadded(s1, s2);
			}
		}
		return o1.compareTo(o2);
	}

	final static int comparePadded(CharSequence s1, CharSequence s2) {
		int len1 = s1.length();
		int len2 = s2.length();
		int n = Math.min(len1, len2);
		int i = 0;
		int result = 0;
		for (; i < n; i++) {
			char c1 = s1.charAt(i);
			char c2 = s2.charAt(i);
			if (c1 != c2) {
				result = c1 - c2;
				break;
			}
		}
		if (result == 0 && len1 != len2) {
			result = len1 - len2;
		}
		for (int j = i; j < len1; j++) {
			if (s1.charAt(j) != ' ') {
				return result;
			}
		}
		for (int j = i; j < len2; j++) {
			if (s2.charAt(j) != ' ') {
				return result;
			}
		}
		return 0;
	}
	
	public boolean isBindEligible() {
		return bindEligible;
	}
	
	public void setBindEligible(boolean bindEligible) {
		this.bindEligible = bindEligible;
	}
	
}
