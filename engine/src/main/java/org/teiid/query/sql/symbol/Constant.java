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

import java.math.BigDecimal;
import java.text.Collator;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionMethods;
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

    public static final Comparator<Object> COMPARATOR = getComparator(DataTypeManager.COLLATION_LOCALE, DataTypeManager.PAD_SPACE);

    static Comparator<Object> getComparator(String localeString, final boolean padSpace) {
        if (localeString == null) {
            return getComparator(padSpace);
        }
        String[] parts = localeString.split("_"); //$NON-NLS-1$
        Locale locale = null;
        if (parts.length == 1) {
            locale = new Locale(parts[0]);
        } else if (parts.length == 2) {
            locale = new Locale(parts[0], parts[1]);
        } else if (parts.length == 3) {
            locale = new Locale(parts[0], parts[1], parts[2]);
        } else {
            LogManager.logError(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30564, localeString));
            return getComparator(padSpace);
        }
        final Collator c = Collator.getInstance(locale);
        LogManager.logError(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30565, locale));
        return new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                Class<?> clazz = o1.getClass();
                if (clazz == String.class) {
                    String s1 = (String)o1;
                    String s2 = (String)o2;
                    if (padSpace) {
                        s1 = FunctionMethods.rightTrim(s1, ' ', false);
                        s2 = FunctionMethods.rightTrim(s2, ' ', false);
                    }
                    return c.compare(s1, s2);
                }
                return ((Comparable<Object>)o1).compareTo(o2);
            }
        };
    }

    static Comparator<Object> getComparator(boolean padSpace) {
        if (!padSpace) {
            return new Comparator<Object>() {
                @Override
                public int compare(Object o1, Object o2) {
                    return ((Comparable<Object>)o1).compareTo(o2);
                }
            };
        }
        return new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                Class<?> clazz = o1.getClass();
                if (clazz == String.class) {
                    CharSequence s1 = (CharSequence)o1;
                    CharSequence s2 = (CharSequence)o2;
                    return comparePadded(s1, s2);
                } else if (clazz == ClobType.class) {
                    CharSequence s1 = ((ClobType)o1).getCharSequence();
                    CharSequence s2 = ((ClobType)o2).getCharSequence();
                    return comparePadded(s1, s2);
                }
                return ((Comparable<Object>)o1).compareTo(o2);
            }
        };
    }
    /**
     * Construct a typed constant.  The specified value is not verified to be a value
     * of the specified type.  If this is not true, stuff probably won't work later on.
     *
     * @param value Constant value, may be null
     * @param type Type for the constant, should never be null
     */
    public Constant(Object value, Class<?> type) {
        this.value = value;

        // Check that type is valid, then set it
        if(type == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0014")); //$NON-NLS-1$
        }
        this.type = type;
    }

    /**
     * Construct a constant with a value, which may be null.  The data type
     * is determined automatically from the type of the value.
     * @param value Constant value, may be null
     */
    public Constant(Object value) {
        this.value = value;
        if (this.value == null) {
            this.type = DataTypeManager.DefaultDataClasses.NULL;
        } else {
            this.type = DataTypeManager.getRuntimeType(value.getClass());
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
     * TODO: remove me when a null type is supported
     * @param type
     */
    public void setType(Class<?> type) {
        this.type = type;
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
        if(this.value != null && !isMultiValued()) {
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
        return COMPARATOR.compare(this.value, o.getValue());
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
                return c1 - c2;
            }
        }
        result = len1 - len2;
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
