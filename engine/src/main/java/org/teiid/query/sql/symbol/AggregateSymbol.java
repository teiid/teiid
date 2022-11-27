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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.query.parser.SQLParserUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.OrderBy;


/**
 * <p>An aggregate symbol represents an aggregate function. The * expression
 * is encoded by setting the expression to null.  This may ONLY be used with the COUNT function.
 *
 * <p>The type of an aggregate symbol depends on the function and the type of the underlying
 * expression.  The type of a COUNT function is ALWAYS integer.  MIN and MAX functions take the
 * type of their contained expression.
 */
public class AggregateSymbol extends Function implements DerivedExpression, NamedExpression {

    private static final Expression[] EMPTY_ARGS = new Expression[0];

    public enum Type {
        COUNT_BIG,
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX,
        XMLAGG,
        TEXTAGG,
        ARRAY_AGG,
        JSONARRAY_AGG,
        ANY,
        SOME,
        EVERY,
        STDDEV_POP,
        STDDEV_SAMP,
        VAR_POP,
        VAR_SAMP,
        RANK(true),
        DENSE_RANK(true),
        PERCENT_RANK(true),
        CUME_DIST(true),
        ROW_NUMBER(true),
        FIRST_VALUE(true),
        LAST_VALUE(true),
        LEAD(true),
        LAG(true),
        STRING_AGG,
        NTILE(true),
        NTH_VALUE(true),
        USER_DEFINED;

        boolean analytical;
        Type(){}
        Type(boolean analytical) {
            this.analytical = analytical;
        }
        public boolean isAnalytical() {
            return analytical;
        }
    }

    private static final Map<String, Type> nameMap = new TreeMap<String, Type>(String.CASE_INSENSITIVE_ORDER);

    static {
        for (Type t : Type.values()) {
            if (t == Type.USER_DEFINED) {
                continue;
            }
            nameMap.put(t.name(), t);
        }
    }

    private Type aggregate;
    private boolean distinct;
    private OrderBy orderBy;
    private Expression condition;
    private boolean isWindowed;

    private static final Class<Integer> COUNT_TYPE = DataTypeManager.DefaultDataClasses.INTEGER;
    private static final Map<Class<?>, Class<?>> SUM_TYPES;
    private static final Map<Class<?>, Class<?>> AVG_TYPES;

    public static final boolean LONG_RANKS = PropertiesUtils.getHierarchicalProperty("org.teiid.longRanks", true, Boolean.class); //$NON-NLS-1$

    static {
        SUM_TYPES = new HashMap<Class<?>, Class<?>>();
        SUM_TYPES.put(DataTypeManager.DefaultDataClasses.BYTE, DataTypeManager.DefaultDataClasses.LONG);
        SUM_TYPES.put(DataTypeManager.DefaultDataClasses.SHORT, DataTypeManager.DefaultDataClasses.LONG);
        SUM_TYPES.put(DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.LONG);
        SUM_TYPES.put(DataTypeManager.DefaultDataClasses.LONG, DataTypeManager.DefaultDataClasses.LONG);
        SUM_TYPES.put(DataTypeManager.DefaultDataClasses.BIG_INTEGER, DataTypeManager.DefaultDataClasses.BIG_INTEGER);
        SUM_TYPES.put(DataTypeManager.DefaultDataClasses.FLOAT, DataTypeManager.DefaultDataClasses.DOUBLE);
        SUM_TYPES.put(DataTypeManager.DefaultDataClasses.DOUBLE, DataTypeManager.DefaultDataClasses.DOUBLE);
        SUM_TYPES.put(DataTypeManager.DefaultDataClasses.BIG_DECIMAL, DataTypeManager.DefaultDataClasses.BIG_DECIMAL);

        AVG_TYPES = new HashMap<Class<?>, Class<?>>();
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.BYTE, SQLParserUtil.DECIMAL_AS_DOUBLE?DataTypeManager.DefaultDataClasses.DOUBLE:DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.SHORT, SQLParserUtil.DECIMAL_AS_DOUBLE?DataTypeManager.DefaultDataClasses.DOUBLE:DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.INTEGER, SQLParserUtil.DECIMAL_AS_DOUBLE?DataTypeManager.DefaultDataClasses.DOUBLE:DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.LONG, SQLParserUtil.DECIMAL_AS_DOUBLE?DataTypeManager.DefaultDataClasses.DOUBLE:DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.BIG_INTEGER, DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.FLOAT, DataTypeManager.DefaultDataClasses.DOUBLE);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.DOUBLE, DataTypeManager.DefaultDataClasses.DOUBLE);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.BIG_DECIMAL, DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
    }

    /**
     * Constructor used for cloning
     * @param name
     * @param aggregateFunction
     * @since 4.3
     */
    protected AggregateSymbol(String name, Type aggregateFunction, boolean isDistinct, Expression[] args) {
        super(name, args);
        this.aggregate = aggregateFunction;
        this.distinct = isDistinct;
    }

    /**
     * Construct an aggregate symbol with all given data.
     * @param aggregateFunction Aggregate function type ({@link org.teiid.language.SQLConstants.NonReserved#COUNT}, etc)
     * @param isDistinct True if DISTINCT flag is set
     * @param expression Contained expression
     */
    public AggregateSymbol(String aggregateFunction, boolean isDistinct, Expression expression) {
        this(aggregateFunction, isDistinct, expression == null?EMPTY_ARGS:new Expression[] {expression}, null);
    }

    public AggregateSymbol(String aggregateFunction, boolean isDistinct, Expression[] args, OrderBy orderBy) {
        super(aggregateFunction, args);
        this.aggregate = nameMap.get(aggregateFunction);
        if (this.aggregate == null) {
            this.aggregate = Type.USER_DEFINED;
        }
        this.distinct = isDistinct;
        this.orderBy = orderBy;
    }

    /**
     * Set the aggregate function.  If the aggregate function is an invalid value, an
     * IllegalArgumentException is thrown.
     * @param aggregateFunction Aggregate function type
     * @see org.teiid.language.SQLConstants.NonReserved#COUNT
     * @see org.teiid.language.SQLConstants.NonReserved#SUM
     * @see org.teiid.language.SQLConstants.NonReserved#AVG
     * @see org.teiid.language.SQLConstants.NonReserved#MIN
     * @see org.teiid.language.SQLConstants.NonReserved#MAX
     */
    public void setAggregateFunction(Type aggregateFunction) {
        this.aggregate = aggregateFunction;
    }

    /**
     * Get the aggregate function type - this will map to one of the reserved words
     * for the aggregate functions.
     * @return Aggregate function type
     */
    public Type getAggregateFunction() {
        return this.aggregate;
    }

    public boolean isRowValueFunction() {
        switch (aggregate) {
        case NTILE:
        case ROW_NUMBER:
        case LEAD:
        case LAG:
            return true;
        default:
            return false;
        }
    }

    /**
     * Get the distinct flag.  If true, aggregate symbol will remove duplicates during
     * computation.
     * @return True if duplicates should be removed during computation
     */
    public boolean isDistinct() {
        return this.distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * Get the type of the symbol, which depends on the aggregate function and the
     * type of the contained expression
     * @return Type of the symbol
     */
    public Class<?> getType() {
        switch (this.aggregate) {
        case COUNT_BIG:
            return DataTypeManager.DefaultDataClasses.LONG;
        case COUNT:
            return COUNT_TYPE;
        case SUM:
            Class<?> expressionType = this.getArg(0).getType();
            return SUM_TYPES.get(expressionType);
        case AVG:
            expressionType = this.getArg(0).getType();
            return AVG_TYPES.get(expressionType);
        case ARRAY_AGG:
            if (this.getArg(0) == null) {
                return null;
            }
            return DataTypeManager.getArrayType(this.getArg(0).getType());
        case TEXTAGG:
            return DataTypeManager.DefaultDataClasses.BLOB;
        case JSONARRAY_AGG:
            return DataTypeManager.DefaultDataClasses.JSON;
        case USER_DEFINED:
        case STRING_AGG:
            return super.getType();
        case PERCENT_RANK:
        case CUME_DIST:
            return DataTypeManager.DefaultDataClasses.DOUBLE;
        }
        if (isBoolean()) {
            return DataTypeManager.DefaultDataClasses.BOOLEAN;
        }
        if (isEnhancedNumeric()) {
            return DataTypeManager.DefaultDataClasses.DOUBLE;
        }
        if (isRanking()) {
            return super.getType();
        }
        if (this.getArgs().length == 0) {
            return null;
        }
        return this.getArg(0).getType();
    }

    public boolean isRanking() {
        switch (this.aggregate) {
        case RANK:
        case ROW_NUMBER:
        case DENSE_RANK:
            return true;
        default:
            return false;
        }
    }

    public boolean isCount() {
        switch (this.aggregate) {
        case COUNT:
        case COUNT_BIG:
            return true;
        default:
            return false;
        }
    }

    public boolean isAnalytical() {
        return this.aggregate.analytical;
    }

    public boolean isBoolean() {
        return this.aggregate == Type.EVERY
                || this.aggregate == Type.SOME
                || this.aggregate == Type.ANY;
    }

    public boolean isEnhancedNumeric() {
        return this.aggregate == Type.STDDEV_POP
        || this.aggregate == Type.STDDEV_SAMP
        || this.aggregate == Type.VAR_SAMP
        || this.aggregate == Type.VAR_POP;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    /**
     * Return a deep copy of this object
     */
    public Object clone() {
        AggregateSymbol copy = new AggregateSymbol(getName(), getAggregateFunction(), isDistinct(), LanguageObject.Util.deepClone(getArgs()));
        if (orderBy != null) {
            copy.setOrderBy(orderBy.clone());
        }
        if (condition != null) {
            copy.setCondition((Expression) condition.clone());
        }
        copy.isWindowed = this.isWindowed;
        copy.type = this.type;
        copy.setFunctionDescriptor(getFunctionDescriptor());
        return copy;
    }

    /**
     * @see org.teiid.query.sql.symbol.ExpressionSymbol#hashCode()
     */
    public int hashCode() {
        int hasCode = HashCodeUtil.hashCode(aggregate.hashCode(), distinct);
        return HashCodeUtil.hashCode(hasCode, super.hashCode());
    }

    /**
     * @see org.teiid.query.sql.symbol.ExpressionSymbol#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof AggregateSymbol)) {
            return false;
        }

        AggregateSymbol other = (AggregateSymbol)obj;

        return super.equals(obj)
               && this.aggregate.equals(other.aggregate)
               && this.distinct == other.distinct
               && this.isWindowed == other.isWindowed
               && EquivalenceUtil.areEqual(this.condition, other.condition)
               && EquivalenceUtil.areEqual(this.getOrderBy(), other.getOrderBy());
    }

    public boolean isCardinalityDependent() {
        if (isDistinct()) {
            return false;
        }
        switch (getAggregateFunction()) {
        case MAX:
        case MIN:
        case ANY:
        case SOME:
        case EVERY:
            return false;
        case USER_DEFINED:
            return !getFunctionDescriptor().getMethod().getAggregateAttributes().usesDistinctRows();
        }
        return true;
    }

    public Expression getCondition() {
        return condition;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public static boolean areAggregatesCardinalityDependent(Collection<AggregateSymbol> aggs) {
        for (AggregateSymbol aggregateSymbol : aggs) {
            if (aggregateSymbol.isCardinalityDependent()) {
                return true;
            }
        }
        return false;
    }

    public boolean respectsNulls() {
        switch (this.aggregate) {
        case TEXTAGG:
        case ARRAY_AGG:
        case JSONARRAY_AGG:
            return true;
        }
        return false;
    }

    public boolean canStage() {
        if (orderBy != null) {
            return false;
        }
        switch (this.aggregate) {
        case TEXTAGG:
        case ARRAY_AGG:
        case JSONARRAY_AGG:
        case STRING_AGG:
            return false;
        case USER_DEFINED:
            return this.getArgs().length == 1 && this.getFunctionDescriptor().getMethod().getAggregateAttributes().isDecomposable();
        }
        return true;
    }

    public boolean isWindowed() {
        return isWindowed;
    }

    public void setWindowed(boolean isWindowed) {
        this.isWindowed = isWindowed;
    }

}
