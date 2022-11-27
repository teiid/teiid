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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil;
import org.teiid.query.processor.relational.DependentValueSource;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Option.MakeDep;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.Expression;



/**
 * The DependentSetCriteria is missing the value set until it is filled during
 * processing.  This allows a criteria to contain a dynamic set of values provided
 * by a separate processing node.
 * @since 5.0.1
 */
public class DependentSetCriteria extends AbstractSetCriteria implements ContextReference {

    public static class AttributeComparison {
        public Expression dep;
        public Expression ind;
        public float ndv;
        public float maxNdv = NewCalculateCostUtil.UNKNOWN_VALUE;
    }

    /**
     * Specifies the expression whose values we want to return in the iterator
     */
    private Expression valueExpression;
    private String id;
    /**
     * The estimated number of distinct values for the value Expression
     */
    private float ndv = NewCalculateCostUtil.UNKNOWN_VALUE;
    private float maxNdv = NewCalculateCostUtil.UNKNOWN_VALUE;

    private float[] ndvs;
    private float[] maxNdvs;

    /**
     * set only for dependent pushdown
     */
    private DependentValueSource dependentValueSource;
    private MakeDep makeDepOptions;

    /**
     * Construct with the left expression
     */
    public DependentSetCriteria(Expression expr, String id) {
        setExpression(expr);
        this.id = id;
    }

    public void setAttributes(List<AttributeComparison> attributes) {
        this.ndvs = new float[attributes.size()];
        this.maxNdvs = new float[attributes.size()];
        for (int i = 0; i < attributes.size(); i++) {
            AttributeComparison comp = attributes.get(i);
            this.ndvs[i] = comp.ndv;
            this.maxNdvs[i] = comp.maxNdv;
        }
    }

    /**
     * There is a mismatch between the expression form and the more convenient attribute comparison,
     * so we reconstruct when needed
     */
    public List<AttributeComparison> getAttributes() {
        if (!hasMultipleAttributes()) {
            AttributeComparison comp = new AttributeComparison();
            comp.dep = getExpression();
            comp.ind = getValueExpression();
            comp.ndv = ndv;
            comp.maxNdv = maxNdv;
            return Arrays.asList(comp);
        }
        ArrayList<AttributeComparison> result = new ArrayList<AttributeComparison>();
        for (int i = 0; i < ndvs.length; i++) {
            AttributeComparison comp = new AttributeComparison();
            comp.dep = ((Array)getExpression()).getExpressions().get(i);
            comp.ind = ((Array)getValueExpression()).getExpressions().get(i);
            comp.ndv = ndv;
            comp.maxNdv = maxNdv;
            result.add(comp);
        }
        return result;
    }

    public boolean hasMultipleAttributes() {
        return this.ndvs != null && this.ndvs.length > 1;
    }

    public String getContextSymbol() {
        return id;
    }

    public float getMaxNdv() {
        return maxNdv;
    }

    public void setMaxNdv(float maxNdv) {
        this.maxNdv = maxNdv;
    }

    public float getNdv() {
        return ndv;
    }

    public void setNdv(float ndv) {
        this.ndv = ndv;
    }

    /**
     * Get the independent value expression
     * @return Returns the valueExpression.
     */
    public Expression getValueExpression() {
        return this.valueExpression;
    }


    /**
     * Set the independent value expression
     * @param valueExpression The valueExpression to set.
     */
    public void setValueExpression(Expression valueExpression) {
        this.valueExpression = valueExpression;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code.  WARNING: The hash code is based on data in the criteria.
     * If data values are changed, the hash code will change - don't hash this
     * object and change values.
     * @return Hash code
     */
    public int hashCode() {
        int hc = 0;
        hc = HashCodeUtil.hashCode(hc, getExpression());
        return hc;
    }

    /**
     * Override equals() method.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Use super.equals() to check obvious stuff and variable
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof DependentSetCriteria)) {
            return false;
        }

        DependentSetCriteria sc = (DependentSetCriteria)obj;
        if (isNegated() != sc.isNegated()) {
            return false;
        }

        return EquivalenceUtil.areEqual(getExpression(), sc.getExpression()) &&
                EquivalenceUtil.areEqual(getValueExpression(), sc.getValueExpression());
    }

    /**
     * Deep copy of object.  The values iterator source of this object
     * will not be cloned - it will be passed over as is and shared with
     * the original object, just like Reference.
     * @return Deep copy of object
     */
    public DependentSetCriteria clone() {
        Expression copy = null;
        if(getExpression() != null) {
            copy = (Expression) getExpression().clone();
        }

        DependentSetCriteria criteriaCopy = new DependentSetCriteria(copy, id);
        if (this.valueExpression != null) {
            criteriaCopy.setValueExpression((Expression) getValueExpression().clone());
        }
        criteriaCopy.id = this.id;
        criteriaCopy.ndv = this.ndv;
        criteriaCopy.maxNdv = this.maxNdv;
        criteriaCopy.maxNdvs = this.maxNdvs;
        criteriaCopy.ndvs = this.ndvs;
        criteriaCopy.makeDepOptions = this.makeDepOptions;
        return criteriaCopy;
    }

    @Override
    public void setNegated(boolean negationFlag) {
        if (!negationFlag) {
            throw new UnsupportedOperationException();
        }
    }

    public DependentValueSource getDependentValueSource() {
        return dependentValueSource;
    }

    public void setDependentValueSource(
            DependentValueSource dependentValueSource) {
        this.dependentValueSource = dependentValueSource;
    }

    public void setMakeDepOptions(MakeDep makeDep) {
        this.makeDepOptions = makeDep;
        //hint overrides computed value
        if (makeDep != null && makeDep.getMax() != null) {
            this.maxNdv = makeDep.getMax();
        }
    }

    public MakeDep getMakeDepOptions() {
        return makeDepOptions;
    }

}
