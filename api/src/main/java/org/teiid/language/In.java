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

package org.teiid.language;

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

public class In extends BaseInCondition {

    private List<Expression> rightExpressions;

    public In(Expression left, List<Expression> right, boolean negated) {
        super(left, negated);
        rightExpressions = right;
    }

    /**
     * Get List of IExpression in the set on the right side of the criteria.
     * @return List of IExpression
     */
    public List<Expression> getRightExpressions() {
        return rightExpressions;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setRightExpressions(List<Expression> expressions) {
        this.rightExpressions = expressions;
    }

}
