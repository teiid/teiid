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

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.WindowFunction;


public class AggregateSymbolCollectorVisitor extends LanguageVisitor {

    public static class AggregateStopNavigator extends PreOrPostOrderNavigator {

        private Collection<? extends Expression> groupingCols;
        private Collection<? super Expression> groupingColsUsed;

        public AggregateStopNavigator(LanguageVisitor visitor, Collection<? super Expression> groupingColsUsed, Collection<? extends Expression> groupingCols) {
            super(visitor, PreOrPostOrderNavigator.PRE_ORDER, false);
            this.groupingCols = groupingCols;
            this.groupingColsUsed = groupingColsUsed;
        }

        public AggregateStopNavigator(LanguageVisitor visitor, Collection<? extends Expression> groupingCols) {
            super(visitor, PreOrPostOrderNavigator.PRE_ORDER, true);
            this.groupingCols = groupingCols;
        }

        public void visit(AggregateSymbol obj) {
            if (!obj.isWindowed()) {
                // Visit aggregate symbol but do not dive into it's expression
                preVisitVisitor(obj);
                postVisitVisitor(obj);
            } else {
                super.visit(obj);
            }
        }

        @Override
        protected void visitNode(LanguageObject obj) {
            if (groupingCols != null && obj instanceof Expression && groupingCols.contains(obj)) {
                if (groupingColsUsed != null) {
                    groupingColsUsed.add((Expression)obj);
                }
                return;
            }
            super.visitNode(obj);
        }

    }

    private Collection<? super AggregateSymbol> aggregates;
    private Collection<? super ElementSymbol> otherElements;
    private Collection<? super WindowFunction> windowFunctions;

    public AggregateSymbolCollectorVisitor(Collection<? super AggregateSymbol> aggregates, Collection<? super ElementSymbol> elements) {
        this.aggregates = aggregates;
        this.otherElements = elements;
    }

    public void visit(AggregateSymbol obj) {
        if (aggregates != null && !obj.isWindowed()) {
            this.aggregates.add(obj);
        }
    }

    public void visit(WindowFunction windowFunction) {
        if (this.windowFunctions != null) {
            this.windowFunctions.add(windowFunction);
        }
    }

    public void visit(ElementSymbol obj) {
        if (this.otherElements != null && !obj.isExternalReference()) {
            this.otherElements.add(obj);
        }
    }

    public static final void getAggregates(LanguageObject obj,
            Collection<? super AggregateSymbol> aggregates,
            Collection<? super ElementSymbol> otherElements,
            Collection<? super Expression> groupingColsUsed,
            Collection<? super WindowFunction> windowFunctions,
            Collection<? extends Expression> groupingCols) {
        AggregateSymbolCollectorVisitor visitor = new AggregateSymbolCollectorVisitor(aggregates, otherElements);
        visitor.windowFunctions = windowFunctions;
        AggregateStopNavigator asn = new AggregateStopNavigator(visitor, groupingColsUsed, groupingCols);
        asn.visitNode(obj);
    }

    public static final Collection<AggregateSymbol> getAggregates(LanguageObject obj, boolean removeDuplicates) {
        if (obj == null) {
            return Collections.emptyList();
        }
        Collection<AggregateSymbol> aggregates = null;
        if (removeDuplicates) {
            aggregates = new LinkedHashSet<AggregateSymbol>();
        } else {
            aggregates = new ArrayList<AggregateSymbol>();
        }
        AggregateSymbolCollectorVisitor visitor = new AggregateSymbolCollectorVisitor(aggregates, null);
        AggregateStopNavigator asn = new AggregateStopNavigator(visitor, null, null);
        obj.acceptVisitor(asn);
        return aggregates;
    }

}
