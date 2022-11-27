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
import java.util.List;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.symbol.Reference;


/**
 * <p>This visitor class will traverse a language object tree and collect all
 * references it finds.
 *
 * <p>The easiest way to use this visitor is to call the static methods which create
 * the visitor (and possibly the collection), run the visitor, and return the collection.
 * The public visit() methods should NOT be called directly.
 */
public class ReferenceCollectorVisitor extends LanguageVisitor {

    private List<Reference> references = new ArrayList<Reference>();

    /**
     * Get the references collected by the visitor.  This should best be called
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link org.teiid.query.sql.symbol.ElementSymbol}
     */
    public List<Reference> getReferences() {
        return this.references;
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(Reference obj) {
        this.references.add(obj);
    }

    /**
     * Helper to quickly get the references from obj in a collection.
     * @param obj Language object
     * @return List of {@link org.teiid.query.sql.symbol.Reference}
     */
    public static List<Reference> getReferences(LanguageObject obj) {
        ReferenceCollectorVisitor visitor = new ReferenceCollectorVisitor();
        DeepPreOrderNavigator.doVisit(obj, visitor);

        return visitor.getReferences();
    }

}

