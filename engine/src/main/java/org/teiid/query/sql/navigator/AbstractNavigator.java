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

package org.teiid.query.sql.navigator;

import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;



/**
 * @since 4.2
 */
public class AbstractNavigator extends LanguageVisitor {

    private LanguageVisitor visitor;

    public AbstractNavigator(LanguageVisitor visitor) {
        this.visitor = visitor;
    }

    public LanguageVisitor getVisitor() {
        return this.visitor;
    }

    protected void visitVisitor(LanguageObject obj) {
        if(this.visitor.shouldAbort()) {
            return;
        }

        obj.acceptVisitor(this.visitor);
    }

    protected void visitNode(LanguageObject obj) {
        if(this.visitor.shouldAbort()) {
            return;
        }

        if(obj != null) {
            obj.acceptVisitor(this);
        }
    }

    protected void visitNodes(Collection<? extends LanguageObject> nodes) {
        if(this.visitor.shouldAbort() || nodes == null) {
            return;
        }
        int size = nodes.size();
        if (size > 0) {
            if (nodes instanceof List<?> && nodes instanceof RandomAccess) {
                List<? extends LanguageObject> list = (List<? extends LanguageObject>) nodes;
                for (int i = 0; i < size; i++) {
                    visitNode(list.get(i));
                }
                return;
            }
            for (LanguageObject languageObject : nodes) {
                visitNode(languageObject);
            }
        }
    }


}
