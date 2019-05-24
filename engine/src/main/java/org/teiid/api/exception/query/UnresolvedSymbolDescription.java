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

package org.teiid.api.exception.query;

import java.io.Serializable;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.symbol.ElementSymbol;

/**
 * This helper object describes an unresolved symbol found during
 * query resolution.
 */
public class UnresolvedSymbolDescription implements Serializable {

    private String symbol;
    private String description;
    private transient LanguageObject object;

    /**
     * Construct a description given the symbol and it's description.
     * @param symbol Unresolved symbol
     * @param description Description of error
     */
    public UnresolvedSymbolDescription(String symbol, String description) {
        this.symbol = symbol;
        this.description = description;
    }

    public UnresolvedSymbolDescription(ElementSymbol symbol,
            String description) {
        this.symbol = symbol.toString();
        this.object = symbol;
        this.description = description;
    }

    public LanguageObject getObject() {
        return object;
    }

    /**
     * Get the symbol that was unresolved
     * @return Unresolved symbol
     */
    public String getSymbol() {
        return this.symbol;
    }

    /**
     * Get the description of the problem
     * @return Problem description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Get string representation of the unresolved symbol description
     * @return String representation
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        if(symbol != null) {
            str.append("Unable to resolve '"); //$NON-NLS-1$
            str.append(symbol);
            str.append("': "); //$NON-NLS-1$
        }
        if(description != null) {
            str.append(description);
        } else {
            str.append("Unknown reason"); //$NON-NLS-1$
        }
        return str.toString();
    }

}
