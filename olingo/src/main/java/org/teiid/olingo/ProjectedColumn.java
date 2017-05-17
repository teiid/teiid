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
package org.teiid.olingo;

import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.teiid.query.sql.symbol.Expression;

public class ProjectedColumn {
    private Expression expr;
    private EdmType edmType;
    private boolean collection;
    private int ordinal;
    private EdmProperty property;
    
    public ProjectedColumn(Expression expr, EdmType edmType, EdmProperty property, boolean collection) {
        this.expr = expr; 
        this.edmType = edmType;
        this.collection = collection;
        this.property = property;
    }
    
    public Expression getExpression() {
        return this.expr;
    }
    
    public EdmType getEdmType() {
        return this.edmType;
    }
    
    public boolean isCollection() {
        return collection;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }
    
    public EdmProperty getProperty() {
		return property;
	}
    
    public Integer getPrecision() {
    	if (property == null) {
    		return null;
    	}
    	return property.getPrecision();
    }
    
    public Integer getScale() {
    	if (property == null) {
    		return null;
    	}
    	return property.getScale();
    }
    
}