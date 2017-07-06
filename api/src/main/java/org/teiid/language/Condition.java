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

import org.teiid.translator.TypeFacility;

/**
 * Represents criteria, which is also a boolean expression
 */
public abstract class Condition extends BaseLanguageObject implements Expression {
	
    private boolean expression;
    
	@Override
	public Class<?> getType() {
		return TypeFacility.RUNTIME_TYPES.BOOLEAN;
	}
	
	/**
	 * 
	 * @return true if this is a boolean expression used as a value
	 */
	public boolean isExpression() {
	    return expression;
	}
	
	public void setExpression(boolean expression) {
        this.expression = expression;
    }

}
