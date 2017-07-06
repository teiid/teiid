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
package org.teiid.translator.coherence.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.teiid.language.Comparison.Operator;
import org.teiid.language.Delete;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;


public class DeleteVisitor extends CoherenceVisitor  {
	private Collection deleteKeys = new ArrayList();
	
	public DeleteVisitor(RuntimeMetadata metadata) {
		super(metadata);
	}

	@Override
	public void visit(Delete delete) {
		super.visit(delete);
	}
	
	public Collection getKeys() {
		return this.deleteKeys;
	}
	
    public void addCompareCriteria(String columnname, Object value, Operator op, Class<?> type ) throws TranslatorException {
    	deleteKeys.add(value);
    }

    public void addInCriteria(String columnname, List<Object> parms, Class<?> type ) throws TranslatorException {
    	deleteKeys.addAll(parms);
    }

}
