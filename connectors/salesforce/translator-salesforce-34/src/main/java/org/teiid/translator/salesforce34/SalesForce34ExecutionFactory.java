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

package org.teiid.translator.salesforce34;

import org.teiid.translator.Translator;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@Translator(name="salesforce-34", description="A translator for Salesforce With API version 34")
public class SalesForce34ExecutionFactory extends SalesForceExecutionFactory {
    
    /*
     * Handles an api difference with {@link QueryResult}.setRecords
     */
    @Override
    public QueryResult buildQueryResult(SObject[] results) {
        QueryResult result = new QueryResult();
        result.setRecords(results);         
        result.setSize(results.length);
        result.setDone(true);
        return result;
    }
}
