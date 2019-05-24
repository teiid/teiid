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

package org.teiid.translator.jdbc.postgresql;

import org.teiid.translator.Translator;

/**
 * Translator for Greenplum.
 */
@Translator(name="greenplum", description="A translator for the Greenplum Database")
public class GreenplumExecutionFactory extends PostgreSQLExecutionFactory {

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsElementaryOlapOperations() {
        //greenplum is/was based upon postgresql 8.2, but added extensions for window functions
        //this can be verified back to greenplum 4.1.  Since that and earlier releases are eol
        //it suffices to just return true
        return true;
    }

}
