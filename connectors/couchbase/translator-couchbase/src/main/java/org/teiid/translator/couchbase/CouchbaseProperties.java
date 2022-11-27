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
package org.teiid.translator.couchbase;

public interface CouchbaseProperties {

    String WAVE = "`"; //$NON-NLS-1$
    String SOURCE_SEPARATOR = "."; //$NON-NLS-1$
    String PLACEHOLDER = "$"; //$NON-NLS-1$
    String JSON = "json"; //$NON-NLS-1$
    String PK = "PK"; //$NON-NLS-1$
    String UNDERSCORE = "_"; //$NON-NLS-1$
    String IDX_SUFFIX = "_idx"; //$NON-NLS-1$
    String DIM_SUFFIX = "dim"; //$NON-NLS-1$
    String SQUARE_BRACKETS = "[]"; //$NON-NLS-1$

    String TRUE_VALUE = "true"; //$NON-NLS-1$
    String FALSE_VALUE = "false"; //$NON-NLS-1$

    String UNNEST = "UNNEST"; //$NON-NLS-1$
    String UNNEST_POSITION = "UNNEST_POSITION"; //$NON-NLS-1$
    String LET = "LET"; //$NON-NLS-1$

    String DEFAULT_NAMESPACE = "default"; //$NON-NLS-1$
    String DEFAULT_TYPENAME = "type"; //$NON-NLS-1$
    String TPYENAME_MATCHER_PATTERN = "([a-zA-Z_]\\w*|(?:`[^`]*`)+):([a-zA-Z_]\\w*|(?:`[^`]*`)+)(?:$|,)"; //$NON-NLS-1$

    String NAME = "name"; //$NON-NLS-1$
    String DOCUMENTID = "documentID"; //$NON-NLS-1$

    // Procedure Name
    String GETDOCUMENTS = "getDocuments"; //$NON-NLS-1$
    String GETDOCUMENT = "getDocument"; //$NON-NLS-1$

    // Procedure Parameter Name
    String ID = "id"; //$NON-NLS-1$
    String RESULT = "result"; //$NON-NLS-1$
    String KEYSPACE = "keyspace"; //$NON-NLS-1$

    String KEYS = "KEYS"; //$NON-NLS-1$

    String N1QL_COLUMN_ALIAS_PREFIX = "$cb_c"; //$NON-NLS-1$
    String N1QL_TABLE_ALIAS_PREFIX = "$cb_t"; //$NON-NLS-1$

    String EMPTY_STRING = "";//$NON-NLS-1$

}
