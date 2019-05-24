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
package org.teiid.odata.api;

import org.apache.olingo.commons.api.edm.EdmReturnType;

public interface ProcedureReturnType {
    boolean hasResultSet();
    /**
     * @return the sql type of the return parameter or null if there is no return parameter
     */
    Integer getSqlType();
    /**
     *
     * @return the return type or null, if no resultset nor return parameter
     */
    EdmReturnType getReturnType();
}