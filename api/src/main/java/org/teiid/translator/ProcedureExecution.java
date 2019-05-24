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

package org.teiid.translator;

import java.util.List;

import org.teiid.language.Call;

/**
 * The procedure execution represents the case where a connector can execute a
 * {@link Call}. The output may include 0 or more output parameters and
 * optionally a result set.
 */
public interface ProcedureExecution extends ResultSetExecution {

    /**
     * Get the output parameter values.  Results should place the return parameter
     * first if it is present, then the IN/OUT and OUT parameters should follow in
     * the order they appeared in the command.
     * @throws TranslatorException If an error occurs while retrieving the output value
     */
    List<?> getOutputParameterValues() throws TranslatorException;

}
