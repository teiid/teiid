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
package org.teiid.translator.jdbc.ucanaccess;


import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.hsql.HsqlExecutionFactory;

@Translator(name="ucanaccess", description="A translator for read/write Microsoft Access Database")
public class UCanAccessExecutionFactory extends HsqlExecutionFactory {

    public static final String UCANACCESS = "ucanaccess"; //$NON-NLS-1$

    public UCanAccessExecutionFactory() {
        setSupportsOrderBy(true);
        setMaxInCriteriaSize(JDBCExecutionFactory.DEFAULT_MAX_IN_CRITERIA);
        setMaxDependentInPredicates(10);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();

        addPushDownFunction(UCANACCESS, "DCount", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(UCANACCESS, "DSum", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(UCANACCESS, "DMax", TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(UCANACCESS, "DMin", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(UCANACCESS, "DAvg", TypeFacility.RUNTIME_NAMES.BIG_INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(UCANACCESS, "DFirst", TypeFacility.RUNTIME_NAMES.OBJECT, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(UCANACCESS, "DLast", TypeFacility.RUNTIME_NAMES.OBJECT, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
    }

    @Override
    public boolean supportsDependentJoins() {
        return false;
    }
}
