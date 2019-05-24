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

package org.teiid.translator.jdbc;

import java.sql.SQLException;
import java.util.Arrays;

import org.teiid.core.BundleUtil;
import org.teiid.translator.TranslatorException;


public class JDBCExecutionException extends TranslatorException {

    private static final long serialVersionUID = 1758087499488916573L;

    public JDBCExecutionException(BundleUtil.Event event, SQLException error,TranslatedCommand... commands) {
        super(error, commands == null || commands.length == 0 ? event.toString()+":"+error.getMessage() : event.toString()+":"+JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11004, Arrays.toString(commands))); //$NON-NLS-1$ //$NON-NLS-2$
        setCode(String.valueOf(error.getErrorCode()));
    }

    public JDBCExecutionException(BundleUtil.Event event, SQLException error, String command) {
        super(error, event.toString()+":"+JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11004, command)); //$NON-NLS-1$
        setCode(String.valueOf(error.getErrorCode()));
    }
}
