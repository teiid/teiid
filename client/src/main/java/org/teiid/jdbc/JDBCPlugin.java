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

package org.teiid.jdbc;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

/**
 * JDBCPlugin
 * <p>Used here in <code>jdbc</code> to have access to the new
 * logging framework.
 */
public class JDBCPlugin { // extends Plugin {

    public static final String PLUGIN_ID = "org.teiid.jdbc" ; //$NON-NLS-1$

    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
                                                         PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$
    public static enum Event implements BundleUtil.Event {
        TEIID20000,
        TEIID20001,
        TEIID20002,
        TEIID20003,
        TEIID20005,
        TEIID20007,
        TEIID20008,
        TEIID20009,
        TEIID20010,
        TEIID20012,
        TEIID20013,
        TEIID20014,
        TEIID20016,
        TEIID20018,
        TEIID20019,
        TEIID20020,
        TEIID20021,
        TEIID20023,
        TEIID20027,
        TEIID20028,
        TEIID20029,
        TEIID20030,
        TEIID20031,
        TEIID20032,
        TEIID20033,
        TEIID20034,
        TEIID20035,
        TEIID20036,
        TEIID20037,
        TEIID20038,
        TEIID20039,
    }
}
