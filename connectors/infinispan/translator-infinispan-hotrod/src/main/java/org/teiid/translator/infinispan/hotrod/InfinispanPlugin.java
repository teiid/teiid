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
package org.teiid.translator.infinispan.hotrod;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class InfinispanPlugin {

    public static final String PLUGIN_ID = InfinispanPlugin.class.getPackage().getName();
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
            PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$

	public static enum Event implements BundleUtil.Event{
		TEIID25000,
		TEIID25001,
		TEIID25002,
		TEIID25003,
		TEIID25004,
		TEIID25005,
		TEIID25006,
		TEIID25007,
		TEIID25008,
		TEIID25009,
		TEIID25010,
		TEIID25011,
		TEIID25012,
		TEIID25013,
		TEIID25014,
		TEIID25015,
		TEIID25016
	}
}
