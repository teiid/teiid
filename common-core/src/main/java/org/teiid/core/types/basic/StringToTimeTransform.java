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

package org.teiid.core.types.basic;

import java.sql.Time;
import java.util.regex.Pattern;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;


public class StringToTimeTransform extends Transform {

    private static boolean validate = true;
    private static Pattern pattern = Pattern.compile("\\d{2}:\\d{2}:\\d{2}"); //$NON-NLS-1$

    static {
        try {
            Time.valueOf("26:10:10"); //$NON-NLS-1$
        } catch (Exception e) {
            validate = false;
        }
    }

    /**
     * This method transforms a value of the source type into a value
     * of the target type.
     * @param value Incoming value of source type
     * @return Outgoing value of target type
     * @throws TransformationException if value is an incorrect input type or
     * the transformation fails
     */
    public Object transformDirect(Object value) throws TransformationException {
        value = ((String) value).trim();
        Time result = null;
        try {
            result = Time.valueOf((String)value);
        } catch(Exception e) {
            if (!validate && pattern.matcher((String)value).matches()) {
                throw new TransformationException(CorePlugin.Event.TEIID10060, CorePlugin.Util.gs(CorePlugin.Event.TEIID10060, value, getTargetType().getSimpleName()));
            }
              throw new TransformationException(CorePlugin.Event.TEIID10068, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10068, value));
        }
        if (validate && !result.toString().equals(value)) {
              throw new TransformationException(CorePlugin.Event.TEIID10060, CorePlugin.Util.gs(CorePlugin.Event.TEIID10060, value, getTargetType().getSimpleName()));
        }
        return result;
    }

    /**
     * Type of the incoming value.
     * @return Source type
     */
    public Class<?> getSourceType() {
        return DataTypeManager.DefaultDataClasses.STRING;
    }

    /**
     * Type of the outgoing value.
     * @return Target type
     */
    public Class<?> getTargetType() {
        return DataTypeManager.DefaultDataClasses.TIME;
    }

    @Override
    public boolean isExplicit() {
        return true;
    }

}
