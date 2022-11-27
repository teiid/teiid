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
package org.teiid.translator.salesforce;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.translator.TypeFacility;

public class Util {

    /**
     * Convert the Teiid value to one that Salesforce will correctly serialized
     * @param value
     * @param type
     * @return
     */
    public static Object toSalesforceObjectValue(Object value, Class<?> type) {
        if (value != null) {
            if (type == TypeFacility.RUNTIME_TYPES.TIME) {
                return new com.sforce.ws.types.Time(((Time)value).getTime());
            } else if (type == TypeFacility.RUNTIME_TYPES.TIMESTAMP) {
                Calendar cal = (Calendar) TimestampWithTimezone.getCalendar().clone();
                cal.setTimeInMillis(((Timestamp)value).getTime());
                return cal;
            }
            //all other pushdown types are directly supported
        }
        return value;
    }

    private static String timeZone;

    public static void resetTimeZone() {
        timeZone = null;
    }

    public static String getDefaultTimeZoneString() {
        if (timeZone == null) {
            String s = new SimpleDateFormat("Z").format(new Date(0)); //$NON-NLS-1$
            timeZone = s.substring(0, 3) + ':'  + s.substring(3, 5);
        }
        return timeZone;
    }

}
