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

import java.text.SimpleDateFormat;
import java.util.Date;


public class Util {

	public static String stripQutes(String id) {
		if((id.startsWith("'") && id.endsWith("'"))) { //$NON-NLS-1$ //$NON-NLS-2$
			id = id.substring(1,id.length()-1);
		} else if ((id.startsWith("\"") && id.endsWith("\""))) { //$NON-NLS-1$ //$NON-NLS-2$
			id = id.substring(1,id.length()-1);
		}
		return id;
	}
	
	public static String addSingleQuotes(String text) {
		StringBuffer result = new StringBuffer();
		if(!text.startsWith("'")) { //$NON-NLS-1$
			result.append('\'');
		}
		result.append(text);
		if(!text.endsWith("'")) { //$NON-NLS-1$
			result.append('\'');
		} 
		return result.toString();
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
