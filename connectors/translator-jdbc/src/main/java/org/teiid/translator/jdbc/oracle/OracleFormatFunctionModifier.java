/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.translator.jdbc.oracle;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.StringUtil;
import org.teiid.translator.jdbc.ParseFormatFunctionModifier;

public final class OracleFormatFunctionModifier extends
		ParseFormatFunctionModifier {

	public static final Pattern tokenPattern = Pattern.compile("(G|y{1,4}|M{1,4}|D{1,3}|d{1,2}|E{1,4}|a{1,2}|H{1,2}|h{1,2}|m{1,2}|s{1,2}|S{1,3}|Z|[\\- /,.;:]+|(?:'[^'\"]*')+|[^'\"a-zA-Z]+)"); //$NON-NLS-1$

	public OracleFormatFunctionModifier(String prefix) {
		super(prefix);
	}
	
	public static boolean supportsLiteral(String literal) {
		Matcher m = tokenPattern.matcher(literal);
		int end = 0;
    	while (m.find()) {
    		if (end != m.start()) {
    			return false;
    		}
    		end = m.end();
    	}
    	return end == literal.length();
	}

	@Override
	protected Object translateFormat(String format) {
		Matcher m = tokenPattern.matcher(format);
		StringBuilder sb = new StringBuilder();
		while (m.find()) {
			if (m.group().length() == 0) {
				continue;
			}
			String group = m.group();
			sb.append(convertToken(group));
		} 
		return sb.toString();
	}

	private Object convertToken(String group) {
		switch (group.charAt(0)) {
		case 'G':
			return "AD"; //$NON-NLS-1$
		case 'y':
			if (group.length() == 4) {
				return "YYYY"; //$NON-NLS-1$
			}
			return "YY"; //$NON-NLS-1$
		case 'M':
			if (group.length() <= 2) {
				return "MM"; //$NON-NLS-1$
			}
			if (group.length() == 3) {
				return "Mon"; //$NON-NLS-1$
			}
			return "Month"; //$NON-NLS-1$
		case 'D':
			return "DDD"; //$NON-NLS-1$
		case 'd':
			return "DD"; //$NON-NLS-1$
		case 'E':
			if (group.length() == 4) {
				return "Day"; //$NON-NLS-1$
			}
			return "Dy"; //$NON-NLS-1$
		case 'a':
			return "PM"; //$NON-NLS-1$
		case 'H':
			return "HH24"; //$NON-NLS-1$
		case 'h':
			return "HH"; //$NON-NLS-1$
		case 'm':
			return "MI"; //$NON-NLS-1$
		case 's':
			return "SS"; //$NON-NLS-1$
		case 'S':
			return "FF" + group.length(); //$NON-NLS-1$
		case 'Z':
			return "TZHTZM";//$NON-NLS-1$
		case '\'':
			return '"' + StringUtil.replace(group.substring(1, group.length() - 1), "''", "'") + '"'; //$NON-NLS-1$ //$NON-NLS-2$
		case ' ':
		case '-':
		case '/':
		case ',':
		case '.':
		case ';':
		case ':':
			return group;
		default:
			return '"' + group + '"';
		}
	}
}