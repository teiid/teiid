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

package org.teiid.translator.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.StringUtil;

/**
 * Represents a comparable version
 */
public class Version implements Comparable<Version> {
	
	public static Version DEFAULT_VERSION = new Version(new Integer[] {0}); 
	private static final Pattern NUMBER_PATTERN = Pattern.compile("(\\d+)"); //$NON-NLS-1$

	private Integer[] parts;
	
	public static Version getVersion(String version) {
		if (version == null) {
			return null;
		}
		String[] parts = version.split("\\."); //$NON-NLS-1$
		List<Integer> versionParts = new ArrayList<Integer>();
		for (String part : parts) {
			Integer val = null;
			Matcher m = NUMBER_PATTERN.matcher(part);
			if (!m.find()) {
				continue;
			}

			String num = m.group(1);
			try {
				val = Integer.parseInt(num);
			} catch (NumberFormatException e) {
				
			}
			versionParts.add(val == null ? 0 : val);
		}
		if (versionParts.isEmpty()) {
			return DEFAULT_VERSION;
		}
		return new Version(versionParts.toArray(new Integer[versionParts.size()]));
	}
	
	Version(Integer[] parts) {
		this.parts = parts;
	}
	
	@Override
	public String toString() {
		return StringUtil.toString(this.parts, ".", false); //$NON-NLS-1$
	}
	
	public int getMajorVersion() {
		return parts[0];
	}
	
	@Override
	public int compareTo(Version o) {
		int length = Math.min(this.parts.length, o.parts.length);
		for (int i = 0; i < length; i++) {
			int comp = this.parts[i].compareTo(o.parts[i]);
			if (comp != 0) {
				return comp;
			}
		}
		if (this.parts.length > length) {
			return 1;
		}
		if (o.parts.length > length) {
			return -1;
		}
		return 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof Version)) {
			return false;
		}
		return this.compareTo((Version)obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(parts);
	}

}
