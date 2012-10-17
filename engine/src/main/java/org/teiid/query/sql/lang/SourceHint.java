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

package org.teiid.query.sql.lang;

import java.util.Map;
import java.util.TreeMap;

import org.teiid.core.util.EquivalenceUtil;

public class SourceHint {
	
	public static class SpecificHint {
		String hint;
		boolean useAliases;

		public SpecificHint(String hint, boolean useAliases) {
			this.hint = hint;
			this.useAliases = useAliases;
		}
		
		public String getHint() {
			return hint;
		}
		public boolean isUseAliases() {
			return useAliases;
		}
	}
	
	private boolean useAliases;
	private String generalHint;
	private Map<String, SpecificHint> sourceHints;
	
	public String getGeneralHint() {
		return generalHint;
	}
	
	public void setGeneralHint(String generalHint) {
		this.generalHint = generalHint;
	}
	
	public void setSourceHint(String translatorName, String hint, boolean useAliases) {
		if (this.sourceHints == null) {
			this.sourceHints = new TreeMap<String, SpecificHint>(String.CASE_INSENSITIVE_ORDER);
		}
		this.sourceHints.put(translatorName, new SpecificHint(hint, useAliases));
	}
	
	public SpecificHint getSpecificHint(String sourceName) {
		if (this.sourceHints == null) {
			return null;
		}
		return this.sourceHints.get(sourceName);
	}

	public String getSourceHint(String sourceName) {
		SpecificHint sp = getSpecificHint(sourceName);
		if (sp != null) {
			return sp.getHint();
		}
		return null;
	}
	
	public Map<String, SpecificHint> getSpecificHints() {
		return sourceHints;
	}
	
	public boolean isUseAliases() {
		return useAliases;
	}
	
	public void setUseAliases(boolean useAliases) {
		this.useAliases = useAliases;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof SourceHint)) {
			return false;
		}
		SourceHint other = (SourceHint)obj;
		return EquivalenceUtil.areEqual(generalHint, other.generalHint) 
		&& EquivalenceUtil.areEqual(this.sourceHints, other.sourceHints);
	}
	
}
