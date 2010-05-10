/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.deployers;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.adminapi.impl.TranslatorMetaData;

@ManagementObject
@XmlRootElement(name = "translator-factory")
public class TranslatorMetaDataGroup {

	@XmlElement(name = "translator", type = TranslatorMetaData.class)
	protected List<TranslatorMetaData> translators = new ArrayList<TranslatorMetaData>();
	
	@ManagementProperty(description="translators", managed=true)
	public List<TranslatorMetaData> getTranslators(){
		return this.translators;
	}
	
	public void addTranslator(TranslatorMetaData data) {
		this.translators.add(data);
	}
}
