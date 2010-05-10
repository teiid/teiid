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

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.sax.SAXSource;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.managed.ManagedObjectCreator;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.deployers.vfs.spi.deployer.AbstractVFSParsingDeployer;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CollectionValue;
import org.jboss.metatype.api.values.GenericValue;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.util.xml.JBossEntityResolver;
import org.jboss.virtual.VirtualFile;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;


public class TranslatorParserDeployer extends AbstractVFSParsingDeployer<TranslatorMetaDataGroup> implements ManagedObjectCreator {   
   public static final String TRANSLATOR_SUFFIX = "-translator.xml"; //$NON-NLS-1$
   
   public TranslatorParserDeployer() {
      super(TranslatorMetaDataGroup.class);
      setIncludeDeploymentFile(true);
      setSuffix(TRANSLATOR_SUFFIX);
      setBuildManagedObject(true);
   }
   
   @Override
   protected TranslatorMetaDataGroup parse(VFSDeploymentUnit unit, VirtualFile file, TranslatorMetaDataGroup root) throws Exception {
	  JAXBContext context = JAXBContext.newInstance(new Class[] {TranslatorMetaDataGroup.class});
      Unmarshaller um = context.createUnmarshaller();      
      InputStream is = file.openStream();

      try{
         InputSource input = new InputSource(is);
         input.setSystemId(file.toURI().toString());
         XMLReader reader = XMLReaderFactory.createXMLReader();
         reader.setEntityResolver(new JBossEntityResolver());
         SAXSource source = new SAXSource(reader, input);
         JAXBElement<TranslatorMetaDataGroup> elem = um.unmarshal(source, TranslatorMetaDataGroup.class);
         TranslatorMetaDataGroup deployment = elem.getValue();
         return deployment;
      }      
      finally {
         if (is != null)
            is.close();            
      }
   }
   
	public void build(DeploymentUnit unit, Set<String> outputs, Map<String, ManagedObject> managedObjects) throws DeploymentException {
		if (isBuildManagedObject()) {
			ManagedObject mo = managedObjects.get(TranslatorMetaDataGroup.class.getName());
			if (mo != null) {
				ManagedProperty translators = mo.getProperty("translators"); //$NON-NLS-1$
				MetaType propType = translators.getMetaType();
				if (propType.isCollection()) {
					CollectionValue value = (CollectionValue) translators.getValue();
					if (value != null) {
						for (MetaValue element:value.getElements()) {
							ManagedObject translator = (ManagedObject)((GenericValue)element).getValue();
							managedObjects.put(translator.getName(), translator);
						}
					}
				}
			}
		}
	}
}
