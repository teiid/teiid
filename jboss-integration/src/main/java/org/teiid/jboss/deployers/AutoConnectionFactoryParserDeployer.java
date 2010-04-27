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
package org.teiid.jboss.deployers;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.sax.SAXSource;

import org.jboss.deployers.vfs.spi.deployer.AbstractVFSParsingDeployer;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.jboss.util.xml.JBossEntityResolver;
import org.jboss.virtual.VirtualFile;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;


public class AutoConnectionFactoryParserDeployer extends AbstractVFSParsingDeployer<AutoConnectionFactoryDeploymentGroup> {   
   
   public AutoConnectionFactoryParserDeployer() {
      super(AutoConnectionFactoryDeploymentGroup.class);
      setIncludeDeploymentFile(true);
      setSuffix("-ds.xml"); //$NON-NLS-1$
   }
   
   @Override
   protected AutoConnectionFactoryDeploymentGroup parse(VFSDeploymentUnit unit, VirtualFile file, AutoConnectionFactoryDeploymentGroup root) throws Exception {
	  JAXBContext context = JAXBContext.newInstance(new Class[] {AutoConnectionFactoryDeploymentGroup.class});
      Unmarshaller um = context.createUnmarshaller();      
      InputStream is = file.openStream();

      try{
         InputSource input = new InputSource(is);
         input.setSystemId(file.toURI().toString());
         XMLReader reader = XMLReaderFactory.createXMLReader();
         reader.setEntityResolver(new JBossEntityResolver());
         SAXSource source = new SAXSource(reader, input);
         JAXBElement<AutoConnectionFactoryDeploymentGroup> elem = um.unmarshal(source, AutoConnectionFactoryDeploymentGroup.class);
         AutoConnectionFactoryDeploymentGroup deployment = elem.getValue();
         return deployment;
      }      
      finally {
         if (is != null)
            is.close();            
      }
   }
}
