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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;

import org.jboss.resource.metadata.mcf.ManagedConnectionFactoryDeploymentMetaData;


@XmlAccessorType(XmlAccessType.NONE)
@XmlRootElement
public class AutoConnectionFactoryDeploymentGroup implements Serializable {
   private static final long serialVersionUID = -7650097438654698297L;

   @XmlElements({@XmlElement(name="local-tx-datasource", type=ManagedConnectionFactoryDeploymentMetaData.class),
                 @XmlElement(name="no-tx-datasource", type=ManagedConnectionFactoryDeploymentMetaData.class),
                 @XmlElement(name="xa-datasource", type=ManagedConnectionFactoryDeploymentMetaData.class)})
   List<ManagedConnectionFactoryDeploymentMetaData> deployments = new ArrayList<ManagedConnectionFactoryDeploymentMetaData>();
   
   public List<ManagedConnectionFactoryDeploymentMetaData> getDeployments() {
      return Collections.unmodifiableList(deployments);
   }   
}
