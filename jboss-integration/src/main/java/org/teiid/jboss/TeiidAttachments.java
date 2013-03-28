/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc., and individual contributors
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

package org.teiid.jboss;

import org.jboss.as.server.deployment.AttachmentKey;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.UDFMetaData;

public final class TeiidAttachments {
	
	enum DeploymentType{VDB, VDB_XML, TRANSLATOR};
	
	public static final AttachmentKey<VDBTranslatorMetaData> TRANSLATOR_METADATA = AttachmentKey.create(VDBTranslatorMetaData.class);
    public static final AttachmentKey<VDBMetaData> VDB_METADATA = AttachmentKey.create(VDBMetaData.class);
    public static final AttachmentKey<UDFMetaData> UDF_METADATA = AttachmentKey.create(UDFMetaData.class);
    
    public static final AttachmentKey<DeploymentType> DEPLOYMENT_TYPE = AttachmentKey.create(DeploymentType.class);
    
    public static boolean isVDBDeployment(final DeploymentUnit deploymentUnit) {
        return DeploymentType.VDB == deploymentUnit.getAttachment(DEPLOYMENT_TYPE) || DeploymentType.VDB_XML == deploymentUnit.getAttachment(DEPLOYMENT_TYPE);
    }
    
    public static boolean isVDBXMLDeployment(final DeploymentUnit deploymentUnit) {
        return DeploymentType.VDB_XML == deploymentUnit.getAttachment(DEPLOYMENT_TYPE);
    }
    
    public static void setAsVDBDeployment(final DeploymentUnit deploymentUnit) {
        deploymentUnit.putAttachment(DEPLOYMENT_TYPE, DeploymentType.VDB);
    }
    
    public static void setAsVDBXMLDeployment(final DeploymentUnit deploymentUnit) {
        deploymentUnit.putAttachment(DEPLOYMENT_TYPE, DeploymentType.VDB_XML);
    }
    
    public static void setAsTranslatorDeployment(final DeploymentUnit deploymentUnit) {
        deploymentUnit.putAttachment(DEPLOYMENT_TYPE, DeploymentType.TRANSLATOR);
    }
    
    public static boolean isTranslator(final DeploymentUnit deploymentUnit) {
        return DeploymentType.TRANSLATOR == deploymentUnit.getAttachment(DEPLOYMENT_TYPE);
    }    
}
