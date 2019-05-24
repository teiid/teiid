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

package org.teiid.jboss;

import org.jboss.as.server.deployment.AttachmentKey;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;

public final class TeiidAttachments {

    enum DeploymentType{VDB, VDB_XML, TRANSLATOR, VDB_DDL};

    public static final AttachmentKey<VDBTranslatorMetaData> TRANSLATOR_METADATA = AttachmentKey.create(VDBTranslatorMetaData.class);
    public static final AttachmentKey<VDBMetaData> VDB_METADATA = AttachmentKey.create(VDBMetaData.class);

    public static final AttachmentKey<DeploymentType> DEPLOYMENT_TYPE = AttachmentKey.create(DeploymentType.class);

    public static boolean isVDBDeployment(final DeploymentUnit deploymentUnit) {
        return DeploymentType.VDB == deploymentUnit.getAttachment(DEPLOYMENT_TYPE)
                || DeploymentType.VDB_XML == deploymentUnit.getAttachment(DEPLOYMENT_TYPE)
                || DeploymentType.VDB_DDL == deploymentUnit.getAttachment(DEPLOYMENT_TYPE);
    }

    public static boolean isVDBXMLDeployment(final DeploymentUnit deploymentUnit) {
        return DeploymentType.VDB_XML == deploymentUnit.getAttachment(DEPLOYMENT_TYPE);
    }

    public static boolean isVDBDDLDeployment(final DeploymentUnit deploymentUnit) {
        return DeploymentType.VDB_DDL == deploymentUnit.getAttachment(DEPLOYMENT_TYPE);
    }

    public static void setAsVDBDeployment(final DeploymentUnit deploymentUnit) {
        deploymentUnit.putAttachment(DEPLOYMENT_TYPE, DeploymentType.VDB);
    }

    public static void setAsVDBXMLDeployment(final DeploymentUnit deploymentUnit) {
        deploymentUnit.putAttachment(DEPLOYMENT_TYPE, DeploymentType.VDB_XML);
    }

    public static void setAsVDBDDLDeployment(final DeploymentUnit deploymentUnit) {
        deploymentUnit.putAttachment(DEPLOYMENT_TYPE, DeploymentType.VDB_DDL);
    }

    public static void setAsTranslatorDeployment(final DeploymentUnit deploymentUnit) {
        deploymentUnit.putAttachment(DEPLOYMENT_TYPE, DeploymentType.TRANSLATOR);
    }

    public static boolean isTranslator(final DeploymentUnit deploymentUnit) {
        return DeploymentType.TRANSLATOR == deploymentUnit.getAttachment(DEPLOYMENT_TYPE);
    }
}
