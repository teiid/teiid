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

package org.teiid.s3;

public interface S3Configuration {

    String getAccessKey();

    String getSecretKey();

    String getBucket();

    String getSseAlgorithm();

    /**
     * base64 encoded key.  If specified server side encryption will be used with the specified {@link #getSseAlgorithm()}
     * @return
     */
    String getSseKey();

    /**
     * If specified it is expected to be the full service endpoint containing protocol, service, region, and hostname information as applicable.
     * @return
     */
    String getEndpoint();

    /**
     * If endpoint is not specified, this is the AWS region.
     * <br>If endpoint is specified, this is the signer region override only and does not affect the endpoint.
     * @return
     */
    String getRegion();

}
