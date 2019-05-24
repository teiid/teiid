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
package org.teiid.translator.phoenix;

import java.io.FileReader;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;

public class TestPhoenixUtil {

    public static TransformationMetadata queryMetadataInterface() {

        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("HBaseModel");

            MetadataFactory mf = new MetadataFactory("hbase", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), mmd);
            mf.setParser(new QueryParser());
            mf.parse(new FileReader(UnitTestUtil.getTestDataFile("customer.ddl")));

            TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x");
            ValidatorReport report = new MetadataValidator().validate(tm.getVdbMetaData(), tm.getMetadataStore());
            if (report.hasItems()) {
                throw new RuntimeException(report.getFailureMessage());
            }
            return tm;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
