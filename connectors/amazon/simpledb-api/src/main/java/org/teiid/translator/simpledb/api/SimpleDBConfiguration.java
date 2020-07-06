/*
 * Copyright 2012-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.simpledb.api;

public class SimpleDBConfiguration implements BaseSimpleDBConfiguration {
    private String accessKey;
    private String secretAccessKey;

    @Override
    public String getAccessKey() {
        return accessKey;
    }

    @Override
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    private SimpleDBConfiguration(Builder builder) {
        this.accessKey = builder.accessKey;
        this.secretAccessKey = builder.secretAccessKey;
    }

    public static class Builder {
        private String accessKey;
        private String secretAccessKey;

        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretAccessKey(String secretAccessKey) {
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        public SimpleDBConfiguration build() {
            if(!isValidSimpleDBConfigurationBuilder()) {
                return null;
            }
            return new SimpleDBConfiguration(this);
        }

        private Boolean isValidSimpleDBConfigurationBuilder() {
            if(secretAccessKey == null) {
                return false;
            }
            if(accessKey == null) {
                return false;
            }
            return true;
        }
    }
}