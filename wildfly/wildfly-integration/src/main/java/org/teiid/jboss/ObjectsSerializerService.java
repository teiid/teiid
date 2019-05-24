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

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

class ObjectsSerializerService implements Service<ObjectSerializer> {
    private InjectedValue<String> pathInjector = new InjectedValue<String>();
    private ObjectSerializer serializer;

    public ObjectsSerializerService(){
    }

    @Override
    public void start(StartContext context) throws StartException {
        this.serializer = new ObjectSerializer(pathInjector.getValue());
    }

    @Override
    public void stop(StopContext context) {
    }

    @Override
    public ObjectSerializer getValue() throws IllegalStateException, IllegalArgumentException {
        return this.serializer;
    }

    public InjectedValue<String> getPathInjector() {
        return this.pathInjector;
    }
}
