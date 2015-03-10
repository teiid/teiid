package org.jboss.resteasy.examples.customer;

import java.util.Set;
import java.util.HashSet;
import javax.ws.rs.core.Application;

public class MyRESTApplication extends Application {

    private Set<Object> singletons = new HashSet<Object>();
    private Set<Class<?>> empty = new HashSet<Class<?>>();
    public MyRESTApplication(){
         singletons.add(new CustomersResource());
    }
    @Override
    public Set<Class<?>> getClasses() {
         return empty;
    }
    @Override
    public Set<Object> getSingletons() {
         return singletons;
    }
}
