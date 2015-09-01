package org.teiid.jboss.rest;

import io.swagger.jaxrs.config.BeanConfig;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BootstrapServlet extends HttpServlet {

    private static final long serialVersionUID = 5704762873796188429L;
    
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        
        BeanConfig beanConfig = new BeanConfig();
        init(beanConfig);
    }

    protected void init(BeanConfig beanConfig) {        
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)throws ServletException, IOException {
        String str1 = req.getContextPath();
        String str2 = req.getScheme() + "://" + req.getServerName() + ":" + req.getServerPort() + str1 + "/";
        String str3 = str1 + "/api.html";
        String str4 = "swagger.json";
        String str5 = "/url=" + str2 + str4;
        String str6 = str3 + "?" + str5;
        resp.sendRedirect(str6);
    }

    
    
    

}
