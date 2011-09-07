
package com.sforce.soap.partner.fault;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the com.sforce.soap.partner.fault package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _UnexpectedErrorFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "UnexpectedErrorFault");
    private final static QName _Fault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "fault");
    private final static QName _LoginFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "LoginFault");
    private final static QName _InvalidQueryLocatorFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "InvalidQueryLocatorFault");
    private final static QName _InvalidNewPasswordFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "InvalidNewPasswordFault");
    private final static QName _InvalidSObjectFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "InvalidSObjectFault");
    private final static QName _MalformedQueryFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "MalformedQueryFault");
    private final static QName _InvalidIdFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "InvalidIdFault");
    private final static QName _InvalidFieldFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "InvalidFieldFault");
    private final static QName _MalformedSearchFault_QNAME = new QName("urn:fault.partner.soap.sforce.com", "MalformedSearchFault");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: com.sforce.soap.partner.fault
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link InvalidFieldFault }
     * 
     */
    public InvalidFieldFault createInvalidFieldFault() {
        return new InvalidFieldFault();
    }

    /**
     * Create an instance of {@link InvalidIdFault }
     * 
     */
    public InvalidIdFault createInvalidIdFault() {
        return new InvalidIdFault();
    }

    /**
     * Create an instance of {@link InvalidSObjectFault }
     * 
     */
    public InvalidSObjectFault createInvalidSObjectFault() {
        return new InvalidSObjectFault();
    }

    /**
     * Create an instance of {@link MalformedQueryFault }
     * 
     */
    public MalformedQueryFault createMalformedQueryFault() {
        return new MalformedQueryFault();
    }

    /**
     * Create an instance of {@link InvalidNewPasswordFault }
     * 
     */
    public InvalidNewPasswordFault createInvalidNewPasswordFault() {
        return new InvalidNewPasswordFault();
    }

    /**
     * Create an instance of {@link LoginFault }
     * 
     */
    public LoginFault createLoginFault() {
        return new LoginFault();
    }

    /**
     * Create an instance of {@link MalformedSearchFault }
     * 
     */
    public MalformedSearchFault createMalformedSearchFault() {
        return new MalformedSearchFault();
    }

    /**
     * Create an instance of {@link ApiFault }
     * 
     */
    public ApiFault createApiFault() {
        return new ApiFault();
    }

    /**
     * Create an instance of {@link InvalidQueryLocatorFault }
     * 
     */
    public InvalidQueryLocatorFault createInvalidQueryLocatorFault() {
        return new InvalidQueryLocatorFault();
    }

    /**
     * Create an instance of {@link UnexpectedErrorFault }
     * 
     */
    public UnexpectedErrorFault createUnexpectedErrorFault() {
        return new UnexpectedErrorFault();
    }

    /**
     * Create an instance of {@link ApiQueryFault }
     * 
     */
    public ApiQueryFault createApiQueryFault() {
        return new ApiQueryFault();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link UnexpectedErrorFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "UnexpectedErrorFault")
    public JAXBElement<UnexpectedErrorFault> createUnexpectedErrorFault(UnexpectedErrorFault value) {
        return new JAXBElement<UnexpectedErrorFault>(_UnexpectedErrorFault_QNAME, UnexpectedErrorFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ApiFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "fault")
    public JAXBElement<ApiFault> createFault(ApiFault value) {
        return new JAXBElement<ApiFault>(_Fault_QNAME, ApiFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link LoginFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "LoginFault")
    public JAXBElement<LoginFault> createLoginFault(LoginFault value) {
        return new JAXBElement<LoginFault>(_LoginFault_QNAME, LoginFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidQueryLocatorFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "InvalidQueryLocatorFault")
    public JAXBElement<InvalidQueryLocatorFault> createInvalidQueryLocatorFault(InvalidQueryLocatorFault value) {
        return new JAXBElement<InvalidQueryLocatorFault>(_InvalidQueryLocatorFault_QNAME, InvalidQueryLocatorFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidNewPasswordFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "InvalidNewPasswordFault")
    public JAXBElement<InvalidNewPasswordFault> createInvalidNewPasswordFault(InvalidNewPasswordFault value) {
        return new JAXBElement<InvalidNewPasswordFault>(_InvalidNewPasswordFault_QNAME, InvalidNewPasswordFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidSObjectFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "InvalidSObjectFault")
    public JAXBElement<InvalidSObjectFault> createInvalidSObjectFault(InvalidSObjectFault value) {
        return new JAXBElement<InvalidSObjectFault>(_InvalidSObjectFault_QNAME, InvalidSObjectFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link MalformedQueryFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "MalformedQueryFault")
    public JAXBElement<MalformedQueryFault> createMalformedQueryFault(MalformedQueryFault value) {
        return new JAXBElement<MalformedQueryFault>(_MalformedQueryFault_QNAME, MalformedQueryFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidIdFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "InvalidIdFault")
    public JAXBElement<InvalidIdFault> createInvalidIdFault(InvalidIdFault value) {
        return new JAXBElement<InvalidIdFault>(_InvalidIdFault_QNAME, InvalidIdFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidFieldFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "InvalidFieldFault")
    public JAXBElement<InvalidFieldFault> createInvalidFieldFault(InvalidFieldFault value) {
        return new JAXBElement<InvalidFieldFault>(_InvalidFieldFault_QNAME, InvalidFieldFault.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link MalformedSearchFault }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "urn:fault.partner.soap.sforce.com", name = "MalformedSearchFault")
    public JAXBElement<MalformedSearchFault> createMalformedSearchFault(MalformedSearchFault value) {
        return new JAXBElement<MalformedSearchFault>(_MalformedSearchFault_QNAME, MalformedSearchFault.class, null, value);
    }

}
