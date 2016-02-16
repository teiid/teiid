CREATE VIEW stock (
                symbol varchar,
                price decimal
                ) AS select null, null;

CREATE function func (val string) returns integer options (JAVA_CLASS 'org.teiid.arquillian.SampleFunctions',  JAVA_METHOD 'doSomething');