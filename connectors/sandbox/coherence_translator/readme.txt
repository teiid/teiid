{\rtf1\ansi\ansicpg1252\cocoartf1038\cocoasubrtf350
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
\margl1440\margr1440\vieww15400\viewh13820\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\ql\qnatural\pardirnatural

\f0\fs24 \cf0 translator readme\
\
The coherence_translator is very simple implementation of mapping a Coherence cache to relational sql request.    This translator requires the deployment of the coherence_connector.\
\
This translator is coded to expect Trades and Legs to be in the cache.   It was not generically implemented to handle any type of objects loaded into the cache.   Feel free to rework it to make it better.\
\
\
There is an example model (Coherence.xmi) and vdb (Coherence.vdb) that are located in the src/test/resources directory.  These can be used as a example for modeling Coherence as a source.\
\
\
BUILD:\
---------\
run  mvn clean install\
\
\
\
DEPLOYMENT\
--------------------\
\
setup \
\
	1.	see coherence_connector for deployment\
	2.	deploy the Coherence.vdb from src/test/resources   to the <profile>/deploy/ directory\
	3.	copy the coherence_translator-0.1.jar to server/<profile>/deploy/teiid directory\
	4. 	restart server\
\
	NOTE: to preload a coherence cache with test data, build the coherence_translotr project, the junit tests will load the cache.\
\
	5.	use the teiid simpleclient example to run the following:\
\
	./run.sh localhost 31000 Coherence "select * from trade"\
\
\
Other notes:\
-	the coherence translator has the translator name of "coherence", which must match the translator defined in the vdb that's mapped to the Coherence source.\
\
}