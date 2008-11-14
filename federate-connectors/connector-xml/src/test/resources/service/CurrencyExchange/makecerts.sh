# read the below web site; use cygwin with openssl to create below
#used from http://www.churchillobjects.com/c/11201g.html

mkdir cert
cd cert
mkdir demoCA
cd demoCA
mkdir certs
mkdir crl
touch index.txt
mkdir newcerts
echo "01" > serial
mkdir private
cd ..

# Generate the Certificate Authority Key
openssl genrsa -out ca.key 1024

# make sure use "STLOUIS" && "Missiouri" && "MetaMatrix.com" and "US"
# Create the Self-Signed CA Certificate
openssl req -new -x509 -key ca.key -out demoCA/cacert.pem

#Create a Keystores for the Client and Server
keytool -genkey -alias client -keystore client_trusted.jks -validity 4000 -storepass keystorepassword -keypass clientpassword -dname "CN=client,OU=caller,O=MetaMatrix.com,L=STLOUIS,ST=Missiouri,c=US"

keytool -genkey -alias server -keystore server_trusted.jks -validity 4000 -storepass keystorepassword -keypass serverpassword -dname "CN=server,OU=callie,O=MetaMatrix.com,L=STLOUIS,ST=Missiouri,c=US"

# Export Keys from Keystores
keytool -alias client -keystore client_trusted.jks -storepass keystorepassword -keypass clientpassword -certreq -file client.crs

keytool -alias server -keystore server_trusted.jks -storepass keystorepassword -keypass serverpassword -certreq -file server.crs

# Sign Keys With the CA Certificate
openssl ca -in client.crs -out client.pem -keyfile ca.key
openssl ca -in server.crs -out server.pem -keyfile ca.key

# Convert Keys to DER
openssl x509 -in client.pem -out client.der -outform DER
openssl x509 -in server.pem -out server.der -outform DER

#Import CA Certificate and Keys into Keystores
keytool -keystore client_trusted.jks -alias systemca -import -file demoCA/cacert.pem -storepass keystorepassword
keytool -keystore server_trusted.jks -alias systemca -import -file demoCA/cacert.pem -storepass keystorepassword

keytool -keystore client_trusted.jks -alias client -import -file client.der -storepass keystorepassword -keypass clientpassword
keytool -keystore server_trusted.jks -alias server -import -file server.der -storepass keystorepassword -keypass serverpassword
