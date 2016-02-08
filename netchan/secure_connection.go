package netchan

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
)

type CertFiles struct {
	CertFile string // A PEM eoncoded certificate file.
	KeyFile  string // A PEM encoded private key file.
	CaFile   string // A PEM eoncoded CA's certificate file.
}

func (c *CertFiles) IsEnabled() bool {
	return c.CaFile != ""
}

func (c *CertFiles) MakeTLSConfig() *tls.Config {
	if !c.IsEnabled() {
		return nil
	}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(c.CaFile)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Create tls config
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		ClientAuth:         tls.RequireAndVerifyClientCert, // server side setting
		InsecureSkipVerify: false,                          // client side setting
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig
}
