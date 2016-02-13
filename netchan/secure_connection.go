package netchan

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"

	"github.com/chrislusf/glow/util"
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

	certFile, err := filepath.Abs(util.CleanPath(c.CertFile))
	if err != nil {
		panic(fmt.Errorf("Failed to load cert file %s: %v", c.CertFile, err))
	}
	keyFile, err := filepath.Abs(util.CleanPath(c.KeyFile))
	if err != nil {
		panic(fmt.Errorf("Failed to load cert file %s: %v", c.KeyFile, err))
	}
	caFile, err := filepath.Abs(util.CleanPath(c.CaFile))
	if err != nil {
		panic(fmt.Errorf("Failed to load cert file %s: %v", c.CaFile, err))
	}

	// Load cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Create tls config
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		ClientCAs:          caCertPool,
		ClientAuth:         tls.RequireAndVerifyClientCert, // server side setting
		InsecureSkipVerify: false,                          // client side setting
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig
}
