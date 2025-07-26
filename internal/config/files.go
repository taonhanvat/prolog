package config

import (
	"fmt"
	"os"
)

var (
	CAFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile         = configFile("model.conf")
	ACLPolicyFile        = configFile("policy.csv")
)

func configFile(fileName string) string {
	var dir string
	dir = os.Getenv("CERT_DIR")
	if dir == "" {
		dir, _ = os.UserHomeDir()
		dir = dir + "/.prolog"
	}
	return fmt.Sprintf("%s/%s", dir, fileName)
}
