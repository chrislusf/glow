#!/bin/bash
# Use this script to regenerate the test certs in test_certs.

echo "Removing test_certs and generates new cert pairs for testing ..."

rm -rf test_certs
mkdir test_certs

echo "Making server cert ..."
openssl req -new -nodes -x509 -out test_certs/server.pem \
  -keyout test_certs/server.key \
  -days 3650 -subj "/emailAddress=test@test.com"

echo "Making client cert ..."
openssl req -new -nodes -x509 -out test_certs/client.pem \
  -keyout test_certs/client.key \
  -days 3650 -subj "/emailAddress=test@test.com"
