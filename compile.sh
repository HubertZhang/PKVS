#!/bin/sh

export GOPATH="`pwd`"

cd $GOPATH/src/paxos
go install

cd $GOPATH/src/server
go install
