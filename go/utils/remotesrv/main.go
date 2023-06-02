// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"github.com/hashicorp/go-sockaddr/template"
	"github.com/pkg/errors"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
)

func run() error {
	readOnlyParam := flag.Bool("read-only", false, "run a read-only server which does not allow writes")
	repoModeParam := flag.Bool("repo-mode", false, "act as a remote for an existing dolt directory, instead of stand alone")
	dirParam := flag.String("dir", "", "root directory that this command will run in; default cwd")
	grpcAddrParam := flag.String("grpc-addr", "localhost:50051", "the address the grpc server will listen on; default localhost:50051")
	httpAddrParam := flag.String("http-addr", "localhost:80", "the port the http server will listen on; default localhost:80; if http-port is equal to grpc-port, both services will serve over the same port")
	httpHostParam := flag.String("http-host", "", "hostname to use in the host component of the URLs that the server generates; default ''; if '', server will echo the :authority header")
	flag.Parse()

	if dirParam != nil && len(*dirParam) > 0 {
		err := os.Chdir(*dirParam)

		if err != nil {
			log.Fatalln("failed to chdir to:", *dirParam, "error:", err.Error())
		} else {
			log.Println("cwd set to " + *dirParam)
		}
	} else {
		log.Println("'dir' parameter not provided. Using the current working dir.")
	}

	grpcAddr, err := resolveIP(*grpcAddrParam)
	if err != nil {
		return errors.Wrap(err, "parse grpc addr")
	}

	httpAddr, err := resolveIP(*httpAddrParam)
	if err != nil {
		return errors.Wrap(err, "parse http addr")
	}

	fs, err := filesys.LocalFilesysWithWorkingDir(".")
	if err != nil {
		log.Fatalln("could not get cwd path:", err.Error())
	}

	var dbCache remotesrv.DBCache
	if *repoModeParam {
		dEnv := env.Load(context.Background(), env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, "remotesrv")
		if !dEnv.Valid() {
			log.Fatalln("repo-mode failed to load repository")
		}
		db := doltdb.HackDatasDatabaseFromDoltDB(dEnv.DoltDB)
		cs := datas.ChunkStoreFromDatabase(db)
		dbCache = SingletonCSCache{cs.(remotesrv.RemoteSrvStore)}
	} else {
		dbCache = NewLocalCSCache(fs)
	}

	server, err := remotesrv.NewServer(remotesrv.ServerArgs{
		HttpHost:       *httpHostParam,
		HttpListenAddr: httpAddr,
		GrpcListenAddr: grpcAddr,
		FS:             fs,
		DBCache:        dbCache,
		ReadOnly:       *readOnlyParam,
	})
	if err != nil {
		log.Fatalf("error creating remotesrv Server: %v\n", err)
	}
	listeners, err := server.Listeners()
	if err != nil {
		log.Fatalf("error starting remotesrv Server listeners: %v\n", err)
	}
	go func() {
		server.Serve(listeners)
	}()
	waitForSignal()
	server.GracefulStop()

	return nil
}

func resolveIP(addr string) (string, error) {
	resolved, err := template.Parse(addr)
	if err != nil {
		return "", errors.Wrap(err, "parse addr")
	}

	host, _, err := net.SplitHostPort(resolved)
	if err != nil {
		return "", errors.Wrap(err, "split host port")
	}

	if host == "" {
		return "", errors.New("host can't be empty. Use e.g. 0.0.0.0 to listen on all addresses")
	}

	return resolved, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalln("error: " + err.Error())
	}
}

func waitForSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}
