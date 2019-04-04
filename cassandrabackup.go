// Copyright 2019 RetailNext, Inc.
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
	"cassandrabackup/backup"
	"cassandrabackup/bucket"
	"cassandrabackup/cache"
	"cassandrabackup/manifests"
	"cassandrabackup/periodic"
	"cassandrabackup/restore"
	"cassandrabackup/unixtime"
	"context"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh/terminal"
	"gopkg.in/alecthomas/kingpin.v2"
)

func setupLogger() func() {
	var logger *zap.Logger
	var err error
	if terminal.IsTerminal(int(os.Stdin.Fd())) {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)

	return func() {
		_ = logger.Sync()
	}
}

func setupInterruptContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case sig := <-c:
			zap.S().Infow("shutting_down", "signal", sig)
			cancel()
		case <-ctx.Done():
		}
	}()
	onExit := func() {
		signal.Stop(c)
		cancel()
	}
	return ctx, onExit
}

func setupPrometheus() {
	if metricsListenAddress == nil || *metricsListenAddress == "" {
		return
	}
	go func() {
		http.Handle(*metricsPath, promhttp.Handler())
		err := http.ListenAndServe(*metricsListenAddress, nil)
		zap.S().Fatalw("metrics_listen_error", "err", err)
	}()
}

func setupProfile() func() {
	if pprofFile == nil || *pprofFile == "" {
		return func() {
		}
	}
	f, err := os.Create(*pprofFile)
	if err != nil {
		panic(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	return func() {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			panic(err)
		}
	}
}

var (
	pprofFile = kingpin.Flag("pprof.cpu.file", "Enable cpu profiling to this file.").String()

	metricsListenAddress = kingpin.Flag("web.listen-address", "Address on which to expose metrics.").String()
	metricsPath          = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()

	snapshotCmd        = kingpin.Command("snapshot", "Make a snapshot backup")
	snapshotCmdCluster = snapshotCmd.Flag("backup.cluster", "Cluster name to back up as").Required().String()

	incrementalCmd                  = kingpin.Command("incremental", "Back up incremental sstables")
	incrementalCmdCluster           = incrementalCmd.Flag("backup.cluster", "Cluster name to back up as").Required().String()
	incrementalCmdCleanIfSuccessful = incrementalCmd.Flag("backup.clean", "Remove incremental backup files that have been backed up").Bool()

	runCmd                  = kingpin.Command("run", "Run as a daemon to back up periodically")
	runCmdCluster           = runCmd.Flag("backup.cluster", "Cluster name to back up as").Required().String()
	runCmdCleanIfSuccessful = runCmd.Flag("backup.clean", "Remove incremental backup files that have been backed up").Bool()

	restoreCmd                  = kingpin.Command("restore", "Restore from backup")
	restoreCmdDryRun            = restoreCmd.Flag("restore.dry-run", "Don't actually download files").Bool()
	restoreCmdAllowChangedFiles = restoreCmd.Flag("restore.allow-changed", "Allow restoration of files that changed between manifests").Bool()
	restoreCmdCluster           = restoreCmd.Flag("restore.cluster", "Cluster name to restore from").Required().String()
	restoreCmdHostname          = restoreCmd.Flag("restore.hostname", "Hostname to restore from").Required().String()
	restoreCmdNotBefore         = restoreCmd.Flag("restore.not-before", "Ignore manifests before this time (unix seconds)").Int64()
	restoreCmdNotAfter          = restoreCmd.Flag("restore.not-after", "Ignore manifests after this time (unix seconds)").Int64()

	manifestsCmd = kingpin.Command("manifests", "Manifest operations")

	listManifestsCmd          = manifestsCmd.Command("list", "List manifests for a host")
	listManifestsCmdCluster   = listManifestsCmd.Flag("restore.cluster", "Cluster name to restore from").Required().String()
	listManifestsCmdHostname  = listManifestsCmd.Flag("restore.hostname", "Hostname to restore from").Required().String()
	listManifestsCmdNotBefore = listManifestsCmd.Flag("restore.not-before", "Ignore manifests before this time (unix seconds)").Int64()
	listManifestsCmdNotAfter  = listManifestsCmd.Flag("restore.not-after", "Ignore manifests after this time (unix seconds)").Int64()
)

func main() {
	kingpin.UsageTemplate(kingpin.CompactUsageTemplate)
	cmd := kingpin.Parse()

	sync := setupLogger()
	defer sync()
	lgr := zap.S()

	ctx, onExit := setupInterruptContext()
	defer onExit()

	stopProfile := setupProfile()
	defer stopProfile()

	setupPrometheus()
	defer func() {
		if cache.Shared != nil {
			if err := cache.Shared.Close(); err != nil {
				lgr.Errorw("cache_close_err", "err", err)
			}
		}
	}()

	switch cmd {
	case "snapshot":
		err := backup.DoSnapshotBackup(ctx, *snapshotCmdCluster)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("backup_error", "err", err)
		}
	case "incremental":
		err := backup.DoIncremental(ctx, *incrementalCmdCleanIfSuccessful, *incrementalCmdCluster)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("backup_error", "err", err)
		}
	case "restore":
		err := restore.Main(ctx, *restoreCmdCluster, *restoreCmdHostname, *restoreCmdDryRun, *restoreCmdAllowChangedFiles, *restoreCmdNotBefore, *restoreCmdNotAfter)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("restore_error", "err", err)
		}
	case "run":
		err := periodic.Main(ctx, *runCmdCluster, *runCmdCleanIfSuccessful)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("backup_error", "err", err)
		}
	case "manifests list":
		lgr := zap.S()
		identity := manifests.NodeIdentity{
			Cluster:  *listManifestsCmdCluster,
			Hostname: *listManifestsCmdHostname,
		}
		bkt := bucket.NewClient()
		manifestKeys, err := bkt.ListManifests(ctx, identity, unixtime.Seconds(*listManifestsCmdNotBefore), unixtime.Seconds(*listManifestsCmdNotAfter))
		if err != nil {
			lgr.Fatalw("list_manifests_error", "err", err)
		}
		for _, mk := range manifestKeys {
			lgr.Infow("got_manifest", "manifest", mk)
		}
	}
}
