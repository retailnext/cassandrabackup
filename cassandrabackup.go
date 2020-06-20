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
	"context"
	"os"
	"os/signal"
	"runtime/pprof"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/retailnext/cassandrabackup/backup"
	"github.com/retailnext/cassandrabackup/bucket"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/cache"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/metrics"
	"github.com/retailnext/cassandrabackup/periodic"
	"github.com/retailnext/cassandrabackup/restore"
	"github.com/retailnext/cassandrabackup/unixtime"
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

	listCmd = kingpin.Command("list", "")

	listManifestsCmd          = listCmd.Command("manifests", "List manifests for a host")
	listManifestsCmdCluster   = listManifestsCmd.Flag("cluster", "Cluster name to restore from").Required().String()
	listManifestsCmdHostname  = listManifestsCmd.Flag("hostname", "Hostname to restore from").Required().String()
	listManifestsCmdNotBefore = listManifestsCmd.Flag("not-before", "Ignore manifests before this time (unix seconds)").Int64()
	listManifestsCmdNotAfter  = listManifestsCmd.Flag("not-after", "Ignore manifests after this time (unix seconds)").Int64()

	listHostsCmd        = listCmd.Command("hosts", "List hosts in a cluster")
	listHostsCmdCluster = listHostsCmd.Flag("cluster", "Cluster name").Required().String()

	_ = listCmd.Command("clusters", "List clusters")

	bucketName      = kingpin.Flag("bucket", "S3 or Google Cloud Storage bucket name.").Required().String()
	bucketKeyPrefix = kingpin.Flag("key-prefix", "Set the prefix for files in the bucket").Default("/").String()
	s3BucketRegion  = kingpin.Flag("s3-region", "S3 bucket region.").String()
	s3StorageClass  = kingpin.Flag("s3-storage-class", "Set the storage class for files in S3").Default(s3.StorageClassStandardIa).String()
	provider        = kingpin.Flag("cloud-provider", "Cloud provider. ["+config.ProviderAWS+" or "+config.ProviderGoogle+"]").Default(config.ProviderAWS).String()

	sharedCacheFile = kingpin.Flag("cache-file", "Location of local cache file.").Required().String()
)

func parseOptions() (string, *config.Config) {
	kingpin.UsageTemplate(kingpin.CompactUsageTemplate)
	cmd := kingpin.Parse()

	if *provider == config.ProviderAWS && *s3BucketRegion == "" {
		*s3BucketRegion = os.Getenv("AWS_REGION")
		if *s3BucketRegion == "" {
			kingpin.Fatalf("required flag --%s not provided", "s3-region")
		}
	}

	config := &config.Config{
		Provider:        *provider,
		BucketName:      *bucketName,
		BucketKeyPrefix: *bucketKeyPrefix,
		S3BucketRegion:  *s3BucketRegion,
		S3StorageClass:  *s3StorageClass,
		SharedCacheFile: *sharedCacheFile,
	}

	return cmd, config
}

func main() {
	cmd, config := parseOptions()

	sync := setupLogger()
	defer sync()
	lgr := zap.S()

	ctx, onExit := setupInterruptContext()
	defer onExit()

	stopProfile := setupProfile()
	defer stopProfile()

	metrics.SetupPrometheus(metricsListenAddress, metricsPath)

	defer func() {
		if cache.Shared != nil {
			if err := cache.Shared.Close(); err != nil {
				lgr.Errorw("cache_close_err", "err", err)
			}
		}
	}()

	switch cmd {
	case "backup snapshot":
		err := backup.DoSnapshotBackup(ctx, config)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("backup_error", "err", err)
		}
	case "backup incremental":
		err := backup.DoIncremental(ctx, config)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("backup_error", "err", err)
		}
	case "backup run":
		err := periodic.Main(ctx, config)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("backup_error", "err", err)
		}
	case "restore host":
		err := restore.RestoreHost(ctx, config)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("restore_error", "err", err)
		}
	case "restore cluster":
		err := restore.RestoreCluster(ctx, config)
		if err == context.Canceled {
			return
		}
		if err != nil {
			lgr.Fatalw("restore_error", "err", err)
		}
	case "list manifests":
		lgr := zap.S()
		identity := manifests.NodeIdentity{
			Cluster:  *listManifestsCmdCluster,
			Hostname: *listManifestsCmdHostname,
		}
		bkt := bucket.OpenShared(config)
		manifestKeys, err := bkt.ListManifests(ctx, identity, unixtime.Seconds(*listManifestsCmdNotBefore), unixtime.Seconds(*listManifestsCmdNotAfter))
		if err != nil {
			lgr.Fatalw("list_manifests_error", "err", err)
		}
		for _, mk := range manifestKeys {
			lgr.Infow("got_manifest", "manifest", mk)
		}
	case "list hosts":
		lgr := zap.S()
		bkt := bucket.OpenShared(config)
		results, err := bkt.ListHostNames(ctx, *listHostsCmdCluster)
		if err != nil {
			lgr.Fatalw("list_hosts_error", "err", err)
		}
		for _, ni := range results {
			lgr.Infow("got_host", "identity", ni)
		}
	case "list clusters":
		lgr := zap.S()
		bkt := bucket.OpenShared(config)
		results, err := bkt.ListClusters(ctx)
		if err != nil {
			lgr.Fatalw("list_clusters_error", "err", err)
		}
		for _, cluster := range results {
			lgr.Infow("got_cluster", "cluster", cluster)
		}
	default:
		lgr.Fatalw("unhandled_command", "cmd", cmd)
	}
}
