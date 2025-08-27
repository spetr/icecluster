package config

import (
	"bytes"
	"flag"
	"os"
	"regexp"
	"time"

	yaml "gopkg.in/yaml.v3"
)

type Config struct {
	NodeID              string        `yaml:"node_id"`
	BindAddr            string        `yaml:"bind"`
	Join                string        `yaml:"join"`
	DataDir             string        `yaml:"data"`
	Backing             string        `yaml:"backing"`
	Mount               string        `yaml:"mount"`
	Keepalive           time.Duration `yaml:"keepalive"`
	ClockSkewMax        time.Duration `yaml:"clock_skew_max"`
	SyncMode            string        `yaml:"sync_mode"`
	KeepaliveFailures   int           `yaml:"keepalive_failures"`
	ConsistencyInterval time.Duration `yaml:"consistency_interval"`
	ConsistencyMode     string        `yaml:"consistency_mode"`
	LockTimeout         time.Duration `yaml:"lock_timeout"`
	LockRetry           time.Duration `yaml:"lock_retry"`
	LogFile             string        `yaml:"log_file"`
	LogVerbose          bool          `yaml:"log_verbose"`
	ReadyRequireSync    bool          `yaml:"ready_require_sync"`
	ReadyRequirePeer    bool          `yaml:"ready_require_peer"`
	APIToken            string        `yaml:"api_token"`
	TLSCert             string        `yaml:"tls_cert"`
	TLSKey              string        `yaml:"tls_key"`
	// Hooks: directory with Lua scripts and default timeout for execution
	HooksDir     string        `yaml:"hooks_dir"`
	HooksTimeout time.Duration `yaml:"hooks_timeout"`
	// HooksLogFile: when set, Lua hooks log to this separate file; otherwise default logging is used
	HooksLogFile string `yaml:"hooks_log_file"`
	// SourcePath is the path of the loaded YAML file (not serialized)
	SourcePath string `yaml:"-"`
}

// FromFlags now only parses -config path; other settings come from YAML.
func FromFlags() *Config {
	cfgPath := flag.String("config", "/opt/icewarp/icecluster/config.yml", "path to YAML configuration file")
	flag.Parse()
	if *cfgPath == "" {
		// return defaults if no config provided
		return defaultConfig()
	}
	cfg, err := FromFile(*cfgPath)
	if err != nil {
		panic(err)
	}
	cfg.SourcePath = *cfgPath
	return cfg
}

func defaultConfig() *Config {
	host, _ := os.Hostname()
	if host == "" {
		host = "node"
	}
	return &Config{
		NodeID:              host,
		BindAddr:            ":9000",
		Join:                "",
		DataDir:             "/var/lib/icecluster",
		Backing:             "/opt/icewarp/config-local",
		Mount:               "/opt/icewarp/config",
		Keepalive:           time.Minute,
		ClockSkewMax:        time.Second,
		SyncMode:            "latest",
		KeepaliveFailures:   1,
		ConsistencyInterval: 0,
		ConsistencyMode:     "report",
		LockTimeout:         5 * time.Second,
		LockRetry:           300 * time.Millisecond,
		LogFile:             "",
		LogVerbose:          false,
		ReadyRequireSync:    true,
		ReadyRequirePeer:    false,
		APIToken:            "",
		TLSCert:             "",
		TLSKey:              "",
		HooksDir:            "",
		HooksTimeout:        2 * time.Second,
		HooksLogFile:        "",
	}
}

// FromFile reads YAML config into Config struct, applying defaults for missing fields.
func FromFile(path string) (*Config, error) {
	cfg := defaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		// Fallback: if parsing failed and consistency_interval: 0 was provided in a form
		// that yaml cannot coerce to time.Duration, rewrite it to "0s" and retry.
		re := regexp.MustCompile(`(?m)^(\s*consistency_interval\s*:\s*)("?0"?)\s*$`)
		data2 := re.ReplaceAll(data, []byte("${1}\"0s\""))
		if !bytes.Equal(data2, data) {
			if err2 := yaml.Unmarshal(data2, cfg); err2 == nil {
				goto post
			}
		}
		return nil, err
	}
post:
	// Ensure node id fallback to hostname when empty in YAML
	if cfg.NodeID == "" {
		if h, _ := os.Hostname(); h != "" {
			cfg.NodeID = h
		} else {
			cfg.NodeID = "node"
		}
	}
	cfg.SourcePath = path
	return cfg, nil
}
