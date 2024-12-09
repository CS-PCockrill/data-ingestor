package config

import (
	"github.com/spf13/viper"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

type FileInformation struct {
	Path string
	Name string
}

type Config struct {
	DB DatabaseConfig `mapstructure:"DATABASE"`
	Runtime RuntimeConfig `mapstructure:"RUNTIME"`
	SupportedVcsConfig []string
}

type DatabaseConfig struct {
	DBDriver string `mapstructure:"DB_DRIVER"`
	DBUser   string `mapstructure:"DB_USER"`
	DBPassword string `mapstructure:"DB_PASSWORD"`
	DBHostname string `mapstructure:"DB_HOSTNAME"`
	DBPort 	   string `mapstructure:"DB_PORT"`
	DBName	   string `mapstructure:"DB_NAME"`
}

type RuntimeConfig struct {
	WorkerCount int `mapstructure:"WORKER_COUNT"`
}

// LoadConfig config file from given path
func LoadConfig(filename, path string) (*viper.Viper, error) {
	v := viper.New()
	v.AddConfigPath(path)
	v.SetConfigName(filename)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	return v, nil
}

// ParseConfig file from the given viper
func ParseConfig(v *viper.Viper) (*Config, error) {
	var c Config
	err := v.Unmarshal(&c)
	if err != nil {
		//logger.Log.Fatal("unable to decode into struct", zap.Error(err))
		return nil, err
	}
	return &c, nil
}

// GetLogConfigName gets the path from local or docker for logging configuration.
func GetLogConfigName() string {
	fileName := os.Getenv("LOG_CONFIG_NAME")
	if fileName != "" {
		return fileName
	}
	return "loggerConfig"
}

func GetLogConfigDirectory() string {
	filePath := os.Getenv("LOG_CONFIG_DIRECTORY")
	if filePath != "" {
		return filePath
	}
	return RootDir()
}

// GetLoggerConfig : will get the config for logging
func GetLoggerConfig() *Config {
	configFileName := GetLogConfigName()
	configFileDirectory := GetLogConfigDirectory()
	//logger.Log.Info("Config Details", zap.String("configFileDirectory", configFileDirectory), zap.String("configFileName", configFileName))

	cfgFile, configFileLoadError := LoadConfig(configFileName, configFileDirectory)
	if configFileLoadError != nil {
		//logger.Log.Fatal("unable to get config", zap.Error(configFileLoadError))

		panic(configFileLoadError.(any))
	}

	cfg, parseError := ParseConfig(cfgFile)
	if parseError != nil {
		//logger.Log.Fatal("unable to get config", zap.Error(parseError))
		panic(parseError.(any))
	}

	cfg.SupportedVcsConfig = supportedVcsConfig()
	return cfg
}

// GetConfigName get the path from local or docker
func GetConfigName() string {
	fileName := os.Getenv("CONFIG_NAME")
	if fileName != "" {
		return fileName
	}
	return "config"
}

func GetConfigDirectory() string {
	filePath := os.Getenv("CONFIG_DIRECTORY")
	if filePath != "" {
		return filePath
	}
	return RootDir()
}
func RootDir() string {
	_, b, _, _ := runtime.Caller(0)
	d := path.Join(path.Dir(b))
	return filepath.Dir(d)
}

// GetConfig : will get the config
func GetConfig() *Config {
	configFileName := GetConfigName()
	configFileDirectory := GetConfigDirectory()
	//logger.Log.Info("Config Details", zap.String("configFileDirectory", configFileDirectory), zap.String("configFileName", configFileName))

	cfgFile, configFileLoadError := LoadConfig(configFileName, configFileDirectory)
	if configFileLoadError != nil {
		//logger.Log.Fatal("unable to get config", zap.Error(configFileLoadError))

		panic(configFileLoadError.(any))
	}

	cfg, parseError := ParseConfig(cfgFile)
	if parseError != nil {
		//logger.Log.Fatal("unable to get config", zap.Error(parseError))
		panic(parseError.(any))
	}

	cfg.SupportedVcsConfig = supportedVcsConfig()
	return cfg
}

// SupportedVcsConfig add supported type from here.
func supportedVcsConfig() []string {
	return []string{"github"}
}