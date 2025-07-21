package config

type BigTableConfig struct {
	Project      string `mapstructure:"project" validate:"required"`
	Instance     string `mapstructure:"instance" validate:"required"`
	Emulated     bool   `mapstructure:"emulated,omitempty"`
	EmulatorHost string `mapstructure:"emulatorhost,omitempty" validate:"required_if=Emulated true,omitempty,hostname|ip"`
	EmulatorPort uint16 `mapstructure:"emulatorport,omitempty" validate:"required_if=Emulated true,omitempty,gte=1,lte=65535"`
}

type SqlDatabaseConfig struct {
	Host         string `mapstructure:"host" validate:"required,hostname"`
	Port         uint16 `mapstructure:"port" validate:"required,gte=1,lte=65535"`
	DbName       string `mapstructure:"db" validate:"required"`
	Username     string `mapstructure:"user" validate:"required"`
	Password     string `mapstructure:"password" validate:"required"`
	MaxOpenConns int    `mapstructure:"maxopenconns" validate:"gte=1"`
	MaxIdleConns int    `mapstructure:"maxidleconns" validate:"gte=1"`
	Ssl          bool   `mapstructure:"ssl,omitempty"`
}

type CacheConfig struct {
	Endpoint string `mapstructure:"endpoint" validate:"required,hostname_port"`
}

type ExecutionClientConfig struct {
	Client   string `mapstructure:"client" validate:"required,oneof=besu"`
	Endpoint string `mapstructure:"endpoint" validate:"required,url"`
}

type ConsensusClientConfig struct {
	Client   string `mapstructure:"client" validate:"required,oneof=teku"`
	Endpoint string `mapstructure:"endpoint" validate:"required,url"`
	Mode     string `mapstructure:"mode" validate:"required,oneof=archive pruned"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled,omitempty"`
	Address string `mapstructure:"address,omitempty" validate:"required_if=Enabled true,omitempty,hostname_port"`
}

type PprofConfig struct {
	Enabled bool   `mapstructure:"enabled,omitempty"`
	Address string `mapstructure:"address,omitempty" validate:"required_if=Enabled true,omitempty,hostname|ip"`
	Port    uint16 `mapstructure:"port,omitempty" validate:"required_if=Enabled true,omitempty,gte=1,lte=65535"`
}