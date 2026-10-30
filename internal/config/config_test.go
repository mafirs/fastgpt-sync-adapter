package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStreamlinedConfigLoad(t *testing.T) {
	tmpDir := t.TempDir()
	confPath := filepath.Join(tmpDir, "config.yaml")

	minimalYAML := `
server:
  addr: ":8080"

auth:
  enabled: true
  addr: ":8089"

sinks:
  nas:
    enabled: true
    url: "https://192.168.10.204:5001"
    username: "FastGPT"
    password: "Password"
    root_path: "/FastGPT-test"

connectors:
  wecom:
    enabled: true
    corp_id: "ww_test"
    secret: "sec_test"
    rsa_private_key_file: "./private.pem"
  feishu:
    enabled: true
    app_id: "cli_test"
    app_secret: "sec_test"

identity:
  source: bitable
  bitable:
    app_token: "app_tok"
    table_id: "tbl_id"
    app_id: "cli_test"
    app_secret: "sec_test"
`
	if err := os.WriteFile(confPath, []byte(minimalYAML), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(confPath)
	if err != nil {
		t.Fatalf("Load streamlined config failed: %v", err)
	}

	// 验证默认值自动填充
	if cfg.Engine.StateFile == "" || !filepath.IsAbs(cfg.Engine.StateFile) {
		t.Fatalf("StateFile 默认值未自动生成: %s", cfg.Engine.StateFile)
	}
	if cfg.Storage.DBFile == "" || !filepath.IsAbs(cfg.Storage.DBFile) {
		t.Fatalf("DBFile 默认值未自动生成: %s", cfg.Storage.DBFile)
	}
	if cfg.Connectors.WeCom.BatchLimit != 100 {
		t.Fatalf("WeCom BatchLimit 默认值不对: %d", cfg.Connectors.WeCom.BatchLimit)
	}
	if cfg.Connectors.Feishu.BatchLimit != 50 {
		t.Fatalf("Feishu BatchLimit 默认值不对: %d", cfg.Connectors.Feishu.BatchLimit)
	}
	if cfg.Connectors.Feishu.Mode != "both" {
		t.Fatalf("Feishu Mode 默认值不对: %s", cfg.Connectors.Feishu.Mode)
	}
	if cfg.Sinks.NAS.MaxFileSizeMB != 0 && cfg.Sinks.NAS.MaxFileSizeMB != 200 {
		t.Fatalf("NAS MaxFileSizeMB 异常: %d", cfg.Sinks.NAS.MaxFileSizeMB)
	}
	if len(cfg.Classifier.Rules) == 0 {
		t.Fatalf("Classifier 规则默认值未填充")
	}
}

// baseYAML 一份能通过校验的最小配置；具体用例在 server 节里追加字段。
const baseYAML = `
auth:
  enabled: true
  addr: ":8089"

sinks:
  nas:
    enabled: true
    url: "https://192.168.10.204:5001"
    username: "FastGPT"
    password: "Password"
    root_path: "/FastGPT-test"

connectors:
  feishu:
    enabled: true
    app_id: "cli_test"
    app_secret: "sec_test"
    mode: both
`

// loadWith 把 server 节片段拼进基础配置后 Load。
func loadWith(t *testing.T, serverExtra string) (*Config, error) {
	t.Helper()
	confPath := filepath.Join(t.TempDir(), "config.yaml")
	yaml := baseYAML
	if serverExtra != "" {
		yaml += "\nserver:\n" + serverExtra
	}
	if err := os.WriteFile(confPath, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	return Load(confPath)
}

func TestServerDefaults(t *testing.T) {
	cfg, err := loadWith(t, `
  addr: ":8080"
`)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.AdminToken != "" {
		t.Fatalf("admin_token 默认应为空（关闭管理员通道），实际 %q", cfg.Server.AdminToken)
	}
	if len(cfg.Server.AdminAllow) != 0 {
		t.Fatalf("admin_allow 默认应为空，实际 %v", cfg.Server.AdminAllow)
	}
	if cfg.Server.SignSecret != "" {
		t.Fatalf("sign_secret 默认应为空（启动时随机生成），实际 %q", cfg.Server.SignSecret)
	}
	if got := cfg.Server.LinkTTL.D(); got != 10*time.Minute {
		t.Fatalf("link_ttl 默认应为 10m，实际 %s", got)
	}
	if got := cfg.Server.TokenGrace.D(); got != 24*time.Hour {
		t.Fatalf("token_grace 默认应为 24h，实际 %s", got)
	}
	if cfg.Server.PendingFile == "" || !filepath.IsAbs(cfg.Server.PendingFile) {
		t.Fatalf("pending_file 默认应填且转为绝对路径，实际 %q", cfg.Server.PendingFile)
	}
	if filepath.Base(cfg.Server.PendingFile) != "pending-tokens.csv" {
		t.Fatalf("pending_file 默认文件名不对: %s", cfg.Server.PendingFile)
	}
}

func TestServerCustomValues(t *testing.T) {
	cfg, err := loadWith(t, `
  admin_token: "12345678901234567890123456789012"
  admin_allow: ["10.0.0.0/8", " 192.168.1.0/24 "]
  sign_secret: "abcdefghijklmnopqrstuvwxyz0123456789"
  link_ttl: 5m
  token_grace: 48h
  pending_file: "./out/pending.csv"
`)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.LinkTTL.D() != 5*time.Minute || cfg.Server.TokenGrace.D() != 48*time.Hour {
		t.Fatalf("时长未生效: %s / %s", cfg.Server.LinkTTL.D(), cfg.Server.TokenGrace.D())
	}
	// 空白应被裁掉，便于后续 net.ParseCIDR
	if cfg.Server.AdminAllow[1] != "192.168.1.0/24" {
		t.Fatalf("admin_allow 未裁剪空白: %q", cfg.Server.AdminAllow[1])
	}
	if filepath.Base(cfg.Server.PendingFile) != "pending.csv" {
		t.Fatalf("pending_file 未生效: %s", cfg.Server.PendingFile)
	}
}

func TestLegacyServerTokenIgnored(t *testing.T) {
	// 旧配置里的 server.token 只告警不报错。
	cfg, err := loadWith(t, `
  token: "dev-token"
`)
	if err != nil {
		t.Fatalf("旧 server.token 不应导致 Load 失败: %v", err)
	}
	if cfg.Server.AdminToken != "" {
		t.Fatalf("server.token 不应被当作管理员 token: %q", cfg.Server.AdminToken)
	}
}

func TestServerValidateSecretLength(t *testing.T) {
	// 31 字节 -> 报错；32 字节 -> 通过
	if _, err := loadWith(t, `
  admin_token: "1234567890123456789012345678901"
`); err == nil {
		t.Fatal("admin_token 长度不足 32 字节应报错")
	}
	if _, err := loadWith(t, `
  sign_secret: "1234567890123456789012345678901"
`); err == nil {
		t.Fatal("sign_secret 长度不足 32 字节应报错")
	}
	if _, err := loadWith(t, `
  admin_token: "12345678901234567890123456789012"
  sign_secret: "12345678901234567890123456789012"
`); err != nil {
		t.Fatalf("32 字节密钥应通过: %v", err)
	}
}

func TestServerValidateAdminAllowCIDR(t *testing.T) {
	cases := []struct {
		name    string
		allow   string
		wantErr bool
	}{
		{"合法 CIDR", `["10.0.0.0/8"]`, false},
		{"多个合法 CIDR", `["10.0.0.0/8", "::1/128"]`, false},
		{"裸 IP 不合法", `["10.0.0.1"]`, true},
		{"乱写", `["not-a-cidr"]`, true},
		{"空项", `[""]`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := loadWith(t, "\n  admin_allow: "+c.allow+"\n")
			if c.wantErr && err == nil {
				t.Fatalf("admin_allow=%s 应报错", c.allow)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("admin_allow=%s 不应报错: %v", c.allow, err)
			}
		})
	}
}

// 示例配置里的共享文件夹搜索配置能读出来：周期和每轮人数都生效。
func TestSharedDiscoverConfigInExample(t *testing.T) {
	cfg, err := Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	fc := cfg.Connectors.Feishu
	if fc.SharedDiscover.D() != time.Hour || fc.SharedFullInterval.D() != 24*time.Hour || fc.SharedPerRound != 10 {
		t.Fatalf("配置不符: %v %v %d", fc.SharedDiscover.D(), fc.SharedFullInterval.D(), fc.SharedPerRound)
	}
}
