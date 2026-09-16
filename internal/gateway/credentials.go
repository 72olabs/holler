// Package gateway serves the opt-in, loopback-only human conversation surface.
// It is an application boundary, not isolation from arbitrary same-OS-user code.
package gateway

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type credentials struct {
	Human  string `json:"human"`
	Scope  string `json:"scope"`
	Bearer string `json:"bearer"`
}

func randomSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func loadCredentials(path, human, scope string) (credentials, error) {
	var c credentials
	if !strings.HasPrefix(human, "human:") || human == "human:" || strings.TrimSpace(human) != human || (scope != "observe" && scope != "observe+admin") {
		return c, errors.New("gateway requires canonical human: identity and observe or observe+admin scope")
	}
	if path == "" || !filepath.IsAbs(path) {
		return c, errors.New("gateway credential path must be absolute")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return c, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return c, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return c, errors.New("gateway credential directory must be private (0700), not a symlink")
	}
	info, err = os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		c = credentials{Human: human, Scope: scope}
		c.Bearer, err = randomSecret()
		if err != nil {
			return c, err
		}
		raw, _ := json.Marshal(c)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return c, err
		}
		_, writeErr := f.Write(raw)
		syncErr := f.Sync()
		closeErr := f.Close()
		return c, errors.Join(writeErr, syncErr, closeErr)
	}
	if err != nil {
		return c, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
		return c, errors.New("gateway credential file must be a small private regular file (0600)")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, errors.New("invalid gateway credential file")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(c.Bearer)
	if err != nil || len(decoded) != 32 {
		return c, errors.New("invalid gateway credential length")
	}
	if c.Human != human || c.Scope != scope {
		return c, errors.New("gateway identity/scope differs from persisted enrollment; explicit re-enrollment required")
	}
	return c, nil
}

func replaceCredentials(path string, c credentials) error {
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".gateway-rotation-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync credential directory: %w", err)
	}
	return nil
}
