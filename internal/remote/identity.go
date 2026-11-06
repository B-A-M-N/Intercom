// Host identity: per-host ed25519 keypair, self-signed TLS certificate
// with SAN = hostId, fingerprint = sha256(pubkey). Enrollment pins the
// fingerprint; this gives authenticated encryption + host identity
// without a CA and without plaintext bearer tokens.
package remote

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// Identity is a host's long-term identity.
type Identity struct {
	HostID      string
	Private     ed25519.PrivateKey
	Public      ed25519.PublicKey
	Fingerprint string
}

// FingerprintOf returns the hex fingerprint of a public key.
func FingerprintOf(pub ed25519.PublicKey) string {
	h := sha256.Sum256([]byte(pub))
	return hex.EncodeToString(h[:])
}

// LoadOrCreateIdentity loads or creates the host identity in dir.
func LoadOrCreateIdentity(dir, hostID string) (*Identity, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	keyPath := filepath.Join(dir, "host_identity.key")
	if hostID == "" {
		return nil, errors.New("remote: hostId required for identity")
	}
	if data, err := os.ReadFile(keyPath); err == nil {
		block, _ := pem.Decode(data)
		if block == nil || block.Type != "ED25519 PRIVATE KEY" {
			return nil, errors.New("remote: bad identity key file")
		}
		priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		edPriv, ok := priv.(ed25519.PrivateKey)
		if !ok {
			return nil, errors.New("remote: identity key is not ed25519")
		}
		pub, ok := edPriv.Public().(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("remote: bad identity public key")
		}
		return &Identity{HostID: hostID, Private: edPriv, Public: pub, Fingerprint: FingerprintOf(pub)}, nil
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "ED25519 PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(keyPath, pemBytes, 0o600); err != nil {
		return nil, err
	}
	return &Identity{HostID: hostID, Private: priv, Public: pub, Fingerprint: FingerprintOf(pub)}, nil
}

// GenerateIdentity creates an ephemeral identity (tests).
func GenerateIdentity(hostID string) (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{HostID: hostID, Private: priv, Public: pub, Fingerprint: FingerprintOf(pub)}, nil
}

// serverTLSConfig builds a self-signed cert for hostID.
func serverTLSConfig(id *Identity) (*tls.Config, error) {
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: id.HostID},
		DNSNames:     []string{id.HostID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, id.Public, id.Private)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: id.Private}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// clientTLSConfig verifies the peer cert by pinned pubkey fingerprint
// and SAN == expected hostId, and presents our own identity cert.
func clientTLSConfig(expectedHostID, expectedFingerprint string, client *Identity) *tls.Config {
	var certs []tls.Certificate
	if client != nil {
		if cfg, err := serverTLSConfig(client); err == nil {
			certs = cfg.Certificates
		}
	}
	return &tls.Config{
		ServerName: expectedHostID,
		// Chain verification is replaced by pubkey pinning in
		// VerifyPeerCertificate (self-signed host certs, no CA).
		InsecureSkipVerify: true,
		Certificates: certs,
		MinVersion: tls.VersionTLS12,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("remote: no peer certificate")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			// Validity window is enforced explicitly in the custom
			// verification path (certs are self-signed, no CA).
			now := time.Now()
			if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
				return fmt.Errorf("remote: peer certificate not valid at %s (valid %s..%s)", now.Format(time.RFC3339), cert.NotBefore.Format(time.RFC3339), cert.NotAfter.Format(time.RFC3339))
			}
			pub, ok := cert.PublicKey.(ed25519.PublicKey)
			if !ok {
				return errors.New("remote: peer key is not ed25519")
			}
			if FingerprintOf(pub) != expectedFingerprint {
				return fmt.Errorf("remote: peer fingerprint mismatch for host %q", expectedHostID)
			}
			if err := cert.VerifyHostname(expectedHostID); err != nil {
				return fmt.Errorf("remote: peer host identity mismatch: %w", err)
			}
			return nil
		},
		VerifyConnection: nil,
	}
}

// fingerprintFromCert extracts the ed25519 fingerprint from a cert.
func fingerprintFromCert(cert *x509.Certificate) string {
	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return ""
	}
	return FingerprintOf(pub)
}

// IsLoopback reports whether host (without port) is loopback.
func IsLoopback(host string) bool {
	return host == "127.0.0.1" || host == "::1" || host == "localhost"
}
