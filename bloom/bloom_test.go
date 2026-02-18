package bloom

import (
	"fmt"
	"testing"
)

func TestAddAndMayContain(t *testing.T) {
	f := New(100)

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		f.Add([]byte(k))
	}

	// All inserted keys must be found.
	for _, k := range keys {
		if !f.MayContain([]byte(k)) {
			t.Fatalf("expected MayContain(%q) = true", k)
		}
	}

	// A key never inserted should (very likely) not be found.
	// With 100-item filter and 5 items the false positive rate is extremely low.
	if f.MayContain([]byte("zzz-never-inserted")) {
		t.Log("unexpected false positive for 'zzz-never-inserted' (possible but unlikely)")
	}
}

func TestEmptyFilter(t *testing.T) {
	f := New(10)
	if f.MayContain([]byte("anything")) {
		t.Fatal("empty filter should not contain anything")
	}
}

func TestFalsePositiveRate(t *testing.T) {
	n := 10000
	f := NewWithFPRate(n, DefaultFPRate)

	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	// Test with keys that were never inserted.
	falsePositives := 0
	trials := 100000
	for i := 0; i < trials; i++ {
		probe := []byte(fmt.Sprintf("probe-%d", i))
		if f.MayContain(probe) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(trials)
	// Allow up to 3x the target rate to account for randomness.
	maxAcceptable := DefaultFPRate * 3
	if fpRate > maxAcceptable {
		t.Fatalf("false positive rate %.4f exceeds threshold %.4f", fpRate, maxAcceptable)
	}
	t.Logf("false positive rate: %.4f (target %.4f)", fpRate, DefaultFPRate)
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	f := New(500)
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("item-%05d", i)
		f.Add([]byte(keys[i]))
	}

	encoded := f.Encode()
	if encoded == "" {
		t.Fatal("encoded string should not be empty")
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Decoded filter must agree with original on all inserted keys.
	for _, k := range keys {
		if !decoded.MayContain([]byte(k)) {
			t.Fatalf("decoded filter missing key %q", k)
		}
	}

	// Internal fields should match.
	if decoded.numBits != f.numBits {
		t.Fatalf("numBits mismatch: %d vs %d", decoded.numBits, f.numBits)
	}
	if decoded.numHash != f.numHash {
		t.Fatalf("numHash mismatch: %d vs %d", decoded.numHash, f.numHash)
	}
	if len(decoded.bits) != len(f.bits) {
		t.Fatalf("bits length mismatch: %d vs %d", len(decoded.bits), len(f.bits))
	}
}

func TestDecodeInvalidBase64(t *testing.T) {
	_, err := Decode("not-valid-base64!!!")
	if err == nil {
		t.Fatal("expected error decoding invalid base64")
	}
}

func TestEstimateCount(t *testing.T) {
	n := 1000
	f := New(n)
	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("k%d", i)))
	}

	est := f.EstimateCount()
	// The estimate should be in the right ballpark (within 30%).
	low := int(float64(n) * 0.7)
	high := int(float64(n) * 1.3)
	if est < low || est > high {
		t.Fatalf("EstimateCount = %d, expected between %d and %d", est, low, high)
	}
	t.Logf("EstimateCount = %d (actual %d)", est, n)
}

func TestNewWithFPRateSmallN(t *testing.T) {
	// n <= 0 should not panic.
	f := NewWithFPRate(0, 0.01)
	f.Add([]byte("x"))
	if !f.MayContain([]byte("x")) {
		t.Fatal("filter should contain 'x'")
	}

	f2 := NewWithFPRate(-5, 0.01)
	f2.Add([]byte("y"))
	if !f2.MayContain([]byte("y")) {
		t.Fatal("filter should contain 'y'")
	}
}
