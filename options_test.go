package spmc

import "testing"

func TestOverflow_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy Overflow
		want   string
	}{
		{policy: OverflowBlock, want: "block"},
		{policy: OverflowDropNewest, want: "drop-newest"},
		{policy: OverflowDropOldest, want: "drop-oldest"},
		{policy: OverflowError, want: "error"},
		{policy: Overflow(9), want: "Overflow(9)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			if got := tt.policy.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
