package impl

import (
	"testing"
)

func TestActiveUsersReport_ReportTitle(t *testing.T) {

	tests := []struct {
		name string
		want string
	}{
		{
			name: "Test ActiveUsersReport_ReportTitle",
			want: "active_users_20220102",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := ActiveUsersReport{}
			if got := a.ReportTitle(); got != tt.want {
				t.Errorf("ReportTitle() = %v, want %v", got, tt.want)
			}
		})
	}
}
