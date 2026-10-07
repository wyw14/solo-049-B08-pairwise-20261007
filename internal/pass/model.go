package pass

import (
	"fmt"
	"time"
)

type Phase string

const (
	Planned      Phase = "planned"
	Acquiring    Phase = "acquiring"
	Tracking     Phase = "tracking"
	Draining     Phase = "draining"
	Sealed       Phase = "sealed"
	Completed    Phase = "completed"
	Failed       Phase = "failed"
	Canceled     Phase = "canceled"
	ReplanReview Phase = "replan_review"
)

type Window struct {
	AOS      time.Time `json:"aos"`
	LOS      time.Time `json:"los"`
	Revision int64     `json:"revision"`
}

func (w Window) Validate() error {
	if w.Revision < 1 || w.AOS.IsZero() || !w.LOS.After(w.AOS) {
		return fmt.Errorf("pass window needs one revision and increasing boundaries")
	}
	return nil
}

type Pass struct {
	ID                     string    `json:"id"`
	SatelliteID            string    `json:"satellite_id"`
	AntennaID              string    `json:"antenna_id"`
	RFChains               []string  `json:"rf_chains"`
	RequiredReceiverChains []string  `json:"required_receiver_chains"`
	Window                 Window    `json:"window"`
	Phase                  Phase     `json:"phase"`
	ReservationID          string    `json:"reservation_id,omitempty"`
	ReservationGeneration  int64     `json:"reservation_generation,omitempty"`
	CurrentAttemptID       string    `json:"current_attempt_id,omitempty"`
	DecoderEpoch           int64     `json:"decoder_epoch,omitempty"`
	RecordingSegmentID     string    `json:"recording_segment_id,omitempty"`
	OwnerStation           string    `json:"owner_station"`
	OwnerGeneration        int64     `json:"owner_generation"`
	FailureReason          string    `json:"failure_reason,omitempty"`
	ReplanRevision         int64     `json:"replan_revision,omitempty"`
	ReplanReason           string    `json:"replan_reason,omitempty"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}

func (p Pass) Validate() error {
	if p.ID == "" || p.SatelliteID == "" || p.AntennaID == "" {
		return fmt.Errorf("pass, satellite and antenna identity are required")
	}
	if err := p.Window.Validate(); err != nil {
		return err
	}
	if len(p.RFChains) == 0 || len(p.RequiredReceiverChains) == 0 {
		return fmt.Errorf("pass requires RF and receiver chains")
	}
	if p.OwnerStation == "" || p.OwnerGeneration < 1 {
		return fmt.Errorf("pass recorder owner is required")
	}
	return nil
}

type CreateRequest struct {
	SatelliteID            string
	AntennaID              string
	RFChains               []string
	RequiredReceiverChains []string
	AOS                    time.Time
	LOS                    time.Time
	Revision               int64
	OwnerStation           string
}
