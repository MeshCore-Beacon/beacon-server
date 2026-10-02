// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func TestUpsertNode_WithRadio(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	freq := float32(915.0)
	sf := int16(7)
	bw := float32(125.0)

	mock.EXPECT().
		UpsertNode(gomock.Any(), gomock.Any()).
		Return(sqlc.Node{ID: nodeID}, nil)

	store := &Store{q: mock}
	id, err := store.UpsertNode(context.Background(), ingest.UpsertNodeParams{
		PublicKey: []byte{0x01},
		NodeType:  1,
		Name:      "test-node",
	}, ingest.RadioSettings{FreqMHz: freq, SF: sf, BWKHz: bw})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != nodeID {
		t.Errorf("expected ID %s, got %s", nodeID, id)
	}
}

func TestUpsertNode_ComputesClockDrift(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	deviceTime := uint32(time.Now().Add(-10 * time.Minute).Unix())

	var captured sqlc.UpsertNodeParams
	mock.EXPECT().
		UpsertNode(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, p sqlc.UpsertNodeParams) (sqlc.Node, error) {
			captured = p
			return sqlc.Node{ID: nodeID}, nil
		})

	store := &Store{q: mock}
	_, err := store.UpsertNode(context.Background(), ingest.UpsertNodeParams{
		PublicKey:       []byte{0x01},
		NodeType:        2, // repeater
		Name:            "test-repeater",
		AdvertTimestamp: deviceTime,
	}, ingest.RadioSettings{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.DeviceClockDriftSeconds == nil {
		t.Fatal("expected DeviceClockDriftSeconds to be set")
	}
	// device clock is ~10 minutes (600s) behind; allow a few seconds of test-runtime slop
	got := *captured.DeviceClockDriftSeconds
	if got > -595 || got < -605 {
		t.Errorf("expected drift near -600s, got %d", got)
	}
}

func TestUpsertNode_NoAdvertTimestamp_OmitsDrift(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	var captured sqlc.UpsertNodeParams
	mock.EXPECT().
		UpsertNode(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, p sqlc.UpsertNodeParams) (sqlc.Node, error) {
			captured = p
			return sqlc.Node{ID: nodeID}, nil
		})

	store := &Store{q: mock}
	_, err := store.UpsertNode(context.Background(), ingest.UpsertNodeParams{
		PublicKey: []byte{0x01},
		NodeType:  2, // repeater, but AdvertTimestamp left zero (e.g. decode failed upstream)
		Name:      "test-repeater",
	}, ingest.RadioSettings{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.DeviceClockDriftSeconds != nil {
		t.Error("expected DeviceClockDriftSeconds to remain nil when AdvertTimestamp is zero")
	}
}

func TestUpsertNode_WithoutRadio(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		UpsertNode(gomock.Any(), gomock.Any()).
		Return(sqlc.Node{ID: nodeID}, nil)

	store := &Store{q: mock}
	id, err := store.UpsertNode(context.Background(), ingest.UpsertNodeParams{
		PublicKey: []byte{0x01},
		NodeType:  1,
		Name:      "test-node",
	}, ingest.RadioSettings{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != nodeID {
		t.Errorf("expected ID %s, got %s", nodeID, id)
	}
}

func TestSetNodeCapability_BothTrue(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().SetNodeMultibytePaths(gomock.Any(), nodeID).Return(nil)
	mock.EXPECT().SetNodeMultibyteTraces(gomock.Any(), nodeID).Return(nil)

	store := &Store{q: mock}
	err := store.SetNodeCapability(context.Background(), nodeID, true, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetNodeCapability_PathsOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().SetNodeMultibytePaths(gomock.Any(), nodeID).Return(nil)

	store := &Store{q: mock}
	err := store.SetNodeCapability(context.Background(), nodeID, true, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetNodeCapability_NeitherSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	// no EXPECT — neither sqlc method should be called
	store := &Store{q: mock}
	err := store.SetNodeCapability(context.Background(), nodeID, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListNodes_Pagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	lastSeen := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	rows := make([]sqlc.ListNodesRow, 3)
	for i := range rows {
		rows[i] = sqlc.ListNodesRow{
			ID:        nodeID,
			PublicKey: []byte{0x01},
			LastSeen:  lastSeen,
		}
	}

	mock.EXPECT().
		ListNodes(gomock.Any(), gomock.Any()).
		Return(rows, nil)

	store := &Store{q: mock}
	page, err := store.ListNodes(context.Background(), 0, []string{"YVR"}, nil, nil, nil, "", "", "", 0, 2, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(page.Items))
	}
	if !page.HasMore {
		t.Error("expected HasMore true")
	}
	if page.NextCursor == nil {
		t.Error("expected NextCursor to be set")
	}
}

func TestListNodes_IATAsUnmarshal(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	iatasJSON := []byte(`[{"iata":"YVR","last_seen":1700000000000}]`)

	mock.EXPECT().
		ListNodes(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListNodesRow{
			{
				ID:        nodeID,
				PublicKey: []byte{0x01},
				Iatas:     iatasJSON,
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListNodes(context.Background(), 0, nil, nil, nil, nil, "", "", "", 0, 10, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items[0].IATAs) != 1 {
		t.Errorf("expected 1 IATA, got %d", len(page.Items[0].IATAs))
	}
	if page.Items[0].IATAs[0].IATA != "YVR" {
		t.Errorf("expected IATA YVR, got %s", page.Items[0].IATAs[0].IATA)
	}
}

func TestListNodes_Stale(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	staleID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	freshID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	unmeasuredID := uuid.MustParse("00000000-0000-0000-0000-000000000003")

	mock.EXPECT().
		ListNodes(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListNodesRow{
			{ID: staleID, PublicKey: []byte{0x01}, LastSeen: pgtype.Timestamptz{Time: time.Now().Add(-48 * time.Hour), Valid: true}},
			{ID: freshID, PublicKey: []byte{0x02}, LastSeen: pgtype.Timestamptz{Time: time.Now(), Valid: true}},
			{ID: unmeasuredID, PublicKey: []byte{0x03}, LastSeen: pgtype.Timestamptz{Valid: false}},
		}, nil)

	store := &Store{q: mock, staleThreshold: 24 * time.Hour}
	page, err := store.ListNodes(context.Background(), 0, nil, nil, nil, nil, "", "", "", 0, 10, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	byID := make(map[uuid.UUID]bool)
	for _, n := range page.Items {
		byID[n.ID] = n.Stale
	}
	if !byID[staleID] {
		t.Error("expected node last seen 48h ago to be stale with a 24h threshold")
	}
	if byID[freshID] {
		t.Error("expected node last seen just now to not be stale")
	}
	if byID[unmeasuredID] {
		t.Error("expected a node with no last_seen at all to not be stale")
	}
}

func TestListNodes_RadioStringFormatting(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	freq := float32(915.0)
	sf := int16(7)
	bw := float32(125.0)

	mock.EXPECT().
		ListNodes(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListNodesRow{
			{
				ID:           nodeID,
				PublicKey:    []byte{0x01},
				RadioFreqMhz: &freq,
				RadioSf:      &sf,
				RadioBwKhz:   &bw,
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListNodes(context.Background(), 0, nil, nil, nil, nil, "", "", "", 0, 10, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.Items[0].Radio == nil {
		t.Fatal("expected Radio to be set")
	}
	if *page.Items[0].Radio != "915,125,7" {
		t.Errorf("expected Radio 915,125,7, got %s", *page.Items[0].Radio)
	}
}

func TestGetNode_LastAdvertAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	lastAdvert := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	mock.EXPECT().
		GetNodeByID(gomock.Any(), nodeID).
		Return(sqlc.GetNodeByIDRow{
			ID:           nodeID,
			PublicKey:    []byte{0x01},
			FirstSeen:    pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:     pgtype.Timestamptz{Time: time.Now(), Valid: true},
			LastAdvertAt: lastAdvert,
		}, nil)

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return([]sqlc.GetNodeNeighborsRow{}, nil)

	store := &Store{q: mock}
	node, err := store.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.LastAdvertAt == nil {
		t.Fatal("expected LastAdvertAt to be set")
	}
	if *node.LastAdvertAt != 1700000000000 {
		t.Errorf("expected LastAdvertAt 1700000000000, got %d", *node.LastAdvertAt)
	}
}

func TestGetNode_LastAdvertAtNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		GetNodeByID(gomock.Any(), nodeID).
		Return(sqlc.GetNodeByIDRow{
			ID:        nodeID,
			PublicKey: []byte{0x01},
			FirstSeen: pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:  pgtype.Timestamptz{Time: time.Now(), Valid: true},
		}, nil)

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return([]sqlc.GetNodeNeighborsRow{}, nil)

	store := &Store{q: mock}
	node, err := store.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.LastAdvertAt != nil {
		t.Errorf("expected nil LastAdvertAt, got %d", *node.LastAdvertAt)
	}
}

func TestGetNode_Stale(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		GetNodeByID(gomock.Any(), nodeID).
		Return(sqlc.GetNodeByIDRow{
			ID:        nodeID,
			PublicKey: []byte{0x01},
			NodeType:  1, // companion -- Stale applies to every node type, unlike clock drift
			FirstSeen: pgtype.Timestamptz{Time: time.Now().Add(-72 * time.Hour), Valid: true},
			LastSeen:  pgtype.Timestamptz{Time: time.Now().Add(-48 * time.Hour), Valid: true},
		}, nil)

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return([]sqlc.GetNodeNeighborsRow{}, nil)

	store := &Store{q: mock, staleThreshold: 24 * time.Hour}
	node, err := store.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !node.Stale {
		t.Error("expected node last seen 48h ago to be stale with a 24h threshold")
	}
}

func TestGetNode_ClockDrift_OutOfSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	drift := int32(-600) // 10 minutes behind, beyond a 5m threshold

	mock.EXPECT().
		GetNodeByID(gomock.Any(), nodeID).
		Return(sqlc.GetNodeByIDRow{
			ID:                      nodeID,
			PublicKey:               []byte{0x01},
			NodeType:                2, // repeater
			FirstSeen:               pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:                pgtype.Timestamptz{Time: time.Now(), Valid: true},
			LastAdvertAt:            pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true},
			DeviceClockDriftSeconds: &drift,
		}, nil)

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return([]sqlc.GetNodeNeighborsRow{}, nil)

	store := &Store{q: mock, clockDriftThreshold: 5 * time.Minute}
	node, err := store.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.ClockDriftSeconds == nil || *node.ClockDriftSeconds != -600 {
		t.Fatalf("expected ClockDriftSeconds -600, got %v", node.ClockDriftSeconds)
	}
	if node.ClockOutOfSync == nil || !*node.ClockOutOfSync {
		t.Errorf("expected ClockOutOfSync true, got %v", node.ClockOutOfSync)
	}
	if node.ClockCheckedAt == nil || *node.ClockCheckedAt != 1700000000000 {
		t.Errorf("expected ClockCheckedAt 1700000000000, got %v", node.ClockCheckedAt)
	}
}

func TestGetNode_ClockDrift_InSync(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	drift := int32(30) // well within a 5m threshold

	mock.EXPECT().
		GetNodeByID(gomock.Any(), nodeID).
		Return(sqlc.GetNodeByIDRow{
			ID:                      nodeID,
			PublicKey:               []byte{0x01},
			NodeType:                3, // room server
			FirstSeen:               pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:                pgtype.Timestamptz{Time: time.Now(), Valid: true},
			LastAdvertAt:            pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true},
			DeviceClockDriftSeconds: &drift,
		}, nil)

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return([]sqlc.GetNodeNeighborsRow{}, nil)

	store := &Store{q: mock, clockDriftThreshold: 5 * time.Minute}
	node, err := store.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.ClockOutOfSync == nil || *node.ClockOutOfSync {
		t.Errorf("expected ClockOutOfSync false, got %v", node.ClockOutOfSync)
	}
}

func TestGetNode_ClockDrift_OmittedForCompanion(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	drift := int32(-600)

	mock.EXPECT().
		GetNodeByID(gomock.Any(), nodeID).
		Return(sqlc.GetNodeByIDRow{
			ID:                      nodeID,
			PublicKey:               []byte{0x01},
			NodeType:                1, // companion -- clock drift only applies to 2/3
			FirstSeen:               pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:                pgtype.Timestamptz{Time: time.Now(), Valid: true},
			LastAdvertAt:            pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true},
			DeviceClockDriftSeconds: &drift,
		}, nil)

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return([]sqlc.GetNodeNeighborsRow{}, nil)

	store := &Store{q: mock, clockDriftThreshold: 5 * time.Minute}
	node, err := store.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.ClockDriftSeconds != nil || node.ClockOutOfSync != nil || node.ClockCheckedAt != nil {
		t.Error("expected all three clock fields nil for a companion node")
	}
}

func TestGetNode_ClockDrift_OmittedWhenUnmeasured(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		GetNodeByID(gomock.Any(), nodeID).
		Return(sqlc.GetNodeByIDRow{
			ID:        nodeID,
			PublicKey: []byte{0x01},
			NodeType:  2, // repeater, but no advert-derived drift yet
			FirstSeen: pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:  pgtype.Timestamptz{Time: time.Now(), Valid: true},
		}, nil)

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return([]sqlc.GetNodeNeighborsRow{}, nil)

	store := &Store{q: mock, clockDriftThreshold: 5 * time.Minute}
	node, err := store.GetNode(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.ClockDriftSeconds != nil || node.ClockOutOfSync != nil || node.ClockCheckedAt != nil {
		t.Error("expected all three clock fields nil when no advert has been measured yet")
	}
}

func TestGetNodesByIDs_Mapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	name := "test-node"

	mock.EXPECT().
		GetNodesByIDs(gomock.Any(), []uuid.UUID{nodeID}).
		Return([]sqlc.GetNodesByIDsRow{
			{
				ID:        nodeID,
				Name:      &name,
				PublicKey: []byte{0xde, 0xad},
			},
		}, nil)

	store := &Store{q: mock}
	result, err := store.GetNodesByIDs(context.Background(), []uuid.UUID{nodeID})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	node, ok := result[nodeID]
	if !ok {
		t.Fatal("expected nodeID in result map")
	}
	if node.PublicKey != "dead" {
		t.Errorf("expected PublicKey dead, got %s", node.PublicKey)
	}
}

func TestGetNodeNeighbors_Deduplication(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	neighborID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	earlier := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	later := pgtype.Timestamptz{Time: time.UnixMilli(1700000001000), Valid: true}

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return([]sqlc.GetNodeNeighborsRow{
			{
				ID:               neighborID,
				PublicKey:        []byte{0x01},
				Iata:             "YVR",
				ObservationCount: 3,
				FirstSeen:        earlier,
				LastSeen:         earlier,
			},
			{
				ID:               neighborID,
				PublicKey:        []byte{0x01},
				Iata:             "YYJ",
				ObservationCount: 2,
				FirstSeen:        earlier,
				LastSeen:         later,
			},
		}, nil)

	store := &Store{q: mock}
	neighbors, err := store.GetNodeNeighbors(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(neighbors) != 1 {
		t.Fatalf("expected 1 deduplicated neighbor, got %d", len(neighbors))
	}
	if neighbors[0].ObservationCount != 5 {
		t.Errorf("expected ObservationCount 5, got %d", neighbors[0].ObservationCount)
	}
	if neighbors[0].IATA != "YYJ" {
		t.Errorf("expected IATA YYJ (most recent), got %s", neighbors[0].IATA)
	}
}

func TestGetNodeNeighbors_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		GetNodeNeighbors(gomock.Any(), nodeID).
		Return(nil, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.GetNodeNeighbors(context.Background(), nodeID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestListNodes_IncludeNeighbors_PassesFlagAndMapsIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	neighborID := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	mock.EXPECT().
		ListNodes(gomock.Any(), gomock.Eq(sqlc.ListNodesParams{
			Column1: int16(0), Column2: nil, Column3: "any", Column4: "any",
			Column5: nil, Column6: "", Column7: pgtype.Timestamptz{},
			Limit: 11, Column9: "", Column10: true,
		})).
		Return([]sqlc.ListNodesRow{
			{
				ID:          nodeID,
				PublicKey:   []byte{0x01},
				NeighborIds: []uuid.UUID{neighborID},
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListNodes(context.Background(), 0, nil, nil, nil, nil, "", "", "", 0, 10, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items[0].NeighborIDs) != 1 || page.Items[0].NeighborIDs[0] != neighborID {
		t.Errorf("expected NeighborIDs [%s], got %v", neighborID, page.Items[0].NeighborIDs)
	}
}

func TestListNodes_ExcludeNeighbors_LeavesIDsNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		ListNodes(gomock.Any(), gomock.Eq(sqlc.ListNodesParams{
			Column1: int16(0), Column2: nil, Column3: "any", Column4: "any",
			Column5: nil, Column6: "", Column7: pgtype.Timestamptz{},
			Limit: 11, Column9: "", Column10: false,
		})).
		Return([]sqlc.ListNodesRow{
			{ID: nodeID, PublicKey: []byte{0x01}, NeighborIds: nil},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListNodes(context.Background(), 0, nil, nil, nil, nil, "", "", "", 0, 10, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.Items[0].NeighborIDs != nil {
		t.Errorf("expected NeighborIDs to stay nil when includeNeighbors is false, got %v", page.Items[0].NeighborIDs)
	}
}

func TestDeleteOldNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	cutoff := time.Now().Add(-30 * 24 * time.Hour)

	mock.EXPECT().
		DeleteOldNodes(gomock.Any(), gomock.Eq(pgtype.Timestamptz{Time: cutoff, Valid: true})).
		Return(nil)

	store := &Store{q: mock}
	if err := store.DeleteOldNodes(context.Background(), cutoff); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListNodes_DefaultScopePreservesCaseAndMissingValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	scope := "#YOW"
	mock.EXPECT().ListNodes(gomock.Any(), gomock.Any()).Return([]sqlc.ListNodesRow{{DefaultScopeName: &scope}, {}}, nil)
	store := &Store{q: mock}
	page, err := store.ListNodes(context.Background(), 0, nil, nil, nil, nil, "", "", "", 0, 10, false)
	if err != nil || len(page.Items) != 2 || page.Items[0].DefaultScope == nil || *page.Items[0].DefaultScope != scope || page.Items[1].DefaultScope != nil {
		t.Fatalf("scope metadata lost or invented: %+v %v", page, err)
	}
}
