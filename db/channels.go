// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// UpsertChannel records a hearing of a keyed (or, with a nil fingerprint, hash-only)
// channel and returns its id. With a cache, a known key is touched by id; a touch that
// finds no row (deleted or re-keyed since cached) falls back to the upsert.
func (s *Store) UpsertChannel(ctx context.Context, channelHash []byte, keyFingerprint []byte, name string, hashtag string) (int, error) {
	var namePtr, hashtagPtr *string
	if name != "" {
		namePtr = &name
	}
	if hashtag != "" {
		hashtagPtr = &hashtag
	}
	if keyFingerprint == nil {
		// The keyed statement's (hash, fingerprint) conflict target never matches a NULL fingerprint, so a second
		// hearing would hit the hash-only unique index; use the hash-only statements.
		if namePtr == nil && hashtagPtr == nil {
			return s.UpsertChannelHashOnly(ctx, channelHash)
		}
		isHashtag := hashtag != ""
		id, err := s.q.UpsertChannelHashOnlyNamed(ctx, sqlc.UpsertChannelHashOnlyNamedParams{
			ChannelHash: channelHash, Name: namePtr, Hashtag: hashtagPtr, IsHashtag: &isHashtag,
		})
		if err != nil {
			return 0, err
		}
		return int(id), nil
	}
	cached := s.channelIDs != nil && keyFingerprint != nil
	key := channelKey{hash: string(channelHash), fp: string(keyFingerprint)}
	if cached {
		if id, ok := s.channelIDs.get(key); ok {
			touched, err := s.q.TouchChannel(ctx, sqlc.TouchChannelParams{
				Name:           namePtr,
				ID:             id,
				ChannelHash:    channelHash,
				KeyFingerprint: keyFingerprint,
			})
			if err == nil {
				return int(touched), nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return 0, err
			}
			s.channelIDs.forget(key)
		}
	}
	isHashtag := hashtag != ""
	row, err := s.q.UpsertChannel(ctx, sqlc.UpsertChannelParams{
		ChannelHash: channelHash,
		Column2:     keyFingerprint, // key_fingerprint
		Name:        namePtr,
		Hashtag:     hashtagPtr,
		IsHashtag:   &isHashtag,
	})
	if err != nil {
		return 0, err
	}
	if cached {
		s.channelIDs.put(key, row.ID)
	}
	return int(row.ID), nil
}

// UpsertChannelHashOnly is UpsertChannel for a channel whose key is unknown.
func (s *Store) UpsertChannelHashOnly(ctx context.Context, channelHash []byte) (int, error) {
	key := channelKey{hash: string(channelHash), hashOnly: true}
	if s.channelIDs != nil {
		if id, ok := s.channelIDs.get(key); ok {
			touched, err := s.q.TouchChannelHashOnly(ctx, sqlc.TouchChannelHashOnlyParams{ID: id, ChannelHash: channelHash})
			if err == nil {
				return int(touched), nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return 0, err
			}
			s.channelIDs.forget(key)
		}
	}
	rowID, err := s.q.UpsertChannelHashOnly(ctx, channelHash)
	if err != nil {
		return 0, err
	}
	if s.channelIDs != nil {
		s.channelIDs.put(key, rowID)
	}
	return int(rowID), nil
}

// ListUndecryptedGroupTextPackets returns GRP_TXT packets never successfully decrypted --
// see internal/ingest.DecryptBackfill, which retries these against the current
// keystore after boot.
func (s *Store) ListUndecryptedGroupTextPackets(ctx context.Context) ([]ingest.UndecryptedPacket, error) {
	rows, err := s.q.ListUndecryptedGroupTextPackets(ctx)
	if err != nil {
		return nil, err
	}
	packets := make([]ingest.UndecryptedPacket, 0, len(rows))
	for _, v := range rows {
		packets = append(packets, ingest.UndecryptedPacket{
			PacketHash: v.PacketHash,
			RawPayload: v.RawPayload,
		})
	}
	return packets, nil
}

// ListUndecryptedGroupTextPacketsByHash limits the boot backfill to channels that just gained a key.
func (s *Store) ListUndecryptedGroupTextPacketsByHash(ctx context.Context, hashes [][]byte) ([]ingest.UndecryptedPacket, error) {
	rows, err := s.q.ListUndecryptedGroupTextPacketsByHash(ctx, hashes)
	if err != nil {
		return nil, err
	}
	packets := make([]ingest.UndecryptedPacket, 0, len(rows))
	for _, v := range rows {
		packets = append(packets, ingest.UndecryptedPacket{PacketHash: v.PacketHash, RawPayload: v.RawPayload})
	}
	return packets, nil
}

// SetChannelConfigScopes replaces every configured channel's region placement;
// an empty region means Beacon-wide.
func (s *Store) SetChannelConfigScopes(ctx context.Context, fingerprints [][]byte, regions []string) error {
	if err := s.q.DeleteChannelConfigScopes(ctx); err != nil {
		return err
	}
	if len(fingerprints) == 0 {
		return nil
	}
	return s.q.AddChannelConfigScopes(ctx, sqlc.AddChannelConfigScopesParams{Fingerprints: fingerprints, Regions: regions})
}

func (s *Store) UpsertChannelIATA(ctx context.Context, channelHash []byte, iata string, heardAt time.Time) error {
	return s.q.UpsertChannelIATA(ctx, sqlc.UpsertChannelIATAParams{
		ChannelHash: channelHash,
		Iata:        iata,
		LastHeard:   pgtype.Timestamptz{Time: heardAt, Valid: true},
	})
}

func (s *Store) DeleteOldChannelIATAs(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldChannelIATAs(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}

func (s *Store) ListChannels(ctx context.Context, limit int32, hash []byte, iatas []string, keyKnown *bool, cursor int64, pageCursor *api.ChannelCursor) (api.ChannelPage, error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	var rows []sqlc.Channel
	var err error
	if pageCursor != nil {
		rows, err = s.q.ListChannelsAfter(ctx, sqlc.ListChannelsAfterParams{
			ChannelHash: hash, Iatas: iatas, KeyKnown: keyKnown, PageLimit: limit + 1,
			CursorTs: pgtype.Timestamptz{Time: pageCursor.LastSeen, Valid: true}, CursorID: pageCursor.ID,
		})
	} else {
		rows, err = s.q.ListChannels(ctx, sqlc.ListChannelsParams{
			ChannelHash: hash, Iatas: iatas, KeyKnown: keyKnown, CursorTs: cursorTS, PageLimit: limit + 1,
		})
	}
	if err != nil {
		return api.ChannelPage{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.ChannelSummary, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.ChannelSummary{
			ID:          int(v.ID),
			Name:        v.Name,
			ChannelHash: hex.EncodeToString(v.ChannelHash),
			LastSeen:    v.LastSeen.Time.UnixMilli(),
			IsHashtag:   v.IsHashtag != nil && *v.IsHashtag,
			KeyKnown:    v.KeyKnown != nil && *v.KeyKnown,
		})
	}
	var nextCursor *int64
	var nextPageCursor *string
	if hasMore {
		last := items[len(items)-1].LastSeen
		nextCursor = &last
		row := rows[len(rows)-1]
		precise := (api.ChannelCursor{LastSeen: row.LastSeen.Time, ID: row.ID}).String()
		nextPageCursor = &precise
	}
	return api.ChannelPage{
		Page:           api.Page[api.ChannelSummary]{Items: items, NextCursor: nextCursor, HasMore: hasMore},
		NextPageCursor: nextPageCursor,
	}, nil
}

func (s *Store) GetChannel(ctx context.Context, channelID int32) (*api.Channel, error) {
	row, err := s.q.GetChannelByID(ctx, channelID)
	if err != nil {
		return nil, err
	}
	channel := api.Channel{
		ChannelSummary: api.ChannelSummary{
			ID:          int(row.ID),
			Name:        row.Name,
			ChannelHash: hex.EncodeToString(row.ChannelHash),
			LastSeen:    row.LastSeen.Time.UnixMilli(),
			IsHashtag:   row.IsHashtag != nil && *row.IsHashtag,
			KeyKnown:    row.KeyKnown != nil && *row.KeyKnown,
		},
		Hashtag:      row.Hashtag,
		MessageCount: row.MessageCount,
	}
	if row.IsHashtag != nil && *row.IsHashtag && row.KeyFingerprint != nil {
		fp := hex.EncodeToString(row.KeyFingerprint)
		channel.KeyFingerprint = &fp
	}
	return &channel, nil
}

// InsertChannelMessage stores a message and, in the same statement, the site of every stored observation of
// its packet. If that statement fails for any reason but cancellation, the message is stored without its sites
// (the read-model reconcile adds them), so no error path loses a message today's statement would store.
func (s *Store) InsertChannelMessage(ctx context.Context, m ingest.InsertChannelMessageParams) (*ingest.InsertedChannelMessage, error) {
	params := sqlc.InsertChannelMessageParams{ChannelID: int32(m.ChannelID), PacketHash: m.PacketHash, SenderName: &m.SenderName, Content: &m.Content, SentAt: pgtype.Timestamptz{Time: m.SentAt, Valid: true}, Column6: m.Historical}
	row, err := s.q.InsertChannelMessage(ctx, params)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // duplicate
	}
	if err != nil {
		// Whatever happens next, a page may miss this packet's sites until the reconcile: the fallback stores the
		// message without them, and a failed insert's Add finds no message while another lane's is in flight.
		s.readModels.invalidateCmi("a channel message insert failed")
	}
	if err != nil && ctx.Err() == nil {
		slog.Warn("channel message site seed failed; storing the message without it", "component", "db", "packet_hash", hex.EncodeToString(m.PacketHash), "error", err)
		unseeded, uerr := s.q.InsertChannelMessageUnseeded(ctx, sqlc.InsertChannelMessageUnseededParams(params))
		// Again once the fallback has returned: a reconcile that took its snapshot before this commit must not be
		// the one that restores trust.
		s.readModels.invalidateCmi("a channel message was stored without its sites")
		if errors.Is(uerr, pgx.ErrNoRows) {
			return nil, nil // duplicate
		}
		if uerr != nil {
			return nil, uerr
		}
		row, err = sqlc.InsertChannelMessageRow(unseeded), nil
	}
	if err != nil {
		return nil, err
	}
	return &ingest.InsertedChannelMessage{ID: row.ID, Scope: row.ScopeName, ScopeStatus: api.RecordedChannelScopeStatus(row.ScopeName, row.TransportCodesPresent)}, nil
}

// AddChannelMessageIATA records iata as a site of the packet's message, when the packet has one.
// found says the packet has a committed message, so the site is recorded. A failure drops cmi trust until the next
// reconcile adds the pair.
func (s *Store) AddChannelMessageIATA(ctx context.Context, packetHash []byte, iata string) (bool, error) {
	found, err := s.q.AddChannelMessageIATA(ctx, sqlc.AddChannelMessageIATAParams{PacketHash: packetHash, Iata: iata})
	if err != nil {
		s.readModels.invalidateCmi("a message site was not recorded")
	}
	return found, err
}

// ListChannelMessages serves /channels/{id}/messages (channelID set) and /messages.
func (s *Store) ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	return listChannelMessages(ctx, s.q, s.readModels.cmiTrusted(), channelID, since, limit, iatas, scope, cursor)
}

// listChannelMessages runs one statement per request shape. A zero since is "no since", nil and empty iatas are
// "no region", "" is "no scope". The shapes filtered by site, and those filtered by scope without since, read
// channel_message_iatas; without useReadModel they answer api.ErrPagesNotReady. The others never read it.
func listChannelMessages(ctx context.Context, q sqlc.Querier, useReadModel bool, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	hasSince, hasSites, hasScope := !since.IsZero(), len(iatas) > 0, scope != ""
	// Sites with a since or a scope across every channel have no index that bounds both: each site's range would
	// be walked until enough rows pass the filter, so the original all-channel statement serves them (on the slow
	// pool), trusted or not.
	if channelID == nil && hasSites && (hasSince || hasScope) {
		return listChannelMessagesLegacy(ctx, q, nil, since, limit, iatas, scope, cursor)
	}
	if (hasSites || (hasScope && !hasSince)) && !useReadModel {
		return api.Page[api.ChannelMessage]{}, fmt.Errorf("channel messages: %w", api.ErrPagesNotReady)
	}
	var scopeID *int32
	if hasScope {
		id, err := q.GetTransportScopeByName(ctx, scope)
		if errors.Is(err, pgx.ErrNoRows) {
			return api.Page[api.ChannelMessage]{Items: []api.ChannelMessage{}}, nil
		}
		if err != nil {
			return api.Page[api.ChannelMessage]{}, err
		}
		scopeID = &id
	}
	sites := dedupeSites(iatas)
	cursorID := cursor
	if cursor == 0 {
		cursorID = math.MaxInt64
	}
	pageLimit := limit + 1
	ts := pgtype.Timestamptz{Time: since, Valid: true}

	var rows []sqlc.ListChannelMessagesRow
	var err error
	switch {
	case channelID != nil && hasSince:
		rows, err = convertRows(q.ListChannelMessagesSinceWalk(ctx, sqlc.ListChannelMessagesSinceWalkParams{ChannelID: *channelID, Since: ts, Iatas: sites, ScopeID: scopeID, CursorID: cursorID, PageLimit: pageLimit}))
	case channelID != nil && hasSites && hasScope:
		rows, err = convertRows(q.ListChannelMessagesByIATAsScoped(ctx, sqlc.ListChannelMessagesByIATAsScopedParams{Iatas: sites, ChannelID: *channelID, ScopeID: *scopeID, CursorID: cursorID, PageLimit: pageLimit}))
	case channelID != nil && hasSites:
		rows, err = convertRows(q.ListChannelMessagesByIATAs(ctx, sqlc.ListChannelMessagesByIATAsParams{Iatas: sites, ChannelID: *channelID, CursorID: cursorID, PageLimit: pageLimit}))
	case channelID != nil && hasScope:
		rows, err = convertRows(q.ListChannelMessagesPlainScoped(ctx, sqlc.ListChannelMessagesPlainScopedParams{ChannelID: *channelID, ScopeID: *scopeID, CursorID: cursorID, PageLimit: pageLimit}))
	case channelID != nil:
		rows, err = convertRows(q.ListChannelMessagesPlain(ctx, sqlc.ListChannelMessagesPlainParams{ChannelID: *channelID, CursorID: cursorID, PageLimit: pageLimit}))
	case hasSites:
		rows, err = convertRows(q.ListAllChannelMessagesByIATAs(ctx, sqlc.ListAllChannelMessagesByIATAsParams{Iatas: sites, CursorID: cursorID, PageLimit: pageLimit}))
	case hasSince:
		rows, err = convertRows(q.ListAllChannelMessagesSince(ctx, sqlc.ListAllChannelMessagesSinceParams{Since: ts, ScopeID: scopeID, CursorID: cursorID, PageLimit: pageLimit}))
	case hasScope:
		rows, err = convertRows(q.ListAllChannelMessagesPlainScoped(ctx, sqlc.ListAllChannelMessagesPlainScopedParams{ScopeID: *scopeID, CursorID: cursorID, PageLimit: pageLimit}))
	default:
		rows, err = convertRows(q.ListAllChannelMessagesPlain(ctx, sqlc.ListAllChannelMessagesPlainParams{CursorID: cursorID, PageLimit: pageLimit}))
	}
	if err != nil {
		return api.Page[api.ChannelMessage]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	messages := make([]api.ChannelMessage, 0, len(rows))
	for _, v := range rows {
		messages = append(messages, toChannelMessage(v.ID, v.PacketHashHex, v.ChannelHash, v.SenderName, v.Content, v.SentAt, v.ObservationCount, v.ScopeName, v.TransportCodesPresent))
	}
	var nextCursor *int64
	if hasMore && len(messages) > 0 {
		last := messages[len(messages)-1].ID
		nextCursor = &last
	}
	return api.Page[api.ChannelMessage]{Items: messages, NextCursor: nextCursor, HasMore: hasMore}, nil
}

// dedupeSites drops repeated codes in one pass, keeping first-occurrence order; never nil.
func dedupeSites(iatas []string) []string {
	sites := make([]string, 0, len(iatas))
	seen := make(map[string]struct{}, len(iatas))
	for _, iata := range iatas {
		if _, dup := seen[iata]; !dup {
			seen[iata] = struct{}{}
			sites = append(sites, iata)
		}
	}
	return sites
}

// messageRow is every list statement's row: the columns of sqlc.ListChannelMessagesRow.
type messageRow interface {
	sqlc.ListChannelMessagesByIATAsRow | sqlc.ListChannelMessagesByIATAsScopedRow | sqlc.ListChannelMessagesPlainRow |
		sqlc.ListChannelMessagesPlainScopedRow | sqlc.ListChannelMessagesSinceWalkRow | sqlc.ListAllChannelMessagesByIATAsRow |
		sqlc.ListAllChannelMessagesPlainRow |
		sqlc.ListAllChannelMessagesPlainScopedRow | sqlc.ListAllChannelMessagesSinceRow
}

func convertRows[R messageRow](rows []R, err error) ([]sqlc.ListChannelMessagesRow, error) {
	if err != nil {
		return nil, err
	}
	out := make([]sqlc.ListChannelMessagesRow, len(rows))
	for i, r := range rows {
		out[i] = sqlc.ListChannelMessagesRow(r)
	}
	return out, nil
}

// listChannelMessagesLegacy is the original statement pair: one join over every observation, filtered in place.
// The all-channel one serves sites with a since or a scope; the per-channel one (ListChannelMessages) is only the
// first-time verification's oracle and is never routed to an API pool.
func listChannelMessagesLegacy(ctx context.Context, q sqlc.Querier, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	ts := pgtype.Timestamptz{Time: since, Valid: !since.IsZero()}
	var messages []api.ChannelMessage
	var hasMore bool
	if channelID == nil {
		rows, err := q.ListAllChannelMessages(ctx, sqlc.ListAllChannelMessagesParams{
			Column1: ts,
			Column2: iatas,
			Column3: scope,
			Column4: cursor,
			Limit:   limit + 1,
		})
		if err != nil {
			return api.Page[api.ChannelMessage]{}, err
		}
		hasMore = len(rows) > int(limit)
		if hasMore {
			rows = rows[:limit]
		}
		messages = make([]api.ChannelMessage, 0, len(rows))
		for _, v := range rows {
			messages = append(messages, toChannelMessage(v.ID, v.PacketHashHex, v.ChannelHash, v.SenderName, v.Content, v.SentAt, v.ObservationCount, v.ScopeName, v.TransportCodesPresent))
		}
	} else {
		rows, err := q.ListChannelMessages(ctx, sqlc.ListChannelMessagesParams{
			ChannelID: *channelID,
			Column2:   ts,
			Column3:   iatas,
			Column4:   scope,
			Column5:   cursor,
			Limit:     limit + 1,
		})
		if err != nil {
			return api.Page[api.ChannelMessage]{}, err
		}
		hasMore = len(rows) > int(limit)
		if hasMore {
			rows = rows[:limit]
		}
		messages = make([]api.ChannelMessage, 0, len(rows))
		for _, v := range rows {
			messages = append(messages, toChannelMessage(v.ID, v.PacketHashHex, v.ChannelHash, v.SenderName, v.Content, v.SentAt, v.ObservationCount, v.ScopeName, v.TransportCodesPresent))
		}
	}

	var nextCursor *int64
	if hasMore && len(messages) > 0 {
		last := messages[len(messages)-1].ID
		nextCursor = &last
	}
	return api.Page[api.ChannelMessage]{
		Items:      messages,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *Store) ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	rows, err := s.q.ListChannelMessagesByHash(ctx, sqlc.ListChannelMessagesByHashParams{
		ChannelHash: hash,
		Column2:     pgtype.Timestamptz{Time: since, Valid: !since.IsZero()},
		Column3:     iatas,
		Column4:     scope,
		Column5:     cursor,
		Limit:       limit + 1,
	})
	if err != nil {
		return api.Page[api.ChannelMessage]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	messages := make([]api.ChannelMessage, 0, len(rows))
	for _, v := range rows {
		messages = append(messages, toChannelMessage(v.ID, hex.EncodeToString(v.PacketHash), v.ChannelHash, v.SenderName, v.Content, v.SentAt, v.ObservationCount, v.ScopeName, v.TransportCodesPresent))
	}
	var nextCursor *int64
	if hasMore && len(messages) > 0 {
		last := messages[len(messages)-1].ID
		nextCursor = &last
	}
	return api.Page[api.ChannelMessage]{
		Items:      messages,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *Store) ListMessagesAfterID(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) ([]api.ChannelMessage, error) {
	rows, err := s.q.ListMessagesAfterID(ctx, sqlc.ListMessagesAfterIDParams{
		ID:      afterID,
		Column2: iatas,
		Column3: scope,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.ChannelMessage, 0, len(rows))
	for _, v := range rows {
		items = append(items, toChannelMessage(
			v.ID,
			v.PacketHashHex,
			v.ChannelHash,
			v.SenderName,
			v.Content,
			v.SentAt,
			v.ObservationCount,
			v.ScopeName,
			v.TransportCodesPresent,
		))
	}
	return items, nil
}
