package db

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Tower/tower-server/db/sqlc"
	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/MeshCore-Tower/tower-server/internal/ingest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) UpsertChannel(ctx context.Context, channelHash []byte, keyFingerprint []byte, name string, hashtag string) (int, error) {
	var namePtr, hashtagPtr *string
	if name != "" {
		namePtr = &name
	}
	if hashtag != "" {
		hashtagPtr = &hashtag
	}
	isHashtag := hashtag != ""
	row, err := s.q.UpsertChannel(ctx, sqlc.UpsertChannelParams{
		ChannelHash:  channelHash,
		Column2:      keyFingerprint, // key_fingerprint
		Name:         namePtr,
		Hashtag:      hashtagPtr,
		IsHashtag:    &isHashtag,
		MessageCount: nil, // message count bumped separately by InsertChannelMessage
	})
	if err != nil {
		return 0, err
	}
	return int(row.ID), nil
}

func (s *Store) UpsertChannelHashOnly(ctx context.Context, channelHash []byte) (int, error) {
	rowID, err := s.q.UpsertChannelHashOnly(ctx, channelHash)
	if err != nil {
		return 0, err
	}
	return int(rowID), nil
}

func (s *Store) ListChannels(ctx context.Context, limit int32, hash []byte, iata string, cursor int64) (api.Page[api.ChannelSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	rows, err := s.q.ListChannels(ctx, sqlc.ListChannelsParams{
		Column1: hash,
		Column2: iata,
		Column3: cursorTS,
		Limit:   limit + 1,
	})
	if err != nil {
		return api.Page[api.ChannelSummary]{}, err
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
	if hasMore {
		last := items[len(items)-1].LastSeen
		nextCursor = &last
	}
	return api.Page[api.ChannelSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
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
		MessageCount: 0,
	}
	if row.MessageCount != nil {
		channel.MessageCount = *row.MessageCount
	}
	if row.IsHashtag != nil && *row.IsHashtag && row.KeyFingerprint != nil {
		fp := hex.EncodeToString(row.KeyFingerprint)
		channel.KeyFingerprint = &fp
	}
	return &channel, nil
}

func (s *Store) InsertChannelMessage(ctx context.Context, m ingest.InsertChannelMessageParams) (bool, error) {
	params := sqlc.InsertChannelMessageParams{ChannelID: int32(m.ChannelID), PacketHash: m.PacketHash, SenderName: &m.SenderName, Content: &m.Content, SentAt: pgtype.Timestamptz{Time: m.SentAt, Valid: true}}
	_, err := s.q.InsertChannelMessage(ctx, params)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // duplicate
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
	ts := pgtype.Timestamptz{Time: since, Valid: !since.IsZero()}
	var messages []api.ChannelMessage
	var hasMore bool
	iataFilter := strings.Join(iatas, ",")
	if channelID == nil {
		rows, err := s.q.ListAllChannelMessages(ctx, sqlc.ListAllChannelMessagesParams{
			Column1: ts,
			Column2: iataFilter,
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
			messages = append(messages, toChannelMessage(v.ID, v.PacketHashHex, v.ChannelHash, v.SenderName, v.Content, v.SentAt, v.ObservationCount))
		}
	} else {
		rows, err := s.q.ListChannelMessages(ctx, sqlc.ListChannelMessagesParams{
			ChannelID: *channelID,
			Column2:   ts,
			Column3:   iataFilter,
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
			messages = append(messages, toChannelMessage(v.ID, v.PacketHashHex, v.ChannelHash, v.SenderName, v.Content, v.SentAt, v.ObservationCount))
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
	iataFilter := strings.Join(iatas, ",")
	rows, err := s.q.ListChannelMessagesByHash(ctx, sqlc.ListChannelMessagesByHashParams{
		ChannelHash: hash,
		Column2:     pgtype.Timestamptz{Time: since, Valid: !since.IsZero()},
		Column3:     iataFilter,
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
		messages = append(messages, toChannelMessage(v.ID, hex.EncodeToString(v.PacketHash), v.ChannelHash, v.SenderName, v.Content, v.SentAt, v.ObservationCount))
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
