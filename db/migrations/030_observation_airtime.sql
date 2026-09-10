-- Time-on-air per observation so per-observer activity never joins back to packets.
ALTER TABLE packet_observations ADD COLUMN airtime_ms REAL;
