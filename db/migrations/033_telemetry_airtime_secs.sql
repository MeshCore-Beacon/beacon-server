-- These always held seconds of radio time from the observer's tx_air_secs/rx_air_secs
-- status fields, never a percentage; name them for what they are.
ALTER TABLE observer_telemetry RENAME COLUMN airtime_tx_pct TO airtime_tx_secs;
ALTER TABLE observer_telemetry RENAME COLUMN airtime_rx_pct TO airtime_rx_secs;
