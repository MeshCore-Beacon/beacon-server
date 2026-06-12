-- Remove neighbor records where either side is not infrastructure (repeater or room).
-- These are dirty rows from historical prefix collisions resolving to non-forwarding nodes.
DELETE FROM node_neighbors nn
USING nodes n
WHERE nn.neighbor_id = n.id
  AND n.node_type NOT IN (2, 3);

DELETE FROM node_neighbors nn
USING nodes n
WHERE nn.node_id = n.id
  AND n.node_type NOT IN (2, 3);
