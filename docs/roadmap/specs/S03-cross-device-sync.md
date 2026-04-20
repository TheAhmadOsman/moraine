# S03 — Cross-Device Sync and Conflict Resolution

**Priority:** P3  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** R01 (backup/restore), C02 (remote import), S01 (identity), P02 (notes), P01 (summaries)

## Objective

Move beyond one-way remote mirrors to bidirectional personal sync. Keep multiple personal machines in one logical corpus with deterministic conflict resolution.

## Design Principles

1. **Sync is explicit, not automatic.** User initiates sync or schedules it. No background sync that surprises users with bandwidth or storage costs. [src: ADR-001]
2. **Conflict resolution is deterministic and auditable.** Same inputs always produce same outputs. Conflicts are logged, not silently merged. [src: ADR-005]
3. **Device identity is cryptographically verifiable.** Each device has a key pair. Sync bundles are signed and encrypted.
4. **Raw events are append-only across devices.** A session started on laptop A and continued on desktop B is a single merged session, not two separate sessions.

## Schema Design

### New Tables

```sql
-- Device registry for a user/tenant.
CREATE TABLE IF NOT EXISTS moraine.devices (
  device_id String,
  user_id String,
  tenant_id String,
  device_name String,
  public_key String,
  last_seen_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (user_id, device_id);

-- Sync manifests: what was sent/received.
CREATE TABLE IF NOT EXISTS moraine.sync_manifests (
  manifest_id String,
  device_id String,
  user_id String,
  direction LowCardinality(String),      -- 'push', 'pull'
  bundle_hash String,
  bundle_size UInt64,
  session_ids Array(String),
  row_counts_json String,
  conflicts_json String,
  created_at DateTime64(3),
  event_version UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, manifest_id);

-- Tombstones: deleted sessions or notes that must propagate.
CREATE TABLE IF NOT EXISTS moraine.tombstones (
  tombstone_uid String,
  target_kind LowCardinality(String),    -- 'session', 'note', 'summary', 'memory_card'
  target_id String,
  device_id String,
  user_id String,
  deleted_at DateTime64(3),
  event_version UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(deleted_at)
ORDER BY (target_id, deleted_at);
```

## Sync Bundle Format

A sync bundle is an encrypted, signed archive:

```
sync-bundle-<{manifest_id}>.tar.gz.enc
  manifest.json
  events/                    # canonical events delta
  notes/                     # notes delta
  summaries/                 # summaries delta
  tombstones.jsonl
  signatures.json            # device signatures
```

**Encryption:** AES-256-GCM with key derived from device private key + recipient public key (ECDH).  
**Signing:** Ed25519 device key pair.

## API Sketches

### CLI Commands

```bash
moraine sync devices list                    # register this device and list others
moraine sync devices register --name "MacBook Pro"
moraine sync push --to-device desktop-home   # push local delta to specific device
moraine sync pull --from-device desktop-home # pull remote delta
moraine sync schedule --every 1h             # automatic sync cadence
moraine sync resolve --conflict <id>         # inspect and resolve conflicts
moraine sync status                          # last sync, pending deltas, conflict count
```

### Sync Protocol (Device-to-Device)

Direct device sync (no cloud server):
1. Devices discover each other via LAN mDNS or configured static addresses.
2. Initiator sends `sync_request` with last-known manifest hash.
3. Responder computes delta (events/notes/summaries newer than last sync).
4. Responder encrypts bundle with initiator's public key.
5. Initiator verifies signature, decrypts, applies delta, reports conflicts.

Optional cloud relay (for devices not on same LAN):
- Small encrypted bundles (<10MB) relayed through a simple blob store.
- Server cannot decrypt; only routes.

## Conflict Rules

| Conflict | Rule | Rationale |
|---|---|---|
| Same note edited on both devices | Last-write-wins by `updated_at`; older revision preserved in `note_revisions`. | Notes are user-authored; user can manually merge later. |
| Same session received from two devices | Merge events by `event_uid`; duplicates converged by `ReplacingMergeTree`. | Events are immutable facts; same UID = same event. |
| Summary stale on one device | Re-mark stale; regenerate on next summary pass. | Summaries are derived and cheap to rebuild. |
| Device A deleted session, device B added events | Tombstone wins; new events from B are quarantined in `sync_conflicts` for user review. | Deletion is intentional; don't silently resurrect. |
| Divergent privacy policies | Higher version number wins; lower-version rows flagged for reprocessing. | Privacy policy changes are explicit. |

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Same source ingested on two machines | Source config includes `device_id`; events from both devices merge by `event_uid`. |
| Clock skew | Use logical clocks (event_version) or NTP requirement. Conflict resolution uses `updated_at` as tiebreaker with 60s tolerance window. |
| Deleted sessions | Tombstones propagate; deleted sessions do not reappear unless user explicitly restores from backup. |
| Divergent privacy policies | Policy version in row metadata; sync flags conflicts when versions differ. |
| Large bundles | Chunking: bundles >10MB are split into numbered chunks with manifest. |
| Device lost/stolen | Revoke device key from other devices; future syncs reject revoked key signatures. |

## Acceptance Contract

### Functional
- [ ] Sync of 10k events between two devices completes in under 60 seconds on LAN.
- [ ] After sync, both devices return identical `session_events` for merged sessions.
- [ ] Tombstones propagate and prevent deleted sessions from reappearing.
- [ ] Conflict log is inspectable via `moraine sync status` and monitor UI.

### Operational
- [ ] Sync does not block ingest or search on either device.
- [ ] Bundle encryption uses authenticated encryption (AES-GCM or ChaCha20-Poly1305).
- [ ] Sync bandwidth is proportional to delta size, not total corpus size.

### Safety
- [ ] Sync bundles are encrypted end-to-end; relay server (if any) cannot read contents.
- [ ] Device keys are stored in OS keychain, not plaintext files.
- [ ] Privacy-redacted rows sync as redacted; raw secrets never leave the originating device.

### Compatibility
- [ ] Sync protocol is versioned; incompatible versions reject sync with clear error.
- [ ] Single-device mode is unchanged when no peer devices are configured.

### Observability
- [ ] Monitor shows sync status, device list, last sync time, and conflict count.
- [ ] `moraine doctor` checks for orphan tombstones and unpropagated deletions.

## PR Sequencing

1. `feat(sync): add device registry and key generation`  
   - Device identity; key pair in OS keychain.
2. `feat(sync): define sync bundle format and encryption`  
   - Archive format, encryption, signing.
3. `feat(sync): add delta computation and sync protocol`  
   - Identify changes since last sync manifest.
4. `feat(sync): add LAN device discovery and direct sync`  
   - mDNS or static address; push/pull.
5. `feat(sync): add conflict detection and resolution UI`  
   - Monitor panel for conflicts; manual merge helpers.
6. `feat(sync): add cloud relay option`  
   - Optional; encrypted blob relay.
7. `test(sync): add multi-device fixture tests`  
   - Simulate two devices, sync, verify convergence.

## Open Questions

1. **LAN vs cloud relay priority:** LAN first (privacy-preserving, no server needed). Cloud relay as optional convenience.
2. **Should sync support real-time collaboration?** No — async sync only. Real-time is out of scope.
3. **Conflict resolution UI complexity:** Start with simple last-write-wins + audit log. Manual merge UI is P2 follow-up.
