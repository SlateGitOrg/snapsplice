"""Applying events to the target.

``apply_guarded`` is the splice rule: any event, snapshot rows included, is applied only if its
LSN is greater than the key's stored LSN, and a delete leaves a tombstone carrying its LSN.
``apply_plain`` and ``upsert_plain`` are the unguarded appliers the comparison strategies use.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from .events import Decision, Event, SnapshotBatch
from .schema import OP_DELETE, SPECS, Pk, pk_text
from .target import Target


def _flush_data(
    target: Target,
    rows: dict[str, dict[Pk, tuple | None]],
    keystate: list[tuple[str, str, int, bool]] | None,
) -> None:
    for tbl, by_pk in rows.items():
        spec = SPECS[tbl]
        target.upsert_rows(spec, [r for r in by_pk.values() if r is not None])
        target.delete_rows(spec, [pk for pk, r in by_pk.items() if r is None])
    if keystate:
        target.write_key_states(keystate)


def apply_guarded(target: Target, events: Sequence[Event]) -> list[Decision]:
    """Apply data events in order under the LSN guard. Markers are ignored."""
    data = [e for e in events if e.is_data]
    decisions: list[Decision] = []
    if not data:
        return decisions
    state: dict[tuple[str, str], tuple[int, bool]] = {}
    for tbl in {e.tbl for e in data}:
        texts = sorted({e.pk_text for e in data if e.tbl == tbl})
        for text, st in target.key_states(tbl, texts).items():
            state[(tbl, text)] = st
    final: dict[str, dict[Pk, tuple | None]] = defaultdict(dict)
    last_lsn: dict[tuple[str, str], tuple[int, bool]] = {}
    for ev in data:
        key = (ev.tbl, ev.pk_text)
        cur = state.get(key)
        if cur is not None and ev.lsn <= cur[0]:
            decisions.append(
                Decision(
                    "event", ev.tbl, ev.pk, ev.lsn, "rejected_stale", detail=f"stored_lsn={cur[0]}"
                )
            )
            continue
        deleted = ev.op == OP_DELETE
        state[key] = (ev.lsn, deleted)
        last_lsn[key] = (ev.lsn, deleted)
        final[ev.tbl][ev.pk] = None if deleted else ev.row
        decisions.append(Decision("event", ev.tbl, ev.pk, ev.lsn, "applied", detail=ev.op))
    keystate = [(t, p, lsn, d) for (t, p), (lsn, d) in last_lsn.items()]
    target.begin()
    try:
        _flush_data(target, final, keystate)
        target.commit()
    except Exception:
        target.rollback()
        raise
    return decisions


def apply_snapshot(target: Target, batch: SnapshotBatch) -> list[Decision]:
    """Apply a chunk's surviving rows; each is accepted only above its key's stored LSN.
    Progress is saved in the same transaction, so a restart resumes after the last applied chunk."""
    spec = SPECS[batch.tbl]
    pks = [spec.pk_of(r) for r in batch.rows]
    state = target.key_states(batch.tbl, [pk_text(pk) for pk in pks])
    decisions: list[Decision] = []
    accepted: list[tuple] = []
    keystate: list[tuple[str, str, int, bool]] = []
    for pk, row in zip(pks, batch.rows, strict=True):
        text = pk_text(pk)
        cur = state.get(text)
        if cur is not None and batch.lsn <= cur[0]:
            verdict = "rejected_tombstone" if cur[1] else "rejected_stale"
            decisions.append(
                Decision(
                    "snapshot",
                    batch.tbl,
                    pk,
                    batch.lsn,
                    verdict,
                    batch.chunk_id,
                    f"stored_lsn={cur[0]}",
                )
            )
            continue
        accepted.append(row)
        keystate.append((batch.tbl, text, batch.lsn, False))
        decisions.append(Decision("snapshot", batch.tbl, pk, batch.lsn, "applied", batch.chunk_id))
    target.begin()
    try:
        target.upsert_rows(spec, accepted)
        target.write_key_states(keystate)
        target.save_progress(batch.tbl, batch.chunk_id, batch.last_key, batch.done)
        target.commit()
    except Exception:
        target.rollback()
        raise
    return decisions


def apply_plain(target: Target, events: Sequence[Event]) -> int:
    """Unguarded apply: the last event per key wins in the order given. Returns events applied."""
    data = [e for e in events if e.is_data]
    final: dict[str, dict[Pk, tuple | None]] = defaultdict(dict)
    for ev in data:
        final[ev.tbl][ev.pk] = None if ev.op == OP_DELETE else ev.row
    target.begin()
    try:
        _flush_data(target, final, None)
        target.commit()
    except Exception:
        target.rollback()
        raise
    return len(data)


def upsert_plain(target: Target, tbl: str, rows: Sequence[tuple]) -> None:
    """Unguarded bulk upsert of dumped rows."""
    target.begin()
    try:
        target.upsert_rows(SPECS[tbl], rows)
        target.commit()
    except Exception:
        target.rollback()
        raise
