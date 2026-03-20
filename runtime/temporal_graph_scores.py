import json
import math
import sys

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None

try:
    from scipy import sparse
except Exception:  # pragma: no cover
    sparse = None


DAMPING = 0.85
MAX_ITER = 100
TOL = 1e-9
DAY_MS = 24 * 60 * 60 * 1000


def _coerce_int(value, default=0):
    try:
        return int(round(float(value)))
    except Exception:
        return int(default)


def _coerce_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _load_payload():
    raw = sys.stdin.read()
    if not raw.strip():
      return {"rows": [], "now_ts": 0}
    parsed = json.loads(raw)
    if isinstance(parsed, list):
        return {"rows": parsed, "now_ts": 0}
    if isinstance(parsed, dict):
        return {
            "rows": parsed.get("rows", []),
            "now_ts": _coerce_int(parsed.get("now_ts", 0), 0),
        }
    return {"rows": [], "now_ts": 0}


def _normalize_rows(rows):
    normalized = []
    nodes = set()
    for item in rows if isinstance(rows, list) else []:
        if not isinstance(item, dict):
            continue
        src = str(item.get("src") or "").strip()
        dst = str(item.get("dst") or "").strip()
        source_key = str(item.get("source_key") or src or dst).strip()
        ts = _coerce_int(item.get("ts", 0), 0)
        weight = _coerce_float(item.get("weight", 0.0), 0.0)
        if source_key:
            nodes.add(source_key)
        if src:
            nodes.add(src)
        if dst:
            nodes.add(dst)
        if not src or not dst:
            continue
        normalized.append({
            "src": src,
            "dst": dst,
            "source_key": source_key or src,
            "ts": ts,
            "weight": weight,
        })
    return sorted(nodes), normalized


def _neighbor_weights(rows):
    out = {}
    for row in rows:
        src = row["src"]
        dst = row["dst"]
        weight = _coerce_float(row["weight"], 0.0)
        if weight <= 0 or src == dst:
            continue
        out.setdefault(src, {})
        out[src][dst] = out[src].get(dst, 0.0) + weight
    return out


def _top_neighbors(nodes, rows):
    weights = _neighbor_weights(rows)
    out = {}
    for node in nodes:
        pairs = []
        for dst, weight in weights.get(node, {}).items():
            pairs.append({"source_key": dst, "weight": weight})
        pairs.sort(key=lambda item: (-float(item["weight"]), item["source_key"]))
        out[node] = pairs[:5]
    return out


def _pagerank_dense(nodes, row_weights):
    n = len(nodes)
    if n == 0:
        return {}
    index = {node: i for i, node in enumerate(nodes)}
    matrix = [[0.0] * n for _ in range(n)]
    out_sums = [0.0] * n
    for src, targets in row_weights.items():
        src_idx = index[src]
        total = sum(float(weight) for weight in targets.values() if float(weight) > 0)
        out_sums[src_idx] = total
        if total <= 0:
            continue
        for dst, weight in targets.items():
            if float(weight) <= 0:
                continue
            matrix[index[dst]][src_idx] += float(weight) / total
    pr = [1.0 / n] * n
    teleport = (1.0 - DAMPING) / n
    for _ in range(MAX_ITER):
        dangling = sum(pr[i] for i in range(n) if out_sums[i] <= 0)
        next_pr = [teleport + (DAMPING * dangling / n) for _ in range(n)]
        for row_idx in range(n):
            acc = next_pr[row_idx]
            for col_idx in range(n):
                if matrix[row_idx][col_idx]:
                    acc += DAMPING * matrix[row_idx][col_idx] * pr[col_idx]
            next_pr[row_idx] = acc
        delta = sum(abs(next_pr[i] - pr[i]) for i in range(n))
        pr = next_pr
        if delta <= TOL:
            break
    return {node: float(pr[index[node]]) for node in nodes}


def _pagerank_numpy(nodes, row_weights):
    n = len(nodes)
    if n == 0 or np is None:
        return _pagerank_dense(nodes, row_weights)
    index = {node: i for i, node in enumerate(nodes)}
    out_sums = np.zeros(n, dtype=float)
    if sparse is not None:
        data = []
        rows = []
        cols = []
        for src, targets in row_weights.items():
            src_idx = index[src]
            total = sum(float(weight) for weight in targets.values() if float(weight) > 0)
            out_sums[src_idx] = total
            if total <= 0:
                continue
            for dst, weight in targets.items():
                w = float(weight)
                if w <= 0:
                    continue
                rows.append(index[dst])
                cols.append(src_idx)
                data.append(w / total)
        matrix = sparse.csr_matrix((data, (rows, cols)), shape=(n, n), dtype=float)
        pr = np.full(n, 1.0 / n, dtype=float)
        teleport = (1.0 - DAMPING) / n
        for _ in range(MAX_ITER):
            dangling = float(pr[out_sums <= 0].sum())
            next_pr = teleport + (DAMPING * dangling / n) + DAMPING * (matrix @ pr)
            if float(np.abs(next_pr - pr).sum()) <= TOL:
                pr = next_pr
                break
            pr = next_pr
        return {node: float(pr[index[node]]) for node in nodes}

    matrix = np.zeros((n, n), dtype=float)
    for src, targets in row_weights.items():
        src_idx = index[src]
        total = sum(float(weight) for weight in targets.values() if float(weight) > 0)
        out_sums[src_idx] = total
        if total <= 0:
            continue
        for dst, weight in targets.items():
            w = float(weight)
            if w <= 0:
                continue
            matrix[index[dst], src_idx] += w / total
    pr = np.full(n, 1.0 / n, dtype=float)
    teleport = (1.0 - DAMPING) / n
    for _ in range(MAX_ITER):
        dangling = float(pr[out_sums <= 0].sum())
        next_pr = teleport + (DAMPING * dangling / n) + DAMPING * (matrix @ pr)
        if float(np.abs(next_pr - pr).sum()) <= TOL:
            pr = next_pr
            break
        pr = next_pr
    return {node: float(pr[index[node]]) for node in nodes}


def _window_rows(rows, min_ts):
    if min_ts <= 0:
        return rows
    return [row for row in rows if _coerce_int(row.get("ts", 0), 0) >= min_ts]


def compute_temporal_scores(rows, now_ts=0):
    nodes, normalized_rows = _normalize_rows(rows)
    top_neighbors = _top_neighbors(nodes, normalized_rows)
    row_weights_all = _neighbor_weights(normalized_rows)
    global_scores = _pagerank_numpy(nodes, row_weights_all)

    now = _coerce_int(now_ts, 0)
    last_30 = now - (30 * DAY_MS) if now > 0 else 0
    last_7 = now - (7 * DAY_MS) if now > 0 else 0
    recent_30_scores = _pagerank_numpy(nodes, _neighbor_weights(_window_rows(normalized_rows, last_30)))
    recent_7_scores = _pagerank_numpy(nodes, _neighbor_weights(_window_rows(normalized_rows, last_7)))

    out = []
    for node in nodes:
        out.append({
            "source_key": node,
            "global_score": float(global_scores.get(node, 0.0)),
            "recent_30d_score": float(recent_30_scores.get(node, 0.0)),
            "recent_7d_score": float(recent_7_scores.get(node, 0.0)),
            "top_neighbors": top_neighbors.get(node, []),
            "computed_at": now,
        })
    return out


def main():
    payload = _load_payload()
    rows = payload.get("rows", [])
    now_ts = payload.get("now_ts", 0)
    scores = compute_temporal_scores(rows, now_ts=now_ts)
    sys.stdout.write(json.dumps({"ok": True, "scores": scores}))


if __name__ == "__main__":
    main()
