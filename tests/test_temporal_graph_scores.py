import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from runtime.temporal_graph_scores import compute_temporal_scores


DAY_MS = 24 * 60 * 60 * 1000


class TemporalGraphScoresTest(unittest.TestCase):
    def test_empty_graph(self):
        self.assertEqual(compute_temporal_scores([], now_ts=0), [])

    def test_single_node_graph_is_stable(self):
        rows = [{"src": "a", "dst": "a", "ts": 0, "weight": 0, "source_key": "a"}]
        scores = compute_temporal_scores(rows, now_ts=0)
        self.assertEqual(len(scores), 1)
        self.assertEqual(scores[0]["source_key"], "a")
        self.assertAlmostEqual(scores[0]["global_score"], 1.0, places=9)
        self.assertAlmostEqual(scores[0]["recent_30d_score"], 1.0, places=9)
        self.assertAlmostEqual(scores[0]["recent_7d_score"], 1.0, places=9)

    def test_disconnected_graph_outputs_are_deterministic(self):
        now = 1_710_000_000_000
        rows = [
            {"src": "a", "dst": "a", "ts": now, "weight": 0, "source_key": "a"},
            {"src": "b", "dst": "b", "ts": now, "weight": 0, "source_key": "b"},
            {"src": "c", "dst": "c", "ts": now, "weight": 0, "source_key": "c"},
            {"src": "d", "dst": "d", "ts": now, "weight": 0, "source_key": "d"},
            {"src": "a", "dst": "b", "ts": now, "weight": 2, "source_key": "a"},
            {"src": "b", "dst": "a", "ts": now, "weight": 2, "source_key": "b"},
            {"src": "c", "dst": "d", "ts": now, "weight": 1, "source_key": "c"},
            {"src": "d", "dst": "c", "ts": now, "weight": 1, "source_key": "d"},
        ]
        scores = compute_temporal_scores(rows, now_ts=now)
        keys = [item["source_key"] for item in scores]
        self.assertEqual(keys, ["a", "b", "c", "d"])
        self.assertTrue(math.isclose(sum(item["global_score"] for item in scores), 1.0, rel_tol=0, abs_tol=1e-9))
        self.assertAlmostEqual(scores[0]["global_score"], scores[1]["global_score"], places=9)
        self.assertAlmostEqual(scores[2]["global_score"], scores[3]["global_score"], places=9)

    def test_rolling_windows_drop_old_edges(self):
        now = 1_710_000_100_000
        old_ts = now - (40 * DAY_MS)
        recent_ts = now - (2 * DAY_MS)
        rows = [
            {"src": "a", "dst": "a", "ts": now, "weight": 0, "source_key": "a"},
            {"src": "b", "dst": "b", "ts": now, "weight": 0, "source_key": "b"},
            {"src": "c", "dst": "c", "ts": now, "weight": 0, "source_key": "c"},
            {"src": "a", "dst": "b", "ts": old_ts, "weight": 3, "source_key": "a"},
            {"src": "b", "dst": "a", "ts": old_ts, "weight": 3, "source_key": "b"},
            {"src": "b", "dst": "c", "ts": recent_ts, "weight": 2, "source_key": "b"},
            {"src": "c", "dst": "b", "ts": recent_ts, "weight": 2, "source_key": "c"},
        ]
        scores = {item["source_key"]: item for item in compute_temporal_scores(rows, now_ts=now)}
        self.assertGreater(scores["a"]["global_score"], 0.0)
        self.assertAlmostEqual(scores["a"]["recent_30d_score"], scores["a"]["recent_7d_score"], places=9)
        self.assertLess(scores["a"]["recent_30d_score"], scores["a"]["global_score"])
        self.assertGreater(scores["b"]["recent_7d_score"], scores["a"]["recent_7d_score"])


if __name__ == "__main__":
    unittest.main()
