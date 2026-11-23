"""Runner quét `configs/scenarios.json` → CSV D3 (F5): nạp được toàn bộ và chạy được thật."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from spe_sim.core.records import CSV_COLUMNS
from spe_sim.runner.study import load_scenarios, run_grid, scenario_configs


class TestScenarioLoading(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.data = load_scenarios()

    def test_every_entry_converts_to_a_valid_config(self) -> None:
        entries = scenario_configs(self.data, seeds=self.data["base"]["seeds"][:1])
        expected = len(self.data["scenarios"]) + len(self.data.get("sensitivity", []))
        self.assertEqual(len(entries), expected)
        for scenario_id, config in entries:
            self.assertTrue(scenario_id)
            self.assertLessEqual(config.p_pin, 0.20)
            self.assertLessEqual(config.p_uv, 0.30)
            self.assertLess(config.t_warmup, config.t_sim)

    def test_unknown_field_in_config_fails_loudly(self) -> None:
        broken = {
            "base": dict(self.data["base"], seeds=[1]),
            "scenarios": [dict(self.data["scenarios"][0], runtime_model="nope")],
        }
        with self.assertRaises(ValueError):
            scenario_configs(broken)


class TestRunGrid(unittest.TestCase):
    def test_writes_one_csv_per_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            written = run_grid(
                tmp,
                kernel="coroutine",
                replications=1,
                t_sim=2.0,
                limit=4,
            )
            self.assertEqual(len(written), 4)
            for path in written:
                self.assertTrue(path.exists())
                with path.open(encoding="utf-8", newline="") as handle:
                    header = next(csv.reader(handle))
                self.assertEqual(header, list(CSV_COLUMNS))

    def test_multiple_replications_share_the_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            # `limit` đếm theo (kịch bản × seed) ⇒ 2 = một kịch bản với 2 replication.
            written = run_grid(tmp, kernel="sequential", replications=2, t_sim=2.0, limit=2)
            self.assertEqual(len(written), 1)
            with written[0].open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len({row["seed"] for row in rows}), 2)


if __name__ == "__main__":
    unittest.main()
