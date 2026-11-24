"""T20 — kiểm `configs/scenarios.json` khớp hằng số trong `core/config.py`.

File JSON là hợp đồng bàn giao cho dev runner/thống kê/vẽ biểu đồ, nên nếu ai sửa hằng số mà
quên sinh lại file thì test này báo ngay (thay vì để lệch âm thầm giữa code và kịch bản).
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from spe_sim.core.config import (
    ADMISSION_CAPACITY_K,
    CPU_SCALE_LEVELS,
    HOL_LAMBDA_LEVELS,
    LAMBDA_LEVELS,
    SENSITIVITY_LAMBDA_LEVELS,
    LAMBDA_NOMINAL,
    N_DB_LEVELS,
    P_PIN_LEVELS,
    P_UV_LEVELS,
    RUNTIME_MODELS,
    R_REPLICATIONS,
    T_SIM_S,
    T_TIMEOUT_S,
    T_WARMUP_S,
)
from spe_sim.core.records import CSV_COLUMNS

CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "scenarios.json"


class TestScenariosConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    def test_base_matches_config_module(self) -> None:
        base = self.data["base"]
        self.assertEqual(base["t_sim"], T_SIM_S)
        self.assertEqual(base["t_warmup"], T_WARMUP_S)
        self.assertEqual(base["capacity_k"], ADMISSION_CAPACITY_K)
        self.assertEqual(base["t_timeout"], T_TIMEOUT_S)
        self.assertEqual(base["replications"], R_REPLICATIONS)
        self.assertEqual(len(base["seeds"]), R_REPLICATIONS)
        self.assertEqual(base["lambda_nominal"], LAMBDA_NOMINAL)

    def test_grid_matches_study_factors(self) -> None:
        grid = self.data["grid"]
        self.assertEqual(grid["runtime_models"], list(RUNTIME_MODELS))
        self.assertEqual(grid["join_modes"], ["gather", "as_completed"])
        self.assertEqual(grid["lambda_levels"], list(LAMBDA_LEVELS))
        self.assertEqual(grid["n_db_levels"], list(N_DB_LEVELS))
        self.assertEqual(grid["p_pin_levels"], list(P_PIN_LEVELS))
        self.assertEqual(grid["p_uv_levels"], list(P_UV_LEVELS))
        self.assertEqual(grid["sensitivity_lambda_levels"], list(SENSITIVITY_LAMBDA_LEVELS))
        self.assertEqual(grid["cpu_scale_levels"], list(CPU_SCALE_LEVELS))
        self.assertEqual(grid["hol_lambda_levels"], list(HOL_LAMBDA_LEVELS))
        self.assertEqual(self.data["base"]["cpu_dist"], "exp")

    def test_main_grid_is_runtime_times_join_times_lambda(self) -> None:
        scenarios = self.data["scenarios"]
        self.assertEqual(len(scenarios), len(RUNTIME_MODELS) * 2 * len(LAMBDA_LEVELS))
        self.assertEqual(len({s["id"] for s in scenarios}), len(scenarios))
        for scenario in scenarios:
            self.assertIn(scenario["runtime_model"], RUNTIME_MODELS)
            self.assertIn(scenario["join_mode"], ("gather", "as_completed"))
            self.assertIn(scenario["lam"], LAMBDA_LEVELS)

    def test_sensitivity_covers_all_factors(self) -> None:
        factors = {s["factor"] for s in self.data["sensitivity"]}
        self.assertEqual(factors, {"n_db", "p_pin", "p_uv", "cpu_scale"})
        ids = [s["id"] for s in self.data["scenarios"] + self.data["sensitivity"]]
        self.assertEqual(len(ids), len(set(ids)))
        for scenario in self.data["sensitivity"]:
            factor = scenario["factor"]
            if factor == "p_pin":
                self.assertEqual(scenario["runtime_model"], "spring_loom_vt")
                self.assertIn(scenario["p_pin"], P_PIN_LEVELS)
            if factor in ("p_uv", "cpu_scale"):
                self.assertIn(scenario["runtime_model"], ("nodejs_event_loop", "bun_event_loop"))
                self.assertIn(scenario["p_uv"], P_UV_LEVELS)
            if factor == "cpu_scale":
                self.assertIn(scenario["cpu_scale"], CPU_SCALE_LEVELS)
                self.assertIn(scenario["lam"], HOL_LAMBDA_LEVELS)
            else:
                self.assertIn(scenario["lam"], SENSITIVITY_LAMBDA_LEVELS)

    def test_sensitivity_reaches_high_load(self) -> None:
        """B1: p_pin và n_db phải có mức λ ≥ 200 (spec §2.1.3)."""
        for factor in ("n_db", "p_pin", "p_uv"):
            lams = {s["lam"] for s in self.data["sensitivity"] if s["factor"] == factor}
            self.assertEqual(lams, set(SENSITIVITY_LAMBDA_LEVELS), factor)

    def test_blocking_as_completed_marked_control(self) -> None:
        """B3: Blocking × as_completed là kịch bản đối chứng."""
        for scenario in self.data["scenarios"]:
            is_control = (
                scenario["runtime_model"] == "blocking_baseline"
                and scenario["join_mode"] == "as_completed"
            )
            self.assertEqual(scenario.get("role") == "control", is_control, scenario["id"])

    def test_counts_are_consistent(self) -> None:
        counts = self.data["counts"]
        self.assertEqual(counts["main"], len(self.data["scenarios"]))
        self.assertEqual(counts["sensitivity"], len(self.data["sensitivity"]))
        self.assertEqual(
            counts["main_control"],
            sum(1 for s in self.data["scenarios"] if s.get("role") == "control"),
        )
        total = counts["main"] + counts["sensitivity"]
        self.assertEqual(counts["runs_total"], total * R_REPLICATIONS)

    def test_csv_schema_matches_D3(self) -> None:
        self.assertEqual(
            self.data["metadata"]["csv_schema"], ",".join(CSV_COLUMNS)
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
