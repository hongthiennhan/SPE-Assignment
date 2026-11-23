# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM
#
# BkitTEvSim release
# Source Code License Grant: The authors hereby grant to Licensee
# personal permission to use and modify the Licensed Source Code
# for the sole purpose of studying while attending the course CO3007.
#
# Simulation module for BkitTEvSim Simulation Framework
#
"""
Simulation class.
"""
from __future__ import annotations
from typing import Dict, List, Optional, Any, Type, Callable
import logging

from .event import Event
from .logical_process import BkSysLogicalProcess, BkSysLogicalProcessState

logger = logging.getLogger(__name__)


class SimulationOptions:
    """Configuration options for a PDES simulation.
    
    Attributes:
        end_time: Simulation end time.
        dispatcher: Dispatcher type ("sequential", "distributed_sequential", or auto for coroutine LPs).
        bucket_width: Bucket width for priority queue.
        log_level: Logging level.
        log_file: Optional log file path.
        stats_file: Optional stats file path.
        verbose: Enable verbose logging.
        model_params: Additional model parameters.
    """
    def __init__(
        self,
        end_time: float = 100.0,
        dispatcher: str = "sequential",
        bucket_width: float = 1.0,
        log_level: str = "INFO",
        log_file: Optional[str] = None,
        stats_file: Optional[str] = None,
        verbose: bool = False,
        model_params: Dict[str, Any] = None,
    ):
        """Initialize SimulationOptions.
        
        Args:
            end_time: Simulation end time. Defaults to 100.0.
            dispatcher: Dispatcher type. Defaults to "sequential".
            bucket_width: Bucket width for priority queue. Defaults to 1.0.
            log_level: Logging level. Defaults to "INFO".
            log_file: Optional log file path. Defaults to None.
            stats_file: Optional stats file path. Defaults to None.
            verbose: Enable verbose logging. Defaults to False.
            model_params: Additional model parameters. Defaults to None.
        """
        self.end_time = end_time
        self.dispatcher = dispatcher
        self.bucket_width = bucket_width
        self.log_level = log_level
        self.log_file = log_file
        self.stats_file = stats_file
        self.verbose = verbose
        self.model_params = model_params or {}


class Simulation:
    """
    High-level simulation controller.
    
    Provides a simple interface for creating and running simulations
    without directly managing dispatchers and LPs.
    
    Attributes:
        options: Simulation configuration options.
        dispatcher: The active dispatcher instance.
        lps: Dictionary mapping LP IDs to LogicalProcess instances.
    """
    
    def __init__(
        self,
        options: SimulationOptions = None,
        models: Dict[str, Type[BkSysLogicalProcess]] = None,
        factories: Dict[str, Callable] = None
    ):
        """Initialize a Simulation.
        
        Args:
            options: Simulation configuration options. If None, defaults are used.
            models: Dictionary mapping model names to BkSysLogicalProcess classes.
            factories: Optional dictionary mapping model names to factory functions.
        """
        self.options = options or SimulationOptions()
        self.dispatcher = None
        self.lps: Dict[int, BkSysLogicalProcess] = {}
        self._model_factories: Dict[str, Type[BkSysLogicalProcess]] = {}
        
        # Register models if provided
        if models:
            for name, lp_class in models.items():
                factory = factories.get(name) if factories else None
                self._model_factories[name] = lp_class
                self._model_factories[name + "_factory"] = factory
                logger.debug(f"Registered model '{name}' in simulation")
    
    def create_lp(self, lp_id: int, model_name: str, **params) -> LogicalProcess:
        """Create an LP from a registered model.
        
        Args:
            lp_id: Unique identifier for the LP.
            model_name: Name of the registered model.
            **params: Additional parameters passed to the model constructor.
            
        Returns:
            The created LogicalProcess instance.
            
        Raises:
            ValueError: If model_name is not registered.
        """
        if model_name not in self._model_factories:
            raise ValueError(f"Model '{model_name}' not registered. Available: {list(self._model_factories.keys())}")
        
        lp_class = self._model_factories[model_name]
        factory = self._model_factories.get(model_name + "_factory")
        
        # Filter out internal parameters that shouldn't be passed to the model
        # Keep model-specific params like num_lps which are needed by some factories
        filtered_params = {k: v for k, v in params.items()
                          if k not in ["model", "end_time", "dispatcher"]}
        # Also ensure num_lps is passed if available (needed by some factory functions)
        if "num_lps" in params:
            filtered_params["num_lps"] = params["num_lps"]
        
        if factory:
            lp = factory(lp_id=lp_id, **filtered_params)
        else:
            lp = lp_class(lp_id=lp_id, **filtered_params)
        
        self.lps[lp_id] = lp
        return lp
    
    def create_lps(self, model_name: str, count: int, **params) -> List[LogicalProcess]:
        """Create multiple LPs of the same model.
        
        Args:
            model_name: Name of the registered model.
            count: Number of LPs to create.
            **params: Additional parameters passed to each model constructor.
            
        Returns:
            List of created LogicalProcess instances.
        """
        lps = []
        for i in range(count):
            lp = self.create_lp(i, model_name, **params)
            lps.append(lp)
        return lps
    
    def _create_dispatcher(self):
        """Create the dispatcher based on options.
        
        Automatically detects if any LP is a BkSysCoLogicalProcess and
        uses BkSysCoEventLoop in that case. Otherwise uses the dispatcher
        type specified in options.
        """
        # Local import to avoid circular dependency
        from bkitevsim.dispatchers import SequentialEventDispatcher, DistributedEventDispatcher
        from bkitevsim.core.logical_coProcess import BkSysCoEventLoop, BkSysCoLogicalProcess
        
        dispatcher_type = self.options.dispatcher.lower()
        
        # Check if any LP is a BkSysCoLogicalProcess - if so, use BkSysCoEventLoop
        has_coroutine_lps = any(isinstance(lp, BkSysCoLogicalProcess) for lp in self.lps.values())
        
        if has_coroutine_lps:
            # Use BkSysCoEventLoop for coroutine LPs
            self.dispatcher = BkSysCoEventLoop(
                end_time=self.options.end_time,
                bucket_width=self.options.bucket_width
            )
        elif dispatcher_type == "sequential":
            self.dispatcher = SequentialEventDispatcher(
                end_time=self.options.end_time,
                bucket_width=self.options.bucket_width
            )
        elif dispatcher_type == "distributed_sequential":
            self.dispatcher = DistributedEventDispatcher(
                end_time=self.options.end_time,
                bucket_width=self.options.bucket_width
            )
        else:
            raise ValueError(f"Unknown dispatcher type: {dispatcher_type}")
    
    def run(self) -> Dict[str, Any]:
        """Run the simulation.
        
        Creates dispatcher if needed, registers all LPs, and runs the simulation.
        
        Returns:
            Dictionary with simulation statistics including events_processed and wall_time.
        """
        if not self.lps:
            logger.warning("No LPs registered, nothing to simulate")
            return {"events_processed": 0, "wall_time": 0.0}
        
        # Create dispatcher if not already created
        if self.dispatcher is None:
            self._create_dispatcher()
        
        # Check if dispatcher is BkSysCoEventLoop
        from bkitevsim.core.logical_coProcess import BkSysCoEventLoop
        is_coroutine_dispatcher = isinstance(self.dispatcher, BkSysCoEventLoop)
        
        # Register all LPs with dispatcher
        for lp in self.lps.values():
            self.dispatcher.register_lp(lp)
            # For coroutine dispatcher, also set the event loop reference on LP
            if is_coroutine_dispatcher and hasattr(lp, 'set_event_loop'):
                lp.set_event_loop(self.dispatcher)
        
        # Run simulation
        logger.info(f"Starting simulation with {len(self.lps)} LPs using {self.options.dispatcher} dispatcher")
        stats = self.dispatcher.run()
        
        return stats
    
    def get_lp(self, lp_id: int) -> Optional[BkSysLogicalProcess]:
        """Get LP by ID.
        
        Args:
            lp_id: LP identifier.
            
        Returns:
            The BkSysLogicalProcess instance or None if not found.
        """
        return self.lps.get(lp_id)
    
    def get_all_lps(self) -> List[BkSysLogicalProcess]:
        """Get all LPs.
        
        Returns:
            List of all BkSysLogicalProcess instances.
        """
        return list(self.lps.values())
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get combined statistics from all LPs.
        
        Returns:
            Dictionary with dispatcher stats and per-LP statistics.
        """
        stats = {}
        if self.dispatcher:
            stats = self.dispatcher.stats  # Use stats attribute directly
        
        lp_stats = {}
        for lp_id, lp in self.lps.items():
            if hasattr(lp, 'get_statistics'):
                lp_stats[lp_id] = lp.get_statistics()
            else:
                lp_stats[lp_id] = {
                    "lp_id": lp_id,
                    "local_virt_time": lp.local_virt_time,
                    "state_keys": list(lp.state.data.keys())
                }
        
        stats["lp_statistics"] = lp_stats
        return stats
