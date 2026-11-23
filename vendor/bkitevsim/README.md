# BkSys - BkiTevSim Simulation Framework

**BkiTevSim - System Performance Evaluation Simulation for Course CO3007**

---

## Overview

BkitEvsim is a simulation framework designed for system performance evaluation and simulation. Built for HCMC University of Technology (VNU-HCM) Course CO3007, this framework provides both synchronous and coroutine-based simulation capabilities with multiple dispatcher options.

### Build Targets
```bash
# Build and run all examples
make examples

# Run coroutine network simulation
make examples-coro

# Run distributed sequential dispatcher app
make examples-distributed

# Run simple synchronous app (no asyncio)
make examples-sync
```

### Individual Example Commands
```bash
# Network simulation (coroutine-based)
python3 -m scenarios.coroutine_network_sim

# Distributed sequential dispatcher (coroutine)
python3 -m apps.distributed_sequential_app

# Synchronous sequential dispatcher (sync)
python3 -m apps.synchronous_sequential_app
```

## Usage Examples

### Creating a Synchronous LP
```python
from bkitevsim.core import Event, BkSysLogicalProcess, BkSysLogicalProcessState

class MyLP(BkSysLogicalProcess):
    def initialize(self):
        event = Event(timestamp=0.0, sender_id=self.lp_id, 
                     receiver_id=self.lp_id, event_type="COMPUTE", payload={"step": 0})
        self.send_event(event)
    
    def handle_event(self, event):
        self.local_virt_time = event.timestamp
        # Process event...
```

### Creating a Coroutine LP
```python
from bkitevsim.core import Event, BkSysCoLogicalProcess, BkSysLogicalProcessState

class MyCoLP(BkSysCoLogicalProcess):
    async def coroutine_run(self):
        event = Event(timestamp=0.0, sender_id=self.lp_id, 
                     receiver_id=self.lp_id, event_type="START", payload={})
        self.send_event(event)
        
        while self._event_loop and self._event_loop.current_time < self._event_loop.end_time:
            event = await self.wait_for("COMPUTE")
            # Process event...
            await self.advance_time(0.1)
```

## License

This project is licensed under the terms specified in [LICENSE](LICENSE) - educational use only for course CO3007 at HCMC University of Technology.

---

**Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM**

*BkitEvsim - System Performance Evaluation Simulation for Course CO3007*