"""Tests for Transport."""

import asyncio
from swarmconsensus.transport import InProcessTransport

def test_in_process_transport():
    """Test in-process transport."""
    async def run():
        InProcessTransport.clear()
        
        t1 = InProcessTransport("n1")
        t2 = InProcessTransport("n2")
        
        await t1.send("n2", {"msg": "hello"})
        
        received = await t2.receive()
        assert received == {"msg": "hello"}
        
    asyncio.run(run())
