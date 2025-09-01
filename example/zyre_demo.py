
from bamboo import Bamboo
from bamboo.envelope import Envelope

def main():
    # Starts two peers using Zyre 
    # Run this in separate terminals or alongside another node
    A = Bamboo("PeerA", transport="zyre", codec="json", keys=["perf.echo", "topic.note"])
    
    # If you run B in a separate process, register actual handlers there.
    # Here, if only one node is running, A won't find B

    # Example usage if a peer named "PeerB" is present:
    try:
        resp = A.request(dest_peer="PeerB", key="perf.echo", body={"msg": "hello over Zyre"})
        print("RESP from PeerB:", resp.body)
    except Exception as e:
        print("No PeerB response (expected if only one node):", e)

    # Group publish
    try:
        A.transport.subscribe("demo")
        A.publish("demo", key="topic.note", body={"note": "hello group"})
    except Exception as e:
        print("Publish failed (no subscribers yet):", e)

    import time
    time.sleep(0.5)
    A.stop()
    try:
        A.transport.close()
    except Exception:
        pass

if __name__ == "__main__":
    main()
