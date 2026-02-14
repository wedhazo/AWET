#!/usr/bin/env python3
"""Mock AWET agent to demonstrate Prometheus metrics."""

import time
import random
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Create metrics (same as real AWET agents would have)
events_processed = Counter('events_processed_total', 'Total events processed', ['agent', 'event_type'])
event_latency = Histogram('event_latency_seconds', 'Event processing latency', ['agent'])
risk_decisions = Counter('risk_decisions_total', 'Risk decisions made', ['agent', 'decision'])
prediction_timestamp = Gauge('prediction_last_timestamp_seconds', 'Last prediction timestamp', ['agent'])
model_version = Gauge('model_version_info', 'Model version info', ['agent', 'model_version'])

def simulate_data_ingestion():
    """Simulate data ingestion agent."""
    while True:
        # Simulate processing market data
        events_processed.labels(agent='data_ingestion', event_type='market_data').inc()
        with event_latency.labels(agent='data_ingestion').time():
            time.sleep(random.uniform(0.01, 0.1))  # Simulate processing time
        time.sleep(1)

def simulate_prediction():
    """Simulate prediction agent."""
    while True:
        # Simulate ML predictions
        events_processed.labels(agent='prediction', event_type='prediction_generated').inc()
        with event_latency.labels(agent='prediction').time():
            time.sleep(random.uniform(0.1, 0.5))  # ML takes longer
        
        prediction_timestamp.labels(agent='prediction').set_to_current_time()
        model_version.labels(agent='prediction', model_version='v1.2.3').set(1)
        time.sleep(2)

def simulate_risk_agent():
    """Simulate risk agent."""
    while True:
        # Simulate risk decisions
        decision = random.choice(['approved', 'rejected'])
        risk_decisions.labels(agent='risk', decision=decision).inc()
        events_processed.labels(agent='risk', event_type='risk_decision').inc()
        
        with event_latency.labels(agent='risk').time():
            time.sleep(random.uniform(0.02, 0.15))
        time.sleep(1.5)

def main():
    """Start the mock agent with metrics server."""
    import threading
    
    # Start Prometheus metrics server
    start_http_server(8001)
    print("🚀 Mock AWET Agent started!")
    print("📊 Metrics available at: http://localhost:8001/metrics")
    print("🔥 Generating sample trading pipeline metrics...")
    
    # Start background threads to simulate different agents
    threading.Thread(target=simulate_data_ingestion, daemon=True).start()
    threading.Thread(target=simulate_prediction, daemon=True).start()
    threading.Thread(target=simulate_risk_agent, daemon=True).start()
    
    # Keep main thread alive
    try:
        counter = 0
        while True:
            counter += 1
            print(f"⚡ Mock agent running... (cycle: {counter})")
            time.sleep(10)
    except KeyboardInterrupt:
        print("🛑 Mock agent stopped")

if __name__ == "__main__":
    main()