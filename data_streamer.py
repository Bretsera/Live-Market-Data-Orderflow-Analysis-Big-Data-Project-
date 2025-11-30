import os
import upstox_client
from confluent_kafka import Producer
import json
import signal
import sys
import time
from datetime import datetime

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:29092')
UPSTOX_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN')
INSTRUMENT = os.getenv('INSTRUMENT', 'NSE_EQ|INE467B01029')

if not UPSTOX_TOKEN:
    print("❌ UPSTOX_ACCESS_TOKEN not set!")
    sys.exit(1)

# Confluent Kafka producer configuration
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': 'upstox-data-streamer'
}

producer = Producer(conf)

print(f"✓ Connected to Kafka: {KAFKA_BROKERS}")
print(f"✓ Streaming instrument: {INSTRUMENT}")

running = True


def delivery_report(err, msg):
    """Delivery callback for Kafka producer"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')


def on_message(message):
    """Callback when Upstox sends a message"""
    try:
        # Send to Kafka
        producer.produce(
            'upstox_ticks',
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report
        )
        producer.flush(timeout=1)
        print(f"✓ Tick sent to Kafka")
    except Exception as e:
        print(f"❌ Error sending to Kafka: {e}")


def cleanup(signal_num, frame):
    global running
    print("\n⚠️  Stopping streamer...")
    running = False
    try:
        producer.flush()
    except:
        pass
    sys.exit(0)


def main():
    """Main function to stream market data from Upstox"""
    global running
    
    try:
        print(f"\n{'='*60}")
        print("🚀 UPSTOX DATA STREAMER")
        print(f"{'='*60}")
        print(f"📡 Kafka Brokers: {KAFKA_BROKERS}")
        print(f"📊 Instrument: {INSTRUMENT}")
        print(f"🔑 Token: {UPSTOX_TOKEN[:20]}...")
        print(f"{'='*60}\n")
        
        # Configure Upstox client
        configuration = upstox_client.Configuration()
        configuration.access_token = UPSTOX_TOKEN
        api_client = upstox_client.ApiClient(configuration)
        
        # Create streamer - MarketDataStreamerV3 is the correct class
        streamer = upstox_client.MarketDataStreamerV3(
            api_client, 
            [INSTRUMENT], 
            "full"
        )
        
        # Handle connection opened
        def on_open():
            print("✓ Connected to Upstox WebSocket")
            print(f"✓ Subscribed to: {INSTRUMENT}")
            print("✓ Streaming to Kafka topic: upstox_ticks")
        
        # Register callbacks
        streamer.on("open", on_open)
        streamer.on("message", on_message)
        
        # Handle shutdown signals
        signal.signal(signal.SIGINT, cleanup)
        signal.signal(signal.SIGTERM, cleanup)
        
        print("🔌 Connecting to Upstox...")
        
        # Connect to Upstox WebSocket
        streamer.connect()
        
        print("\n✅ Streamer is LIVE! Waiting for market data...")
        print("Press Ctrl+C to stop\n")
        
        # Keep running
        signal.pause()
        
    except Exception as e:
        print(f"❌ Streamer error: {e}")
        import traceback
        traceback.print_exc()
        if running:
            time.sleep(5)
            main()  # Retry


if __name__ == '__main__':
    main()