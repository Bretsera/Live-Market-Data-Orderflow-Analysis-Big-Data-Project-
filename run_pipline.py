import os
import subprocess
import time
import sys
from datetime import datetime

def log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")

def is_market_open():
    """Check if market is open (9:15 AM - 3:30 PM IST, Mon-Fri)"""
    from datetime import datetime, time
    import pytz
    
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    # Market hours: 9:15 AM to 3:30 PM
    market_open = time(9, 15)
    market_close = time(15, 30)
    
    # Check if weekday (0-4 = Mon-Fri)
    if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False, "Market closed (weekend)"
    
    if now.time() < market_open:
        return False, f"Market not yet open (opens at 9:15 AM, now {now.strftime('%H:%M')})"
    
    if now.time() > market_close:
        return False, f"Market closed (closes at 3:30 PM, now {now.strftime('%H:%M')})"
    
    return True, "Market is open"

def main():
    log("=" * 60)
    log("🚀 TRADING DATA PIPELINE - DOCKER ORCHESTRATOR")
    log("=" * 60)
    
    # Check environment setup
    log("\n📋 Checking environment setup...")
    
    if not os.path.exists('.env'):
        log("❌ .env file not found! Creating template...")
        with open('.env', 'w') as f:
            f.write("""# Upstox API credentials
UPSTOX_ACCESS_TOKEN=your_token_here
INSTRUMENT=NSE_EQ|INE467B01029

# Service URLs
KAFKA_BROKERS=kafka:29092
NAMENODE_URL=hdfs://namenode:9000
SPARK_MASTER_URL=spark://spark-master:7077
""")
        log("⚠️  Please edit .env file with your Upstox token and try again")
        sys.exit(1)
    
    log("✓ .env file found")
    
    # Load environment
    from dotenv import load_dotenv
    load_dotenv()
    
    upstox_token = os.getenv('UPSTOX_ACCESS_TOKEN')
    if not upstox_token or upstox_token == 'your_token_here':
        log("❌ UPSTOX_ACCESS_TOKEN not configured in .env")
        sys.exit(1)
    
    log(f"✓ Upstox token configured")
    
    # Check market
    log("\n🕐 Checking market status...")
    is_open, status = is_market_open()
    log(f"   {status}")
    
    if not is_open:
        log("\n⚠️  Market is currently closed. Pipeline will wait...")
        log("   Containers will be ready when market opens.")
    
    # Start Docker containers
    log("\n🐳 Starting Docker containers...")
    
    try:
        result = subprocess.run(
            ["docker-compose", "up", "-d"],
            cwd=os.getcwd(),
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            log(f"❌ Failed to start containers: {result.stderr}")
            sys.exit(1)
        
        log("✓ Docker containers started")
        
    except FileNotFoundError:
        log("❌ docker-compose not found. Please install Docker and Docker Compose")
        sys.exit(1)
    
    # Wait for services to be ready
    log("\n⏳ Waiting for services to be ready...")
    time.sleep(15)
    
    # Check container health
    log("\n✓ Checking container status...")
    result = subprocess.run(
        ["docker-compose", "ps"],
        cwd=os.getcwd(),
        capture_output=True,
        text=True
    )
    log(result.stdout)
    
    # Display access URLs
    log("\n" + "=" * 60)
    log("📊 SERVICE URLs:")
    log("=" * 60)
    log("\n🌐 Dashboards:")
    log("   • Candlestick Chart:  http://localhost:8050")
    log("   • Hadoop NameNode:    http://localhost:50070")
    log("   • Spark Master:       http://localhost:8080")
    
    log("\n📡 Kafka:")
    log("   • Brokers: localhost:9092")
    log("   • Topics: upstox_ticks, upstox_orderflow")
    
    log("\n💾 HDFS Storage:")
    log("   • NameNode: hdfs://localhost:9000")
    log("   • Ticks:    /trading/ticks/")
    log("   • Candles:  /trading/candles/")
    
    log("\n" + "=" * 60)
    log("✅ PIPELINE READY")
    log("=" * 60)
    
    log("\n📌 Next Steps:")
    log("   1. Open http://localhost:8050 in your browser")
    log("   2. Wait for market to open (9:15 AM)")
    log("   3. Data will automatically start flowing")
    log("   4. To stop: docker-compose down")
    
    log("\n📊 Pipeline Status:")
    log("   • Data Streamer: Will start when market opens")
    log("   • Spark Processor: Running (waiting for data)")
    log("   • Dashboard: Listening on port 8050")
    log("   • HDFS Storage: Ready to receive data")
    
    log("\n💡 Monitoring Commands:")
    log("   • docker-compose logs -f data-streamer")
    log("   • docker-compose logs -f processor")
    log("   • docker-compose logs -f plotter")
    log("   • docker ps")

if __name__ == '__main__':
    main()

