import os
from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import threading
from collections import deque
from datetime import datetime


# Global variables to store candle data
candles_buffer = deque(maxlen=100)  # Store last 100 candles
data_lock = threading.Lock()


# Kafka Configuration
# Use environment variables
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
KAFKA_TOPIC = 'upstox_orderflow'


class KafkaConsumerThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        
        # Confluent Kafka configuration
        conf = {
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': 'candlestick-dashboard',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        }
        
        self.consumer = Consumer(conf)
        self.consumer.subscribe([KAFKA_TOPIC])
        self.running = True
        print(f"✓ Kafka consumer initialized for topic: {KAFKA_TOPIC} at {KAFKA_BROKERS}")
    
    def run(self):
        """Consume messages and store in buffer"""
        print("✓ Starting Kafka consumer thread...")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Kafka error: {msg.error()}")
                    continue
                
                try:
                    candle = json.loads(msg.value().decode('utf-8'))
                    
                    # Process candle data
                    processed_candle = {
                        'timestamp': pd.to_datetime(candle['window_start']),
                        'window_end': pd.to_datetime(candle['window_end']),
                        'instrument': candle.get('instrument', 'Unknown'),
                        'open': float(candle['open']),
                        'high': float(candle['high']),
                        'low': float(candle['low']),
                        'close': float(candle['close']),
                        'buy_volume': int(candle['buy_volume']),
                        'sell_volume': int(candle['sell_volume']),
                        'total_volume': int(candle['total_volume']),
                        'delta': int(candle['delta']),
                        'tbq': float(candle.get('tbq', 0)) if candle.get('tbq') else 0,
                        'tsq': float(candle.get('tsq', 0)) if candle.get('tsq') else 0
                    }
                    
                    # Thread-safe append to buffer
                    with data_lock:
                        candles_buffer.append(processed_candle)
                    
                    print(f"✓ Candle received: {processed_candle['instrument']} | "
                          f"O: {processed_candle['open']:.2f} | "
                          f"C: {processed_candle['close']:.2f} | "
                          f"H: {processed_candle['high']:.2f} | "
                          f"L: {processed_candle['low']:.2f} | "
                          f"Vol: {processed_candle['total_volume']}")
                
                except Exception as e:
                    print(f"Error processing message: {e}")
        
        except Exception as e:
            print(f"Kafka consumer error: {e}")
        finally:
            self.consumer.close()
            print("✓ Kafka consumer closed")
    
    def stop(self):
        """Stop the consumer thread"""
        self.running = False



def create_candlestick_figure(candles_list):
    """Create Plotly candlestick chart with volume subplot"""
    
    if not candles_list:
        # Return empty figure if no data
        fig = go.Figure()
        fig.add_annotation(
            text="⏳ Waiting for candle data from Kafka...",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=20, color="#888")
        )
        fig.update_layout(
            title="Real-Time Candlestick Dashboard",
            template='plotly_dark',
            height=800,
            paper_bgcolor='#0e0e0e',
            plot_bgcolor='#1e1e1e'
        )
        return fig
    
    # Convert to DataFrame
    df = pd.DataFrame(candles_list)
    
    # Create subplots
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=('📈 Price (OHLC)', '📊 Volume', '💹 Order Flow Delta'),
        row_heights=[0.5, 0.25, 0.25]
    )
    
    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df['timestamp'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='Price',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350',
            increasing_fillcolor='#26a69a',
            decreasing_fillcolor='#ef5350',
            hoverinfo='x+y'
        ),
        row=1, col=1
    )
    
    # Volume bars (colored by buy/sell)
    colors = []
    for i in range(len(df)):
        if df['buy_volume'].iloc[i] > df['sell_volume'].iloc[i]:
            colors.append('#26a69a')  # Green (buy dominant)
        else:
            colors.append('#ef5350')  # Red (sell dominant)
    
    fig.add_trace(
        go.Bar(
            x=df['timestamp'],
            y=df['total_volume'],
            name='Total Volume',
            marker_color=colors,
            showlegend=False,
            hovertemplate='<b>Volume:</b> %{y:,.0f}<extra></extra>'
        ),
        row=2, col=1
    )
    
    # Delta (Buy volume - Sell volume) with colors
    delta_colors = ['#26a69a' if d >= 0 else '#ef5350' for d in df['delta']]
    
    fig.add_trace(
        go.Bar(
            x=df['timestamp'],
            y=df['delta'],
            name='Delta',
            marker_color=delta_colors,
            showlegend=False,
            hovertemplate='<b>Delta:</b> %{y:+,.0f}<extra></extra>'
        ),
        row=3, col=1
    )
    
    # Get instrument name and stats
    instrument_name = df['instrument'].iloc[-1] if not df.empty else 'Unknown'
    latest_price = df['close'].iloc[-1] if not df.empty else 0
    latest_open = df['open'].iloc[0] if not df.empty else 0
    price_change = latest_price - latest_open
    price_change_pct = (price_change / latest_open * 100) if latest_open != 0 else 0
    
    # Update layout
    fig.update_layout(
        title=f'📊 Real-Time Dashboard - {instrument_name} | Price: ₹{latest_price:.2f} '
              f'({price_change:+.2f} / {price_change_pct:+.2f}%)',
        template='plotly_dark',
        height=800,
        hovermode='x unified',
        xaxis_rangeslider_visible=False,
        showlegend=True,
        paper_bgcolor='#0e0e0e',
        plot_bgcolor='#1e1e1e',
        font=dict(family='Arial', size=12, color='#fff')
    )
    
    # Update axes
    fig.update_yaxes(title_text="<b>Price (₹)</b>", row=1, col=1)
    fig.update_yaxes(title_text="<b>Volume</b>", row=2, col=1)
    fig.update_yaxes(title_text="<b>Delta</b>", row=3, col=1)
    fig.update_xaxes(title_text="<b>Time</b>", row=3, col=1)
    
    return fig



# Initialize Dash app
app = Dash(__name__)
app.title = "Real-Time Candlestick Dashboard"


# App layout
app.layout = html.Div([
    html.Div([
        html.H1("📊 Real-Time Candlestick Dashboard", 
                style={'textAlign': 'center', 'color': '#26a69a', 'marginBottom': 10}),
        html.P(f"🔗 Kafka Topic: {KAFKA_TOPIC} | 🏠 Broker: {KAFKA_BROKERS}", 
               style={'textAlign': 'center', 'color': '#888', 'marginTop': 0, 'fontSize': 12}),
    ]),
    
    html.Div([
        html.Div(id='stats-display', style={
            'textAlign': 'center', 
            'padding': '15px',
            'backgroundColor': '#1e1e1e',
            'marginBottom': '20px',
            'borderRadius': '8px',
            'border': '1px solid #26a69a'
        })
    ]),
    
    dcc.Graph(id='live-candlestick-chart', style={'height': '800px'}),
    
    dcc.Interval(
        id='interval-component',
        interval=2*1000,  # Update every 2 seconds
        n_intervals=0
    )
], style={'backgroundColor': '#0e0e0e', 'padding': '20px', 'fontFamily': 'Arial'})



@app.callback(
    [Output('live-candlestick-chart', 'figure'),
     Output('stats-display', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_chart(n):
    """Update candlestick chart periodically"""
    
    with data_lock:
        candles_list = list(candles_buffer)
    
    # Create figure
    fig = create_candlestick_figure(candles_list)
    
    # Stats display
    if candles_list:
        latest = candles_list[-1]
        highest = max([c['high'] for c in candles_list])
        lowest = min([c['low'] for c in candles_list])
        total_buy = sum([c['buy_volume'] for c in candles_list])
        total_sell = sum([c['sell_volume'] for c in candles_list])
        total_delta = sum([c['delta'] for c in candles_list])
        
        buy_sell_ratio = total_buy / total_sell if total_sell > 0 else 0
        
        stats = html.Div([
            html.Span(f"📈 Candles: {len(candles_list)}", 
                     style={'margin': '0 15px', 'color': '#fff', 'fontWeight': 'bold'}),
            html.Span(f"🕐 {latest['timestamp'].strftime('%H:%M:%S')}", 
                     style={'margin': '0 15px', 'color': '#26a69a'}),
            html.Span(f"💹 O: ₹{latest['open']:.2f} | H: ₹{highest:.2f} | L: ₹{lowest:.2f} | C: ₹{latest['close']:.2f}", 
                     style={'margin': '0 15px', 'color': '#fff'}),
            html.Span(f"📊 Buy: {total_buy:,} | Sell: {total_sell:,} | Ratio: {buy_sell_ratio:.2f}x", 
                     style={'margin': '0 15px', 'color': '#26a69a' if total_buy > total_sell else '#ef5350'}),
            html.Span(f"💹 Total Delta: {total_delta:+,}", 
                     style={'margin': '0 15px', 'color': '#26a69a' if total_delta >= 0 else '#ef5350', 'fontWeight': 'bold'}),
        ])
    else:
        stats = html.P("⏳ Waiting for candle data from Kafka...", 
                      style={'color': '#888', 'fontSize': 14})
    
    return fig, stats



if __name__ == '__main__':
    print("\n" + "="*70)
    print("🚀 STARTING REAL-TIME CANDLESTICK DASHBOARD")
    print("="*70)
    print(f"\n📡 Kafka Configuration:")
    print(f"   • Brokers: {KAFKA_BROKERS}")
    print(f"   • Topic: {KAFKA_TOPIC}")
    print(f"   • Consumer Group: candlestick-dashboard")
    print(f"\n🌐 Dashboard Access:")
    print(f"   • URL: http://127.0.0.1:8050")
    print(f"   • Open this URL in your browser")
    print(f"\n⏳ Waiting for market data...")
    print("="*70 + "\n")
    
    # Start Kafka consumer in background thread
    kafka_thread = KafkaConsumerThread()
    kafka_thread.start()
    
    try:
        # Run Dash app
        app.run(debug=False, host='0.0.0.0', port=8050)
    except KeyboardInterrupt:
        print("\n\n⚠️  Shutting down dashboard...")
        kafka_thread.stop()
        print("✓ Dashboard stopped")
        print("="*70)