import os
import sys
import time
import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
import yaml
from datetime import datetime

# ----------------------
# Load Cassandra config
# ----------------------
def load_config(config_file):
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'configuration',
        config_file
    )
    with open(config_path) as f:
        return yaml.safe_load(f)

try:
    cassandra_config = load_config('cassandra.yml')
    print("[CONFIG] Successfully loaded Cassandra configuration")
except Exception as e:
    print(f"[CONFIG ERROR] Failed to load config: {e}")
    sys.exit(1)

host = cassandra_config['HOST']
keyspace = cassandra_config['KEYSPACE']
table = cassandra_config['TABLE']
username = cassandra_config['USERNAME']
password = cassandra_config['PASSWORD']

# ----------------------
# Cassandra connection
# ----------------------
cluster = Cluster([host])
session = cluster.connect(keyspace)

# ----------------------
# Helper function
# ----------------------
def _darken_color(color):
    # Simple function to darken a hex color
    if color.startswith('#'):
        color = color[1:]
    r, g, b = int(color[0:2], 16), int(color[2:4], 16), int(color[4:6], 16)
    r = max(0, r - 40)
    g = max(0, g - 40)
    b = max(0, b - 40)
    return f"#{r:02x}{g:02x}{b:02x}"

# ----------------------
# Streamlit UI
# ----------------------
st.set_page_config(
    page_title="📰 Real-Time News", 
    layout="wide"
)

# Custom CSS for styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.8rem;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 1rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .subheader { font-size: 1.2rem; color: #7f8c8d; text-align: center; margin-bottom: 2rem; }
    .news-card { border-radius: 12px; box-shadow: 0 6px 16px rgba(0,0,0,0.08); padding: 22px; margin-bottom: 24px; transition: all 0.3s ease; background: linear-gradient(145deg, #ffffff, #f8f9fa); border-left: 6px solid; border-top: 1px solid #eaeaea; }
    .news-card:hover { transform: translateY(-3px); box-shadow: 0 12px 24px rgba(0,0,0,0.12); }
    .category-tag { padding: 6px 14px; border-radius: 20px; font-size: 0.85rem; font-weight: 600; margin-bottom: 12px; display: inline-block; color: white; text-shadow: 0 1px 2px rgba(0,0,0,0.2); }
    .timestamp { font-size: 0.85rem; color: #95a5a6; display: flex; align-items: center; gap: 6px; }
    .source-badge { background: linear-gradient(135deg, #ecf0f1, #bdc3c7); padding: 4px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; color: #2c3e50; margin-right: 8px; }
    .read-button { background: linear-gradient(135deg, #3498db, #2980b9); color: white; border: none; padding: 10px 20px; border-radius: 8px; cursor: pointer; font-weight: 600; font-size: 0.9rem; transition: all 0.2s ease; text-decoration: none; display: inline-block; }
    .read-button:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(52, 152, 219, 0.3); color: white; text-decoration: none; }
    .refresh-button { background: linear-gradient(135deg, #2ecc71, #27ae60); color: white; border: none; padding: 12px 24px; border-radius: 10px; cursor: pointer; font-weight: 600; font-size: 1rem; transition: all 0.2s ease; width: 100%; margin-bottom: 20px; }
    .refresh-button:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(46, 204, 113, 0.3); }
    .stSelectbox > div > div { border-radius: 10px; padding: 10px; border: 2px solid #e0e0e0; font-size: 1rem; }
    .stSelectbox > div > div:hover { border-color: #3498db; }
    .footer { text-align: center; color: #7f8c8d; margin-top: 40px; padding: 20px; border-top: 1px solid #ecf0f1; }
    </style>
""", unsafe_allow_html=True)

# Initialize session state for auto-refresh
if 'last_update' not in st.session_state:
    st.session_state.last_update = time.time()

# Header
st.markdown('<h1 class="main-header">📰 Real-Time News Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p class="subheader">Stay updated with the latest news in real-time! </p>', unsafe_allow_html=True)

# Fetch distinct categories
rows = session.execute(f"SELECT DISTINCT category FROM {table}")
categories = [row.category for row in rows]

# Layout columns
col1, col2 = st.columns([1, 3])

with col1:
    selected_category = st.selectbox("Select Category", categories, key="category_select")
    
    if st.button("🔄 Refresh Now", use_container_width=True, key="refresh_btn"):
        st.session_state.last_update = 0
        st.rerun()

with col2:
    current_time = time.time()
    if current_time - st.session_state.last_update > 10:  # auto-refresh every 10s
        st.session_state.last_update = current_time
        st.rerun()

    # Fetch latest news
    try:
        query = f"""
            SELECT category, publishedat, title, description, source, url
            FROM {table}
            WHERE category = %s
            LIMIT 20
        """
        rows = session.execute(query, [selected_category])
        df = pd.DataFrame(list(rows))

        # Deduplicate by URL
        df = df.drop_duplicates(subset=['url'])
        
        # Sort by published date descending
        df = df.sort_values(by='publishedat', ascending=False).head(20)

    except Exception as e:
        st.error(f"Error fetching data: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("📭 No news available for this category.")
    else:
        # Format timestamp
        df['publishedat'] = pd.to_datetime(df['publishedat'])
        df['time_ago'] = df['publishedat'].apply(lambda x: 
            f"{int((datetime.now() - x).total_seconds() // 3600)}h {int((datetime.now() - x).total_seconds() % 3600 // 60)}m ago")
        
        # Category colors
        category_colors = {
            "health": "#1abc9c",
            "sports": "#e74c3c",
            "politics": "#3498db",
            "tech": "#9b59b6",
            "entertainment": "#f39c12",
            "business": "#2ecc71",
            "science": "#e67e22",
            "world": "#34495e",
            "finance": "#16a085",
            "education": "#8e44ad"
        }
        
        # Display news cards
        for i in range(len(df)):
            row = df.iloc[i]
            color = category_colors.get(row['category'].lower(), "#95a5a6")
            
            st.markdown(
                f"""
                <div class="news-card" style="border-left-color: {color};">
                    <div class="category-tag" style="background: linear-gradient(135deg, {color}, {_darken_color(color)});">
                        {row['category'].upper()}
                    </div>
                    <h3 style="margin: 0 0 12px 0; color: #2c3e50; font-size: 1.4rem;">{row['title']}</h3>
                    <p style="color: #34495e; line-height: 1.6; margin-bottom: 16px;">{row['description'] or 'No description available'}</p>
                    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                        <div style="display: flex; align-items: center; gap: 10px;">
                            <span class="source-badge">📰 {row['source']}</span>
                            <span class="timestamp">⏱️ {row['time_ago']}</span>
                        </div>
                        <a href="{row['url']}" target="_blank" style="text-decoration: none;">
                            <span class="read-button">Read Full Article →</span>
                        </a>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        st.success(f"📊 Showing {len(df)} articles for {selected_category}")

# Footer
st.markdown(
    """
    <div class="footer">
        <p>Real-Time News Dashboard • Auto-updates every 10 seconds • Made with Streamlit</p>
    </div>
    """,
    unsafe_allow_html=True
)
