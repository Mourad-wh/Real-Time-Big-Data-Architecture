#!/usr/bin/env python3
"""
Dashboard Streamlit pour visualisation temps réel
Projet Big Data - Analyse de Sentiment et Catégorisation
"""
import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import plotly.express as px
import plotly.graph_objects as go
from collections import defaultdict, deque
from datetime import datetime
import time
import threading

# ============================================
# CONFIGURATION
# ============================================
KAFKA_BROKER = "192.168.43.227:9092"  # 🔥 Remplace par ton IP VM1
TOPIC = "analyzed_results"  # 🔥 CHANGÉ : Lire les résultats analysés par Spark
MAX_MESSAGES = 500  # Garder les 500 derniers messages

# ============================================
# CONFIGURATION PAGE STREAMLIT
# ============================================
st.set_page_config(
    page_title="Big Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# STYLES CSS PERSONNALISÉS
# ============================================
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #262730;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    h1 {
        color: #00d4ff;
        text-align: center;
        padding: 20px;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        margin-bottom: 30px;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================
# INITIALISATION SESSION STATE
# ============================================
if 'messages' not in st.session_state:
    st.session_state.messages = deque(maxlen=MAX_MESSAGES)
    st.session_state.stats = {
        'total': 0,
        'by_source': defaultdict(int),
        'by_sentiment': defaultdict(int),
        'by_category': defaultdict(int),
        'timeline': []
    }
    st.session_state.consumer = None
    st.session_state.running = True

# ============================================
# FONCTION KAFKA CONSUMER
# ============================================
@st.cache_resource
def create_kafka_consumer():
    """Créer le consumer Kafka (une seule fois)"""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
            group_id='streamlit_dashboard'
        )
        return consumer
    except Exception as e:
        st.error(f"❌ Erreur connexion Kafka: {e}")
        return None

# ============================================
# LECTURE DES MESSAGES KAFKA
# ============================================
def read_kafka_messages():
    """Lire les nouveaux messages depuis Kafka"""
    consumer = create_kafka_consumer()
    
    if consumer is None:
        return
    
    try:
        # Poll messages
        messages = consumer.poll(timeout_ms=1000)
        
        for topic_partition, records in messages.items():
            for record in records:
                try:
                    data = record.value
                    timestamp = datetime.now()
                    
                    # Ajouter le message
                    message_data = {
                        'timestamp': timestamp,
                        'source': data.get('source', 'unknown'),
                        'title': data.get('title', 'No title')[:100],
                        'sentiment': data.get('sentiment', 'neutral'),  # 🔥 Vient de Spark maintenant
                        'category': data.get('predicted_category', 'uncategorized'),  # 🔥 Vient de Spark
                        'by': data.get('by', 'unknown'),
                        'word_count': data.get('word_count', 0)
                    }
                    
                    st.session_state.messages.append(message_data)
                    
                    # Mettre à jour les stats
                    st.session_state.stats['total'] += 1
                    st.session_state.stats['by_source'][message_data['source']] += 1
                    st.session_state.stats['by_sentiment'][message_data['sentiment']] += 1
                    st.session_state.stats['by_category'][message_data['category']] += 1
                    
                    st.session_state.stats['timeline'].append({
                        'time': timestamp,
                        'sentiment': message_data['sentiment']
                    })
                    
                    # Limiter la timeline à 100 points
                    if len(st.session_state.stats['timeline']) > 100:
                        st.session_state.stats['timeline'].pop(0)
                        
                except Exception as e:
                    st.warning(f"Erreur traitement message: {e}")
                    continue
                    
    except Exception as e:
        st.error(f"Erreur lecture Kafka: {e}")

# ============================================
# HEADER
# ============================================
st.markdown("<h1>🚀 Big Data Analytics Dashboard</h1>", unsafe_allow_html=True)
st.markdown("---")

# ============================================
# SIDEBAR - INFORMATIONS SYSTÈME
# ============================================
with st.sidebar:
    st.header("⚙️ Configuration Système")
    
    st.info(f"""
    **Kafka Broker:**  
    `{KAFKA_BROKER}`
    
    **Topic:**  
    `{TOPIC}`
    
    **Statut:** 🟢 Connecté
    """)
    
    st.markdown("---")
    
    st.subheader("📊 Statistiques Globales")
    st.metric("Total Messages", st.session_state.stats['total'])
    st.metric("Sources Actives", len(st.session_state.stats['by_source']))
    
    # Taux de positivité
    total_sent = sum(st.session_state.stats['by_sentiment'].values())
    if total_sent > 0:
        positive_rate = (st.session_state.stats['by_sentiment'].get('positive', 0) / total_sent * 100)
        st.metric("Taux Positif", f"{positive_rate:.1f}%")
    
    st.markdown("---")
    
    if st.button("🔄 Rafraîchir", type="primary"):
        st.rerun()
    
    st.caption("Dashboard mis à jour toutes les 2 secondes")

# ============================================
# LIRE NOUVEAUX MESSAGES
# ============================================
read_kafka_messages()

# ============================================
# MÉTRIQUES PRINCIPALES (KPI)
# ============================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="📨 Total Messages",
        value=st.session_state.stats['total'],
        delta="+1" if st.session_state.stats['total'] > 0 else "0"
    )

with col2:
    total_sent = sum(st.session_state.stats['by_sentiment'].values())
    positive_count = st.session_state.stats['by_sentiment'].get('positive', 0)
    st.metric(
        label="😊 Messages Positifs",
        value=positive_count,
        delta=f"{(positive_count/total_sent*100):.1f}%" if total_sent > 0 else "0%"
    )

with col3:
    st.metric(
        label="📡 Sources",
        value=len(st.session_state.stats['by_source']),
        delta="Actif"
    )

with col4:
    st.metric(
        label="🏷️ Catégories",
        value=len(st.session_state.stats['by_category']),
        delta="Détectées"
    )

st.markdown("---")

# ============================================
# GRAPHIQUES - LIGNE 1
# ============================================
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📊 Distribution des Sentiments")
    
    if st.session_state.stats['by_sentiment']:
        sentiment_df = pd.DataFrame([
            {'Sentiment': k, 'Nombre': v}
            for k, v in st.session_state.stats['by_sentiment'].items()
        ])
        
        # Couleurs personnalisées
        color_map = {
            'positive': '#28a745',
            'neutral': '#ffc107',
            'negative': '#dc3545'
        }
        
        fig_sentiment = px.pie(
            sentiment_df,
            values='Nombre',
            names='Sentiment',
            color='Sentiment',
            color_discrete_map=color_map,
            hole=0.4
        )
        
        fig_sentiment.update_traces(
            textposition='inside',
            textinfo='percent+label',
            textfont_size=14
        )
        
        fig_sentiment.update_layout(
            showlegend=True,
            height=400,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white')
        )
        
        st.plotly_chart(fig_sentiment, use_container_width=True)
    else:
        st.info("⏳ En attente de données...")

with col_right:
    st.subheader("🏷️ Top 5 Catégories")
    
    if st.session_state.stats['by_category']:
        category_df = pd.DataFrame([
            {'Catégorie': k, 'Nombre': v}
            for k, v in st.session_state.stats['by_category'].items()
        ])
        
        category_df = category_df.sort_values('Nombre', ascending=True).tail(5)
        
        fig_category = px.bar(
            category_df,
            x='Nombre',
            y='Catégorie',
            orientation='h',
            color='Nombre',
            color_continuous_scale='Viridis'
        )
        
        fig_category.update_layout(
            showlegend=False,
            height=400,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
        )
        
        st.plotly_chart(fig_category, use_container_width=True)
    else:
        st.info("⏳ En attente de données...")

st.markdown("---")

# ============================================
# GRAPHIQUES - LIGNE 2
# ============================================
col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("📡 Distribution par Source")
    
    if st.session_state.stats['by_source']:
        source_df = pd.DataFrame([
            {'Source': k, 'Messages': v}
            for k, v in st.session_state.stats['by_source'].items()
        ])
        
        fig_source = px.bar(
            source_df,
            x='Source',
            y='Messages',
            color='Messages',
            color_continuous_scale='Blues'
        )
        
        fig_source.update_layout(
            showlegend=False,
            height=400,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
        )
        
        st.plotly_chart(fig_source, use_container_width=True)
    else:
        st.info("⏳ En attente de données...")

with col_right2:
    st.subheader("⏱️ Timeline des Sentiments")
    
    if st.session_state.stats['timeline']:
        timeline_df = pd.DataFrame(st.session_state.stats['timeline'])
        timeline_df['minute'] = timeline_df['time'].dt.floor('1min')
        
        timeline_agg = timeline_df.groupby(['minute', 'sentiment']).size().reset_index(name='count')
        
        fig_timeline = px.line(
            timeline_agg,
            x='minute',
            y='count',
            color='sentiment',
            color_discrete_map={
                'positive': '#28a745',
                'neutral': '#ffc107',
                'negative': '#dc3545'
            }
        )
        
        fig_timeline.update_layout(
            height=400,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
        )
        
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("⏳ En attente de données...")

st.markdown("---")

# ============================================
# TABLE DES DERNIERS MESSAGES
# ============================================
st.subheader("📝 Derniers Messages Traités")

if st.session_state.messages:
    # Prendre les 15 derniers messages
    recent_messages = list(st.session_state.messages)[-15:]
    
    df_display = pd.DataFrame(recent_messages)
    df_display['timestamp'] = df_display['timestamp'].dt.strftime('%H:%M:%S')
    
    # Colorier le sentiment
    def color_sentiment(val):
        colors = {
            'positive': 'background-color: #d4edda',
            'neutral': 'background-color: #fff3cd',
            'negative': 'background-color: #f8d7da'
        }
        return colors.get(val, '')
    
    styled_df = df_display.style.applymap(
        color_sentiment,
        subset=['sentiment']
    )
    
    st.dataframe(
        df_display[['timestamp', 'source', 'title', 'sentiment', 'category']],
        use_container_width=True,
        hide_index=True,
        height=400
    )
else:
    st.info("⏳ En attente de messages depuis Kafka...")

# ============================================
# AUTO-REFRESH
# ============================================
time.sleep(2)
st.rerun()