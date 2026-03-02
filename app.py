import streamlit as st
import os
import base64
from agents.query_agent import QueryAgent
from core.database import db

# ================= 1. PAGE CONFIG =================
st.set_page_config(
    page_title="Analytics Avenue AI",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for a professional look
st.markdown("""
    <style>
    .block-container {padding-top: 1.5rem;}
    section[data-testid="stSidebar"] {background-color: #f4f4f4;}
    .stChatInput {border-radius: 12px;}
    .pdf-container { border: 1px solid #ddd; border-radius: 8px; overflow: hidden; margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

# ================= 2. HELPER: PDF VIEWER =================
def display_pdf(file_path):
    """
    Reads a PDF file and displays it using an HTML iframe.
    """
    try:
        if not os.path.exists(file_path):
            st.error(f"⚠️ File not found at: {file_path}")
            return

        with open(file_path, "rb") as f:
            base64_pdf = base64.b64encode(f.read()).decode('utf-8')
        
        # Embed PDF in HTML
        pdf_display = f'''
            <div class="pdf-container">
                <iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="700" type="application/pdf"></iframe>
            </div>
        '''
        st.markdown(pdf_display, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"⚠️ Could not display PDF: {e}")

# ================= 3. SIDEBAR (System Status) =================
with st.sidebar:
    st.image("https://raw.githubusercontent.com/Analytics-Avenue/streamlit-dataapp/main/logo.png", width=150)
    st.markdown("### Resume Intelligence")
    st.divider()
    
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM candidates")
        count = cursor.fetchone()[0]
        conn.close()
        st.success("🟢 System Online")
        st.metric("Total Indexed Resumes", count)
    except Exception as e:
        st.error("🔴 DB Offline")
        count = 0

    st.divider()
    if st.button("🗑️ Clear Chat History"):
        st.session_state.messages = []
        # We don't clear the agent so it keeps its memory during the session
        st.rerun()

# ================= 4. INITIALIZATION =================
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.messages.append({
        "role": "assistant", 
        "content": "Hello! I am your Resume Analyzer. I can count candidates, find top talent by experience, or open specific resumes. How can I help?"
    })

# CRITICAL: Initialize the Agent ONCE and keep it alive in session state
if "query_agent" not in st.session_state:
    st.session_state.query_agent = QueryAgent()

# ================= 5. CHAT INTERFACE =================
st.title("Analytics Avenue AI")
st.caption("🚀 AI-Powered Recruiting Assistant")

# Render History
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        if msg["content"].startswith("FILE_FOUND:"):
            # If it's a file path in history, show the label
            paths = msg["content"].replace("FILE_FOUND:", "").split("||")
            for p in paths:
                st.info(f"📂 Resume: {os.path.basename(p)}")
        else:
            st.markdown(msg["content"])

# ================= 6. USER INPUT & EXECUTION =================
if prompt := st.chat_input("Ask about candidates or say 'Show me their resumes'"):
    # Add user message to UI
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Generate Response
    with st.chat_message("assistant"):
        with st.spinner("Analyzing Database..."):
            try:
                # Use the persistent agent that remembers previous names
                agent = st.session_state.query_agent
                response = agent.process_query(prompt)
                
                # Check if the AI wants to show files
                if response.startswith("FILE_FOUND:"):
                    paths_string = response.replace("FILE_FOUND:", "").strip()
                    file_paths = paths_string.split("||")
                    
                    for file_path in file_paths:
                        file_name = os.path.basename(file_path)
                        st.subheader(f"📄 Preview: {file_name}")
                        display_pdf(file_path)
                        
                        # Add download button for convenience
                        with open(file_path, "rb") as f:
                            st.download_button(
                                label=f"⬇️ Download {file_name}",
                                data=f,
                                file_name=file_name,
                                key=f"dl_{file_path}" # Unique key for Streamlit
                            )
                    
                    st.session_state.messages.append({"role": "assistant", "content": response})
                
                else:
                    # Regular text/table response
                    st.markdown(response)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                    
            except Exception as e:
                st.error(f"Execution Error: {str(e)}")