import pandas as pd
import time
import re
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from core.database import db
from core.vector_db import VectorDB
from config.settings import Config


class QueryAgent:
    def __init__(self):
        self.llm = ChatGroq(
            model="llama-3.1-8b-instant",
            temperature=0,
            api_key=Config.get_groq_key()  # ✅ FIXED
        )
        self.vector_db = VectorDB()
        
        self.conversation_history = []
        self.last_query_results = None
        self.last_candidates_list = []
        self.context_window = 5

    def process_query(self, query: str):
        start_time = time.time()
        self.conversation_history.append({"role": "user", "query": query})
        
        if self._is_contextual_query(query):
            result = self._handle_contextual_query(query, start_time)
        else:
            strategy = self._decide_strategy(query)
            
            if strategy == "FILE":
                result = self._fetch_file(query)
            elif strategy == "RAG":
                result = self._query_rag(query, start_time)
            elif strategy == "SQL":
                result = self._query_sql(query, start_time)
            else:
                result = self._query_rag(query, start_time)
        
        self.conversation_history.append({"role": "assistant", "response": result})
        
        if len(self.conversation_history) > self.context_window * 2:
            self.conversation_history = self.conversation_history[-self.context_window*2:]
        
        return result

    def _is_contextual_query(self, query: str) -> bool:
        q = query.lower()
        contextual_triggers = [
            'their resume', 'their cv', 'those candidates', 'these candidates',
            'top 2', 'top 3', 'top 5', 'top 10', 'first', 'show them',
            'these people', 'above candidates', 'from the list'
        ]
        return any(trigger in q for trigger in contextual_triggers)

    def _handle_contextual_query(self, query: str, start_time: float):
        if not self.last_candidates_list:
            return "❌ No previous candidates to reference. Please run a search first."
        
        q = query.lower()
        
        num_match = re.search(r'top\s+(\d+)|first\s+(\d+)', q)
        if num_match:
            n = int(num_match.group(1) or num_match.group(2))
        else:
            n = len(self.last_candidates_list)
        
        n = min(n, len(self.last_candidates_list))
        
        conn = db.get_connection()
        file_paths = []
        
        for candidate_name in self.last_candidates_list[:n]:
            try:
                name_parts = candidate_name.split()
                longest_part = max(name_parts, key=len) if name_parts else candidate_name
                
                sql = f"SELECT file_path FROM candidates WHERE LOWER(name) LIKE LOWER('%{longest_part}%') LIMIT 1"
                df = pd.read_sql_query(sql, conn)
                
                if not df.empty:
                    file_paths.append(df.iloc[0]['file_path'])
            except:
                continue
        
        conn.close()
        
        if not file_paths:
            return f"❌ Could not find resume files for the requested candidates."
        
        elapsed = round(time.time() - start_time, 2)
        return f"FILE_FOUND:{'||'.join(file_paths)}"

    def _decide_strategy(self, query: str) -> str:
        q = query.lower()
        
        if "resume of" in q: return "FILE"
        file_triggers = ['resume', 'cv', 'file', 'document', 'pdf']
        action_triggers = ['send', 'download', 'open', 'show', 'give', 'fetch']
        if any(x in q for x in file_triggers) and any(x in q for x in action_triggers):
            return "FILE"

        sql_triggers = [
            'how many', 'count', 'total', 'list', 'top', 'find', 'who',
            'experience', 'exp', 'years', 'yrs', 'skills', 'candidates',
            'shortlist', 'names', 'category', 'categories'
        ]
        if any(x in q for x in sql_triggers):
            if "describe" in q or "summarize" in q:
                return "RAG"
            return "SQL"

        rag_concepts = [
            'project', 'projects', 'describe', 'summarize', 'explain',
            'details', 'tell me about', 'summary', 'context', 'what did',
            'responsibilities', 'work history'
        ]
        if any(x in q for x in rag_concepts):
            return "RAG"

        return "RAG"

    def _fetch_file(self, query: str):
        try:
            target_name = self.llm.invoke(f"Extract Name from '{query}'. Return JUST the name.").content.strip()
            target_name = target_name.replace("'", "").replace("?", "").replace("!", "").strip()
            
            conn = db.get_connection()
            name_parts = target_name.split()
            if name_parts:
                longest_part = max(name_parts, key=len)
                sql = f"SELECT name, file_path FROM candidates WHERE LOWER(name) LIKE LOWER('%{longest_part}%') LIMIT 1"
            else:
                sql = f"SELECT name, file_path FROM candidates WHERE LOWER(name) LIKE LOWER('%{target_name}%') LIMIT 1"

            df = pd.read_sql_query(sql, conn)
            conn.close()
            
            if df.empty: return f"🔍 I looked for **{target_name}**, but found no file."
            return f"FILE_FOUND:{df.iloc[0]['file_path']}"
        except Exception as e: return f"⚠️ Error: {str(e)}"

    def _query_sql(self, query: str, start_time: float):
        schema_hint = """
        Table: candidates | Columns: name, total_experience, skills, full_text
        Rules:
        1. ALWAYS SELECT * FROM candidates (Never use COUNT).
        2. IF searching INDUSTRY (e.g. 'manufacturing'), search `full_text`:
           `WHERE LOWER(full_text) LIKE LOWER('%keyword%')`
        3. Sort by `total_experience DESC`.
        """

        try:
            prompt = ChatPromptTemplate.from_template("{schema}\nQuestion: {question}\nReturn SQL query only.")
            sql_response = self.llm.invoke(prompt.format(schema=schema_hint, question=query))
            sql_query = sql_response.content.strip().replace("```sql", "").replace("```", "")
            
            conn = db.get_connection()
            df = pd.read_sql_query(sql_query, conn)
            conn.close()
            
            self.last_query_results = df.copy()
            self.last_candidates_list = df['name'].tolist() if 'name' in df.columns else []
            
            elapsed = round(time.time() - start_time, 2)
            if df.empty: return f"❌ No matches found. (Time: {elapsed}s)"
            
            for col in ['name', 'total_experience', 'skills']:
                if col not in df.columns: df[col] = "N/A"

            count = len(df)
            if count > 10:
                top_df = df.head(10)[['name', 'total_experience', 'skills']]
                msg = f"**I found {count} candidates.** Top 10:\n\n" + top_df.to_markdown(index=False)
            else:
                lines = [f"**I found {count} candidates:**"]
                for _, row in df.iterrows():
                    skills = str(row['skills'])[:50] + "..." if len(str(row['skills'])) > 50 else str(row['skills'])
                    lines.append(f"• **{row['name']}** ({row['total_experience']} years) - {skills}")
                msg = "\n".join(lines)

            return msg + f"\n\n---\n*📊 Matches: {count} | ⏱️ Time: {elapsed}s | ✅ High Confidence*"
        except Exception as e: return f"⚠️ SQL Error: {str(e)}"

    def _query_rag(self, query: str, start_time: float):
        target_name = self.llm.invoke(f"Extract Name from '{query}'. Return JUST the name.").content.strip()
        target_name = target_name.replace("'", "").replace("?", "").replace("!", "").strip()
        
        docs_content = ""
        is_targeted = False

        if target_name and target_name.upper() != "NO":
            try:
                conn = db.get_connection()
                name_parts = target_name.split()
                if name_parts:
                    longest_part = max(name_parts, key=len)
                    sql = f"SELECT name, full_text FROM candidates WHERE LOWER(name) LIKE LOWER('%{longest_part}%') LIMIT 1"
                else:
                    sql = f"SELECT name, full_text FROM candidates WHERE LOWER(name) LIKE LOWER('%{target_name}%') LIMIT 1"
                
                df = pd.read_sql_query(sql, conn)
                conn.close()
                if not df.empty:
                    docs_content = f"Candidate: {df.iloc[0]['name']}\nText:\n{df.iloc[0]['full_text'][:3500]}"
                    is_targeted = True
            except: pass

        if not docs_content:
            docs = self.vector_db.search(query, k=5)
            if not docs: return "I couldn't find relevant details."
            docs_content = "\n".join([f"Candidate: {d.metadata.get('name')}\nText: {d.page_content}" for d in docs])

        elapsed = round(time.time() - start_time, 2)
        prompt = f"""
        You are an AI Recruiter. Answer based ONLY on the text below.
        Resume Context:
        {docs_content}
        Question: {query}
        """
        response = self.llm.invoke(prompt).content
        confidence = "100%" if is_targeted else "Variable"
        icon = "🟢" if is_targeted else "🟡"
        return response + f"\n\n---\n*📊 Relevance: {confidence} {icon} | ⏱️ Time: {elapsed}s*"
