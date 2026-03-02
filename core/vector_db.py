import os
import pickle
import logging
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from config.settings import Config

class VectorDB:
    def __init__(self):
        # We use HuggingFace embeddings (Efficient & Free)
        # It creates a 384-dimensional vector for every resume chunk.
        self.embeddings = HuggingFaceEmbeddings(model_name=Config.EMBEDDING_MODEL)
        self.vectorstore = self._load_vectorstore()

    def _load_vectorstore(self):
        """Safe loader for the FAISS index"""
        if os.path.exists(Config.VECTORSTORE_PATH):
            try:
                with open(Config.VECTORSTORE_PATH, "rb") as f:
                    return pickle.load(f)
            except Exception as e:
                logging.error(f"⚠️ Vector DB corrupted or unreadable: {e}")
                return None
        return None

    def add_resume(self, text: str, metadata: dict):
        """
        Ingests a resume into the vector database.
        Uses smart chunking to keep Skills and Projects context intact.
        """
        # Optimized Chunking for Resumes
        # 1000 chars covers a full 'Project' or 'Experience' block usually.
        # 200 overlap ensures we don't cut a sentence in half.
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000, 
            chunk_overlap=200,
            separators=["\n\n", "\n", " ", ""] # Priority: Split by paragraph first
        )
        
        doc = Document(page_content=text, metadata=metadata)
        splits = splitter.split_documents([doc])
        
        if not splits:
            return

        # Add to FAISS Index
        if self.vectorstore is None:
            self.vectorstore = FAISS.from_documents(splits, self.embeddings)
        else:
            self.vectorstore.add_documents(splits)
            
        # Auto-Save (Critical for persistence)
        self._save()

    def search(self, query: str, k=3):
        """
        Performs Semantic Search with Metric Calculation.
        Returns: List of Documents (with 'confidence_score' added to metadata)
        """
        if not self.vectorstore:
            return []
            
        # 1. Get Results with L2 Distance Scores
        # (Lower L2 distance = Better match)
        results_with_scores = self.vectorstore.similarity_search_with_score(query, k=k)
        
        cleaned_results = []
        
        for doc, score in results_with_scores:
            # 2. Convert L2 Distance to Percentage (Heuristic)
            # A score of 0.0 is perfect. A score of 1.0 is weak.
            # Formula: (1 - score) roughly gives relevance for normalized vectors.
            # We clamp it between 0% and 100%.
            
            raw_confidence = (1.0 - min(score, 1.0)) * 100
            
            # Inject metrics into metadata so the Agent can display them
            doc.metadata['score_raw'] = round(score, 4)
            doc.metadata['confidence_score'] = round(raw_confidence, 1) # Example: 85.5
            
            cleaned_results.append(doc)
            
        return cleaned_results

    def _save(self):
        """Safely saves the index to disk"""
        try:
            with open(Config.VECTORSTORE_PATH, "wb") as f:
                pickle.dump(self.vectorstore, f)
        except Exception as e:
            logging.error(f"⚠️ Failed to save Vector DB: {e}")