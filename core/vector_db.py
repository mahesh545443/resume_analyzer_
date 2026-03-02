import logging
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone, ServerlessSpec
from config.settings import Config


class VectorDB:

    def __init__(self):
        # HuggingFace embeddings — 384 dimensions
        self.embeddings = HuggingFaceEmbeddings(
            model_name=Config.EMBEDDING_MODEL
        )
        self.vectorstore = self._load_vectorstore()

    def _load_vectorstore(self):
        """Connect to Pinecone index"""
        try:
            pc = Pinecone(api_key=Config.get_pinecone_key())

            # Check existing indexes
            existing_indexes = [i.name for i in pc.list_indexes()]

            if Config.PINECONE_INDEX_NAME not in existing_indexes:
                pc.create_index(
                    name=Config.PINECONE_INDEX_NAME,
                    dimension=384,
                    metric="cosine",
                    spec=ServerlessSpec(
                        cloud="aws",
                        region="us-east-1"
                    )
                )

                logging.info(
                    f"✅ Created Pinecone index: {Config.PINECONE_INDEX_NAME}"
                )

            vectorstore = PineconeVectorStore(
                index_name=Config.PINECONE_INDEX_NAME,
                embedding=self.embeddings,
                pinecone_api_key=Config.get_pinecone_key()
            )

            logging.info("✅ Connected to Pinecone index.")

            return vectorstore

        except Exception as e:
            logging.error(f"⚠️ Pinecone connection failed: {e}")
            return None

    def add_resume(self, text: str, metadata: dict):
        """Ingests a resume into Pinecone"""

        try:
            splitter = RecursiveCharacterTextSplitter(
                chunk_size=1000,
                chunk_overlap=200,
                separators=["\n\n", "\n", " ", ""]
            )

            doc = Document(
                page_content=text,
                metadata=metadata
            )

            splits = splitter.split_documents([doc])

            if not splits:
                return

            if self.vectorstore is None:
                self.vectorstore = self._load_vectorstore()

            # Pinecone persistence automatic
            self.vectorstore.add_documents(splits)

            logging.info(
                f"✅ Added {len(splits)} chunks to Pinecone for {metadata.get('name')}"
            )

        except Exception as e:
            logging.error(
                f"⚠️ Failed to add resume to Pinecone: {e}"
            )

    def search(self, query: str, k=3):
        """Performs semantic search in Pinecone"""

        if not self.vectorstore:
            logging.warning("⚠️ Pinecone not connected.")
            return []

        try:
            results_with_scores = self.vectorstore.similarity_search_with_score(
                query,
                k=k
            )

            cleaned_results = []

            for doc, score in results_with_scores:

                raw_confidence = (1.0 - min(score, 1.0)) * 100

                doc.metadata["score_raw"] = round(score, 4)
                doc.metadata["confidence_score"] = round(raw_confidence, 1)

                cleaned_results.append(doc)

            return cleaned_results

        except Exception as e:
            logging.error(
                f"⚠️ Pinecone search failed: {e}"
            )
            return []
