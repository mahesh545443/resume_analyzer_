import sqlite3
import logging
from datetime import datetime
from config.settings import Config

class DatabaseManager:
    def __init__(self):
        self.db_path = Config.DB_PATH

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def init_db(self):
        """Initialize the database tables with ENHANCED schema."""
        conn = self.get_connection()
        c = conn.cursor()
        
        # 1. Enhanced Candidates Table
        c.execute('''
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT UNIQUE,
                name TEXT,
                email TEXT,
                phone TEXT,
                category TEXT,
                total_experience REAL,
                skills TEXT,
                domains TEXT,
                file_path TEXT,
                full_text TEXT,
                uploaded_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 2. Work History
        c.execute('''
            CREATE TABLE IF NOT EXISTS work_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER,
                role TEXT,
                company TEXT,
                start_date TEXT,
                end_date TEXT,
                description TEXT,
                FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
            )
        ''')

        # 3. Enhanced Projects Table
        c.execute('''
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER,
                project_name TEXT,
                description TEXT,
                tech_stack TEXT,
                domain TEXT,
                FOREIGN KEY(candidate_id) REFERENCES candidates(id) ON DELETE CASCADE
            )
        ''')
        
        conn.commit()
        conn.close()
        logging.info(f"✅ Database initialized at: {self.db_path}")

    def check_processed(self, filename: str):
        conn = self.get_connection()
        c = conn.cursor()
        c.execute("SELECT name FROM candidates WHERE filename = ?", (filename,))
        result = c.fetchone()
        conn.close()
        return result is not None

    def save_candidate_full(self, profile_data, work_data, project_data):
        """
        Save candidate with CATEGORY and PROJECT DOMAINS
        """
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute('''
                INSERT OR REPLACE INTO candidates 
                (filename, name, email, phone, category, total_experience, skills, domains, file_path, full_text, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                profile_data['filename'], 
                profile_data['name'], 
                profile_data['email'],
                profile_data['phone'], 
                profile_data.get('category', 'Other'),
                profile_data['total_experience'], 
                profile_data['skills'],
                profile_data['domains'], 
                profile_data['file_path'], 
                profile_data['full_text'],
                datetime.now()
            ))
            
            c.execute("SELECT id FROM candidates WHERE filename = ?", (profile_data['filename'],))
            candidate_id = c.fetchone()[0]
            
            c.execute("DELETE FROM work_history WHERE candidate_id = ?", (candidate_id,))
            for work in work_data:
                c.execute('''
                    INSERT INTO work_history (candidate_id, role, company, start_date, end_date, description)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (candidate_id, work.role, work.company, work.start_date, work.end_date, work.description))

            c.execute("DELETE FROM projects WHERE candidate_id = ?", (candidate_id,))
            for proj in project_data:
                c.execute('''
                    INSERT INTO projects (candidate_id, project_name, description, tech_stack, domain)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    candidate_id, 
                    proj.name, 
                    proj.description, 
                    proj.tech_stack,
                    getattr(proj, 'domain', 'General')
                ))
            
            conn.commit()
            return candidate_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

# CRITICAL: This is where we define the instance that OTHER files import.
db = DatabaseManager()
db.init_db()