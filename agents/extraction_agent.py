import json
import re
import logging
from typing import List, Optional
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field, ValidationError
from config.settings import Config

# ==================== 1. DATA MODELS (SCHEMA) ====================

class WorkExperience(BaseModel):
    role: str = Field(..., description="Job title, e.g., 'Senior Data Scientist'")
    company: str = Field(..., description="Company name")
    start_date: str = Field(..., description="YYYY-MM format. Use 'Unknown' if not found.")
    end_date: str = Field(..., description="YYYY-MM format. Use 'Present' if currently working.")
    description: str = Field(..., description="Summary of responsibilities and achievements.")

class Project(BaseModel):
    name: str = Field(..., description="Project Name")
    description: str = Field(..., description="What the project did and the impact.")
    tech_stack: str = Field(..., description="Comma separated technologies used")
    domain: str = Field("General", description="Project domain (e.g., Finance, Healthcare, Automobile)")

class ResumeData(BaseModel):
    full_name: str = Field(..., description="Candidate's full name")
    email: str = Field(..., description="Email address")
    phone: str = Field("", description="Phone number")
    category: str = Field("Other", description="Job category classification")
    skills: List[str] = Field(default_factory=list, description="List of technical skills")
    domains: List[str] = Field(default_factory=list, description="Business domains")
    work_history: List[WorkExperience] = Field(default_factory=list, description="Work history")
    projects: List[Project] = Field(default_factory=list, description="Projects")

# ==================== 2. THE AGENT ====================

class ExtractionAgent:
    def __init__(self):
        self.llm = ChatGroq(
            model="llama-3.1-8b-instant", 
            temperature=0,
            api_key=Config.get_groq_key()
        )

    def extract(self, text: str):
        """
        Extracts structured JSON from resume text with CATEGORY classification.
        """
        try:
            clean_text = text[:8000] 
            
            prompt = f"""
            You are an expert AI Resume Analyzer. Your task is to extract structured data for a candidate database.
            
            RESUME TEXT:
            {clean_text}
            
            ### INSTRUCTIONS:
            
            1. **Category Classification:** Based on the job titles, skills, and experience, classify the candidate into ONE primary category:
               - "Data Analyst" → Focus on BI, dashboards, SQL, Excel, Tableau, reporting
               - "Data Scientist" → Focus on ML, Python, statistics, modeling, algorithms
               - "Data Engineer" → Focus on pipelines, ETL, Spark, Kafka, databases, big data
               - "Business Analyst" → Focus on requirements, process improvement, stakeholder management
               - "ML Engineer" → Focus on deploying ML models, MLOps, production systems, scalability
               - "Business Development" → Focus on sales, partnerships, growth, client relations
               - "Software Engineer" → Focus on web/mobile development, APIs, microservices
               - "Other" → If doesn't fit above categories
            
            2. **Dates:** Convert all dates to `YYYY-MM` format. If only year is found, use `YYYY-01`. If currently working, use "Present".
            
            3. **Skills:** Extract ALL technical skills mentioned (programming languages, tools, frameworks, libraries).
               - If a project description mentions a tool (e.g., "Built dashboard using Tableau"), add "Tableau" to skills.
            
            4. **Domains:** Infer the business domain from company descriptions:
               - Banks, financial institutions → "Finance"
               - Hospitals, clinics, pharma → "Healthcare"  
               - Retail companies, e-commerce → "Retail"
               - Car companies, automotive → "Automobile"
               - Tech companies → "Technology"
               - Manufacturing plants → "Manufacturing"
            
            5. **Projects:** For each project:
               - **CRITICAL:** tech_stack MUST be a COMMA-SEPARATED STRING like "Python, TensorFlow, AWS"
               - DO NOT return tech_stack as a list like ["Python", "TensorFlow"]
               - Infer the domain based on the description:
                 * "Built ML model for car price prediction" → domain: "Automobile"
                 * "Healthcare chatbot for patient queries" → domain: "Healthcare"
                 * "Inventory management system for retail" → domain: "Retail"
                 * If unclear, use "General"
            
            ### REQUIRED JSON FORMAT (Strictly follow this):
            {{
                "full_name": "string",
                "email": "string",
                "phone": "string",
                "category": "Data Analyst|Data Scientist|Data Engineer|Business Analyst|ML Engineer|Business Development|Software Engineer|Other",
                "skills": ["Python", "SQL", "Tableau"],
                "domains": ["Finance", "Healthcare"],
                "work_history": [
                    {{
                        "role": "Data Scientist",
                        "company": "XYZ Corp",
                        "start_date": "2020-01",
                        "end_date": "2023-06",
                        "description": "Built ML models for customer segmentation"
                    }}
                ],
                "projects": [
                    {{
                        "name": "Customer Churn Prediction",
                        "description": "ML model to predict customer churn using RandomForest",
                        "tech_stack": "Python, Scikit-learn, Pandas",
                        "domain": "Finance"
                    }}
                ]
            }}
            
            ### CRITICAL RULES:
            - tech_stack MUST be a STRING with commas: "Python, SQL, Tableau"
            - tech_stack is NOT a list: ["Python", "SQL"] is WRONG
            - skills IS a list: ["Python", "SQL", "Tableau"] is CORRECT
            - OUTPUT ONLY VALID JSON
            - NO MARKDOWN FORMATTING (no ```json or ```)
            - NO COMMENTS OR EXPLANATIONS
            - If a field is not found, use empty string "" or empty list []
            """
            
            # 1. Get Raw Response
            response = self.llm.invoke(prompt)
            raw_content = response.content

            # 2. Clean Markdown (Remove ```json and ```)
            json_str = re.sub(r"```json|```", "", raw_content).strip()

            # 3. ✅ Extract JSON object even if LLM adds extra text around it
            json_match = re.search(r'\{.*\}', json_str, re.DOTALL)
            if not json_match:
                logging.error("❌ No JSON object found in response.")
                return None
            json_str = json_match.group()

            # 4. Parse JSON
            data_dict = json.loads(json_str)
            
            # 5. Validate with Pydantic
            validated_data = ResumeData(**data_dict)
            return validated_data
            
        except json.JSONDecodeError as e:
            logging.error(f"❌ LLM returned bad JSON: {e}")
            return None
        except ValidationError as e:
            logging.error(f"❌ Data Validation Failed: {e}")
            return None
        except Exception as e:
            logging.error(f"❌ Extraction Error: {e}")
            return None

    def calculate_experience(self, work_history: List[WorkExperience]) -> float:
        """
        Calculates total years of experience based on work history dates.
        """
        from datetime import datetime
        total_months = 0
        
        for job in work_history:
            try:
                if len(job.start_date) == 4: 
                    start = datetime.strptime(f"{job.start_date}-01", "%Y-%m")
                else: 
                    start = datetime.strptime(job.start_date, "%Y-%m")
                
                if job.end_date.lower() in ["present", "current", "now", "till date"]: 
                    end = datetime.now()
                elif len(job.end_date) == 4: 
                    end = datetime.strptime(f"{job.end_date}-01", "%Y-%m")
                else: 
                    end = datetime.strptime(job.end_date, "%Y-%m")
                
                months = (end.year - start.year) * 12 + (end.month - start.month)
                
                if months <= 0: months = 1
                
                total_months += months
            except Exception:
                continue
            
        return round(total_months / 12, 1)