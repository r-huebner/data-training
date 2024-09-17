import pandas as pd
from pydantic import BaseModel
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Header, Query
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Create a logger
logger = logging.getLogger(__name__)

# Load db
db = pd.read_excel("questions_en.xlsx")

# Create users
users = {
    "admin": "4dm1N",
    "alice": "wonderland",
    "bob": "builder",
    "clementine": "mandarine"
}

# Question class
class Question(BaseModel):
    question: str
    subject: List[str]
    correct: str
    use: str
    responseA: str
    responseB: str
    responseC: Optional[str]= None
    responseD: Optional[str]= None

# Init api
api = FastAPI(
    title="Questionnaires",
    description="Create questionnaires.",
    version="1.0.1"
)

async def authenticate(header_data: str):
    if header_data is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        creds = header_data.split()[1]
        username, password = creds.split(":")
        
        if users.get(username) is None: 
            raise HTTPException(status_code=401, detail="Invalid username.")
        
        if password != users.get(username):
            raise HTTPException(status_code=401, detail="Invalid password.")
    except IndexError:
        raise HTTPException(status_code=401, detail="Invalid credentials format.")
    
    return True

@api.get('/')
async def get_index(authorization: Optional[str] = Header(None)):
    """Returns message for the index"""
    if not await authenticate(authorization): 
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    return {'message': 'Create questionnaires with this api.'}

@api.get('/status')
async def get_status(authorization: Optional[str] = Header(None)):
    """Check if API running."""
    if not await authenticate(authorization): 
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    return {"status": "running"}

@api.post('/new_question')
async def post_question(question: Question, authorization: Optional[str] = Header(None)):
    """Add a question."""
    if not await authenticate(authorization) or users.get("admin") is None: 
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Add question
    global db
    new_row = pd.DataFrame([question.dict()])
    db = pd.concat([db, new_row], ignore_index=True) 

    
    return question

@api.get('/get_questions')
async def get_questions(use: str,
                        num: int,
                        subject: List[str] = Query(None),
                        authorization: Optional[str] = Header(None)):
    """Get questions."""
    if not await authenticate(authorization): 
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    try:
        # Not a valid number of questions
        if num not in [5, 10, 20]:
            raise HTTPException(status_code=400, detail="Wrong number of questions. Choose 5, 10, or 20.")

        # Check if all subjects in db
        for subj in subject: 
            if not any(db["subject"].isin([subj])):
                raise HTTPException(status_code=400, detail=f"{subj} subject not found.")
                
        # Check if use type in db
        if not any(db["use"].isin([use])):
            raise HTTPException(status_code=400, detail="Invalid use type.")
        
        # Get questions
        questions = db[(db["subject"].isin(subject)) & (db["use"] == use)].sample(num)
        questions = questions.fillna("").to_dict()
        logger.debug(f'{questions}')

        return questions
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Bad type")
    
    
