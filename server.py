from typing import Optional
from fastapi import FastAPI 
from process_data import data_process

app = FastAPI()

@app.get("/")
def read_root():
    done = data_process("./MyQualtricsDownload/Post Appointment Survey - October 19, 2021 - Josh.csv")
    return {"Hello": done}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}