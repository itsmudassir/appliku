import os
from flask import Flask, request
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///test.db')
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    email = Column(String, index=True)
    message = Column(String, index=True)

Base.metadata.create_all(bind=engine)

@app.route('/messages', methods=['POST'])
def add_message():
    data = request.get_json()
    name = data['name']
    email = data['email']
    message = data['message']
    new_message = Message(name=name, email=email, message=message)
    db = SessionLocal()
    db.add(new_message)
    db.commit()
    db.refresh(new_message)
    db.close()
    return {
        "name": new_message.name,
        "email": new_message.email,
        "message": new_message.message,
        "id": new_message.id
    }

@app.route('/', methods=['GET'])
def home():
    return 'Hello, world!'

if __name__ == '__main__':
    app.run(host='0.0.0.0')