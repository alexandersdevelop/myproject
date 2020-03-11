# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 16:28:51 2020

@author: etolmakova
"""

from app import app#, db
# app.models import

if __name__ == '__main__':
    app.run( debug=True, use_reloader = True, port=8000)#host='0.0.0.0', 
    
@app.shell_context_processor
def make_shell_context():
    return {'app' : app}