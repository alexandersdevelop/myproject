# -*- coding: utf-8 -*-

import os

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    '''
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'mysql+pymysql://research:y7&3ojcNabl*@84.201.165.217:3306/research_rus_null_plus'
    SQLALCHEMY_BINDS = {
        'raw_data_nat_null_plus':    'mysql+pymysql://research:y7&3ojcNabl*@84.201.165.217:3306/research_rus_null_plus' ,
        'palomars':    'DRIVER={SQL Server};SERVER=62.231.8.162;DATABASE=M1_161;UID=etolmakova;PWD=Ghjcnjnfr123!'        
    }
    '''
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    SEND_FILE_MAX_AGE_DEFAULT = 0 # время хранения кэша