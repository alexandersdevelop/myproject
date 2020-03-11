from app import app
from flask import request, jsonify 

import pyodbc
import pandas as pd
import numpy as np
import time
import re
from sqlalchemy import create_engine
from datetime import datetime
import os

@app.route('/')      
@app.route('/get', methods=['GET'])
def get():
    print('тут')
    '''
    d = {}
    for k in request.args.keys():
        d[k] = request.args.getlist(k)
    return str(d)
    '''
    #return('!!!')
    if request.method == 'GET':
        ##############
        if 'base' in request.args:
            base = request.args['base']
        else:
            return 'Пожалуйста, выберите базу.'
        if 'date_st' in request.args:
            try:
                date_st = int(request.args['date_st'])
            except ValueError:
                return 'Дата должна быть числом в формате ГГГГММДД.'
        else:
            return 'Пожалуйста, выберите даты.'
        if 'date_en' in request.args:
            try:
                date_en = int(request.args['date_en'])
            except ValueError:
                return 'Дата должна быть числом в формате ГГГГММДД.'
        else:
            return 'Пожалуйста, выберите даты.'
        if 'channels' in request.args:
            channels = request.args['channels'].split(',')
            try:    
                for i, ch in enumerate(channels):
                    channels[i] = int(ch)
            except ValueError:
                return 'Параметр channels должен содержать список id каналов в числовом формате.'
        else:
            channels = None
        #?
        
        if 'ta_lst' in request.args:
            ta_l = request.args['ta_lst'].split(',')
            if len(ta_l)%2!=0:
                return 'Целевые аудитории должны быть введены как пары (название, функция) по очереди. Длина списка должна быть кратна 2.'
            ta_lst = []
            for i in range(0,len(ta_l),2):
                ta_lst = ta_lst+[tuple(ta_l[i:i+2])]
            #return str(ta_l) + str( ta_lst)
        else:
            return 'Пожалуйста, выберите хотя бы одну целевую аудиторию.'
        if 'total' in request.args:
            total = True
        else:
            total = False
        if 'time_group' in request.args:
            time_group = request.args['time_group']
            if time_group != 'prime':
                try:
                    time_group = int(time_group)
                except ValueError:
                    return "Параметр time_group должен принимать одно из следующих значений: 0, 15, 30, 60, 'prime'"
        else:
            time_group = 0
        if 'statistics' in request.args:
            statistics = request.args['statistics'].split(',')
            for i, stat in enumerate(statistics):
                stat = stat[0].upper() + stat[1:]
                statistics[i] = stat
        else:
            return 'Пожалуйста, выберите хотя бы одну статистику для рассчета.'
        if 'medias' in request.args:
            medias = request.args['medias'].lower().split(',')
        else:
            return 'Пожалуйста, поместите хотя бы один атрибут на медиа ось.'
        ##############
        #some internal settings
        path = './app/' # path to pr_off.csv
        aggfunc = {'Audience': np.sum,
                   'Reach': np.sum,
                   'Projection': np.sum,
                   'TVR': np.sum,
                   'Quantity': pd.Series.nunique}     
        
        delimiter = '.'
        time_start = time.time()
        ############ обработка целевых аудиторий
        def kids_age_param(m):
            integer = 0
            for i in range(int(m.group(2)), int(m.group(3))+1):
                integer +=2**(7-i)
            res = m.group(1) + ' & ' + str(integer)
            return res
        for i, (ta_name, ta) in enumerate(ta_lst):
            ta = ta.lower()
            ta = re.sub(r'([a-zA-Z_]+)\s*\.\s*(\d+)', r'\1 = \2', ta)
            ta = re.sub(r'//=','!=', ta)
            ta = re.sub('(kids_age)\s+[iI][nN]\s*(\d+)\s*\.\.\s*(\d+)', kids_age_param, ta)
            ta = re.sub(r'([a-zA-Z_]+)\s+[iI][nN]\s*(\d+)\s*\.\.\s*(\d+)', r'\1 between \2 and \3', ta)
            ta_lst[i] = (ta_name, ta)
        ################## блоки из sql базы - 
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=172.16.3.138\SQLSERVER2014;DATABASE=M1_161;UID=etolmakova;PWD=Ghjcnjnfr123!')
        cursor = cnxn.cursor()
        
        #генератор дат из бд паломарса
        # input: дата начала (int), дата конца (int)
        # output: дата (int)
        def get_dates(date_start, date_finish):
            # генератор дат из базы
            sql = '''
            SELECT Calendar.pm_day
            FROM Calendar
            WHERE Calendar.pm_day BETWEEN %i AND %i
            ORDER BY Calendar.pm_day
            ''' %(date_start, date_finish)
            dates = pd.read_sql(sql, cnxn)
            for date in dates['pm_day']:
                yield date
          
        # функция окрукления времени старта и конца блока
        # input: время старта (pd.Series), время конца (pd.Series)
        # output: время старта (pd.Series), время конца (pd.Series)
        def roundtime(time_st, time_en): #применять все к series
            time_st_r = time_st.copy()
            time_en_r = time_en.copy()
            time_st_r.loc[time_st_r%100 < 30] = (time_st_r//100)*100
            time_st_r.loc[time_st_r%100 >= 30] = (time_st_r//100)*100+100
            time_st_r.loc[time_st_r%10000 >= 6000] = time_st_r-6000+10000
            time_en_r.loc[time_en_r%100 < 30] = (time_en_r//100)*100
            time_en_r.loc[time_en_r%100 >= 30] = (time_en_r//100)*100+100
            time_en_r.loc[time_en_r%10000 >= 6000] = time_en_r-6000+10000 
            # условие если длина меньше 1минуты после округления:
            mask = (time_en_r - time_st_r < 100)
            time_st_r.loc[mask] = (time_st//100)*100
            time_en_r.loc[mask] = time_st_r + 100
            time_en_r.loc[(time_en_r%10000) >= 6000] = time_en_r-6000+10000
            return time_st_r, time_en_r
        
        # функция запроса и округления времи блоков по одному дню и одному каналу
        # input: дата (int), Id канала (int)
        # output: блоки (pd.DataFrame)
        def get_blocks(date, channels):
            sql = '''
            SELECT TVCompany.cid, TVCompany.name, Breaks.distr, Calendar.day_type, Br_iss.pm_dow, Br_iss.pm_stmom, Br_iss.pm_enmom, Br_iss.pm_day
            FROM Br_iss, TVCompany, Calendar, Breaks
            WHERE Br_iss.cid IN(SELECT cid from TVCompany, TVChannel where TVCompany.hid=TVChannel.hid and TVChannel.rgn=99) 
            AND ( ( Br_iss.pm_day = %i ) 
            AND ( Br_iss.cid in (%s) )
            AND ( Br_iss.estat IN ( 'R' ) ) 
            AND ( Br_iss.lid IN ( SELECT Breaks.lid FROM Breaks WHERE ( ( Breaks.distr IN ( 'N','O' ) ) AND ( Breaks.cont IN ( 'C' ) ) ) ) ) ) 
            AND ( TVCompany.cid = Br_iss.cid ) 
            AND ( Calendar.pm_day = Br_iss.pm_day ) 
            AND ( Breaks.lid = Br_iss.lid )
            ORDER BY Br_iss.pm_day, Br_iss.pm_stmom, Br_iss.cid
            '''%(date, str(channels).strip('[]'))
            blocks = pd.read_sql(sql, cnxn)
            blocks['Start'], blocks['Finish'] =  roundtime(blocks['pm_stmom'], blocks['pm_enmom'])
            return blocks
        ################## данные телесмотрения
        # функции для разбивки по времени
        def timeformat_0(time_series): #применять все к series
            df = pd.DataFrame()
            df['time'] = time_series
            df['res'] = '0' + df['time'].astype(str)
            df['res'] = df['res'].str[-6:-4] + ':' + df['res'].str[-4:-2] + ':00 - ' + df['res'].str[-6:-4] + ':' + df['res'].str[-4:-2] + ':59'
            return df['res']
        
        def timeformat_15(time_series): #применять все к series
            df = pd.DataFrame()
            df['time'] = time_series
            df['quarter'] = (df['time'] % 10000) // 1500
            df['hour'] = '0' + (df['time'] // 10000).astype(str)
            df['hour'] = df['hour'].str[-2:]
            df['res'] = ''
            df.loc[ (df['quarter']==0), 'res'] = df['hour'] + ':00:00 - ' + df['hour'] + ':14:59'
            df.loc[ (df['quarter']==1), 'res'] = df['hour'] + ':15:00 - ' + df['hour'] + ':29:59'
            df.loc[ (df['quarter']==2), 'res'] = df['hour'] + ':30:00 - ' + df['hour'] + ':44:59'
            df.loc[ (df['quarter']==3), 'res'] = df['hour'] + ':45:00 - ' + df['hour'] + ':59:59'
            return df['res']
        
        def timeformat_30(time_series): #применять все к series
            df = pd.DataFrame()
            df['time'] = time_series
            df['halfanhour'] = df['time'] % 10000 // 3000
            df['hour'] = '0' + (df['time'] // 10000).astype(str)
            df['hour'] = df['hour'].str[-2:]
            df['res'] = ''
            df.loc[ (df['halfanhour']==0), 'res'] = df['hour'] + ':00:00 - ' + df['hour'] + ':29:59'
            df.loc[ (df['halfanhour']==1), 'res'] = df['hour'] + ':30:00 - ' + df['hour'] + ':59:59'
            return df['res']
        
        def timeformat_60(time_series): #применять все к series
            df = pd.DataFrame()
            df['time'] = time_series
            df['hour'] = '0' + (df['time'] // 10000).astype(str)
            df['hour'] = df['hour'].str[-2:]
            df['res'] = df['hour'] + ':00:00 - ' + df['hour'] + ':59:59'
            return df['res']
        
        def timeformat_prime(time_series, day_type, base): #применять все к series
            pr_csv = pd.read_csv(path + 'pr_off.csv', sep=';', header=0,  encoding='cp1251')
            df = pd.DataFrame()
            df['USL'] = time_series + day_type
            df = df.merge(pr_csv, how='left', on = 'USL')
            return df[base]
        ########### Расчет  
        engine = create_engine('mysql+pymysql://research:y7&3ojcNabl*@84.201.165.217:3306/research_rus_null_plus')
        res = pd.DataFrame()
        
        def check_dates(date_st, date_en):
            dates = pd.read_sql('SELECT Calendar.pm_day FROM Calendar WHERE Calendar.pm_day BETWEEN %i AND %i ORDER BY Calendar.pm_day'%(date_st, date_en), cnxn)
            uni = pd.read_sql(sql='select Date from uni where Date BETWEEN %i AND %i'%(date_st, date_en), con=engine)
            if len(dates) == len(uni):
                return True
            return False
            
        if not check_dates(date_st, date_en):
            st = str(pd.read_sql(sql='select min(Date) from uni', con=engine).iloc[0,0])
            en = str(pd.read_sql(sql='select max(Date) from uni', con=engine).iloc[0,0])
            cnxn.close()
            engine.dispose()
            return 'Выберите даты с %s до %s'%(st[-2:]+'.'+st[4:6]+'.'+st[:4], en[-2:]+'.'+en[4:6]+'.'+en[:4])
        #######
        if 'Reach' in statistics: 
            sql = '''SELECT Calendar.pm_day FROM Calendar
            WHERE Calendar.pm_day BETWEEN %i AND %i ORDER BY Calendar.pm_day''' %(date_st, date_en)
            dates = pd.read_sql(sql, cnxn)
            base_day = dates.loc[len(dates)//2 + (len(dates)%2 != 0), 'pm_day']
            print('base day:', base_day)
            
            base_target_aud = pd.read_sql(sql='select Member_nr, Weight from dem where (Date = %i and %s)'%(base_day, ta), con=engine)
            base_uni = pd.read_sql(sql='select * from uni where (Date = %i)'%(base_day), con=engine)
            weight_sum = pd.read_sql(sql='select sum(Weight) from dem where (Date = %i)'%(base_day), con=engine).iloc[0,0]
            base_uni['PF'] = base_uni['Universe_size']/weight_sum/1000
            base_target_aud['Reach'] = base_target_aud['Weight']*base_uni.loc[0,'PF']
        
        #######
        for _date in get_dates(date_st, date_en):
            print(_date)
            time_start_day = time.time()
            
            #######
            tdem = time.time()
            dem = pd.read_sql(sql='select Member_nr, Weight from dem where (Date = %i)'%(_date), con=engine)
            uni = pd.read_sql(sql='select * from uni where (Date = %i)'%(_date), con=engine)
            weight_sum = pd.read_sql(sql='select sum(Weight) from dem where (Date = %i)'%(_date), con=engine).iloc[0,0]
            uni['PF'] = uni['Universe_size']/weight_sum/1000
            dem['Projection'] = dem['Weight']*uni.loc[0,'PF']
            dem.drop(columns = ['Weight'], inplace=True)
            dem['Total'] = 1
            for (ta_name, ta) in ta_lst:
                if not ta_name:
                    print('Нет названия для аудитории %s'%(ta))
                    raise
                if ta:
                    target_dem = pd.read_sql(sql='select Member_nr from dem where (Date = %i and (%s))'%(_date, ta), con=engine)
                else:
                    target_dem = pd.read_sql(sql='select Member_nr from dem where (Date = %i)'%(_date), con=engine)
                dem[ta_name] = 0
                dem.loc[dem['Member_nr'].isin(target_dem['Member_nr']), ta_name] = 1
            print('get_dem_from_sql', time.time()-tdem)
            
            #######
            intersections = pd.DataFrame()
            blocks_without_intersections = pd.DataFrame()
            tchan = time.time()
            if channels:
                swd = pd.read_sql(sql='select Member_nr, Start, Finish, ChannelID from swd where (Date = %i and ChannelID in (%s))'%(_date, str(channels).strip('[]')), con=engine)
                blocks = get_blocks(_date, channels) #[['Start','Finish']]
            else:
                swd = pd.read_sql(sql='select Member_nr, Start, Finish, ChannelID from swd where (Date = %i)'%(_date), con=engine)
                _channels = swd['ChannelID'].unique().tolist()
                blocks = get_blocks(_date, _channels) #[['Start','Finish']]
            blocks['Quantity'] = blocks.index
            print('get_blocks_from_sql', time.time()-tchan)	
            #######
            time_start_intersect = time.time()
            swd = swd.merge(dem, on='Member_nr', how='inner')
            intersections = pd.merge(blocks, swd, how='outer', suffixes=('_block','_Member'), left_on='cid', right_on='ChannelID', sort=False)
            intersections = intersections[((intersections['Start_Member'] <= intersections['Start_block']) & \
                                           (intersections['Finish_Member'] > intersections['Start_block'])) | \
                                          ((intersections['Start_Member']  > intersections['Start_block']) & \
                                           (intersections['Start_Member']  < intersections['Finish_block']))]
            if len(blocks) != len(intersections['Quantity'].unique()):
                blocks_without_intersections = blocks.loc[~blocks['Quantity'].isin(intersections['Quantity'].unique())].copy()
                blocks_without_intersections.rename(columns={'Start':'Start_block','Finish':'Finish_block'}, inplace=True)
                for col in list(set(intersections.columns)-set(blocks_without_intersections.columns)):
                    blocks_without_intersections[col] = 0
                intersections = intersections.append(blocks_without_intersections, sort=False, ignore_index=True) 
            #print('tchan', time.time() - tchan)      
            tstat = time.time()
            
            intersections['tn(min)'] = intersections[['Finish_Member','Finish_block']].min(axis=1) - intersections[['Start_Member','Start_block']].max(axis=1)
            intersections.loc[intersections[['Finish_Member','Finish_block']].min(axis=1)//10000 >  intersections[['Start_Member','Start_block']].max(axis=1)//10000,
                              'tn(min)'] = intersections['tn(min)'] - 4000
            intersections['tn(min)'] = (intersections['tn(min)']//10000)*60 + (intersections['tn(min)']%10000)//100
            intersections['T_event(min)'] = intersections['Finish_block']-intersections['Start_block']
            intersections.loc[intersections['Finish_block']//10000 > intersections['Start_block']//10000, 'T_event(min)'] = intersections['T_event(min)'] - 4000
            intersections['T_event(min)'] = (intersections['T_event(min)']//10000)*60 + (intersections['T_event(min)']%10000)//100
            if 'Reach' in statistics:
                intersections = intersections.merge(base_target_aud, how='left', on='Member_nr')
            print('get_intersections', time.time()-time_start_intersect)
            #######
            tstat = time.time()
            #intersections.columns = pd.MultiIndex.from_product([intersections.columns.tolist(),['Total']])
            #intersections['ta'] = 'Total'
            #intersections['Audience'] = intersections['tn(min)']*intersections['Projection']/intersections['T_event(min)']
            #intersections['Target group size Total'] = dem['Projection'].sum()
            #intersections['TVR Total'] = intersections['Audience Total']/intersections['Target group size']*100
            
            # ?! как вариант здесь ввести мультииндекс и добавлять последующие статистики сразу по мультииндексу колонок; не забыть столбцы в индексы
            for stat in statistics:
                if stat == 'Quantity':
                    for ta_name in ['Total'] + [x for (x,y) in ta_lst]:
                        intersections['Quantity' + delimiter + ta_name] = intersections['Quantity']
                if stat == 'Audience':
                    for ta_name in ['Total'] + [x for (x,y) in ta_lst]:
                        intersections['Audience' + delimiter+ ta_name] = (intersections['tn(min)']*intersections['Projection']/intersections['T_event(min)'])*intersections[ta_name]
                if stat == 'TVR':
                    for ta_name in ['Total'] + [x for (x,y) in ta_lst]:
                        if 'Target group size' + delimiter + ta_name not in intersections.columns:
                            intersections['Target group size' + delimiter + ta_name] = (dem['Projection']*dem[ta_name]).sum()
                        if 'Audience' + delimiter + ta_name not in intersections.columns:
                            intersections['Audience' + delimiter + ta_name] = (intersections['tn(min)']*intersections['Projection']/intersections['T_event(min)'])*intersections[ta_name]
                        intersections['TVR' + delimiter + ta_name] = intersections['Audience' + delimiter + ta_name]/intersections['Target group size' + delimiter + ta_name]*100
                if stat == 'Reach':
                    for ta_name in ['Total'] + [x for (x,y) in ta_lst]:
                        intersections['Reach' + delimiter + ta_name] =  intersections['Reach']*intersections[ta_name]
            #intersections = intersections.append(intersections_ta, sort=True, ignore_index=True)
            print('get_stat', time.time() - tstat)
            
            #######
            ttime = time.time()
            intersections['pm_stmom'] = intersections['pm_stmom'].astype(int)
            if time_group == 0:
                intersections['start_time'] = timeformat_0(intersections['pm_stmom'])
            if time_group == 15:
                intersections['start_time'] = timeformat_15(intersections['pm_stmom'])
            if time_group == 30:
                intersections['start_time'] = timeformat_30(intersections['pm_stmom'])
            if time_group == 60:
                intersections['start_time'] = timeformat_60(intersections['pm_stmom'])
            if time_group == 'prime':
                intersections['start_time'] = timeformat_30(intersections['pm_stmom'])
                #return timeformat_prime(intersections['start_time'], intersections['day_type'], base)
                intersections['start_time'] = timeformat_prime(intersections['start_time'], intersections['day_type'], base)
                #return str(intersections['start_time'])
            print('timegroup', time.time() - ttime )    
        
            #######
            tpiv = time.time()
            #intersections = intersections.stack().reset_index()
            if 'Reach' in statistics:
                intersections.loc[intersections.duplicated( subset = medias +['Member_nr']), re.findall(r"'(Reach%s[^']*)"%(delimiter), str(intersections.columns.tolist()))] = 0 #col 'level_1' - col with names of target audiences
                #intersections.loc[intersections.duplicated( subset = medias +['Member_nr']), 'Projection'] = 0
                
            intersections.set_index(medias, inplace=True) #append=True
            intersections.columns =pd.MultiIndex.from_tuples([tuple(x.split(delimiter, maxsplit=1)) for x in intersections.columns])
            statistics_aggfunc = {}
            values = []
            #for stat in statistics: 
            for lvl1,lvl2 in zip(intersections.columns.get_level_values(0), intersections.columns.get_level_values(1)): #re.findall(r"'(%s%s[^']*)"%(stat, delimeter),str(intersections.columns.tolist())):
                if lvl1 in statistics:
                    if (not total) and (lvl2 == 'Total'):
                        continue
                    if lvl2!=lvl2: # if lvl2 is nan
                        continue 
                    statistics_aggfunc[(lvl1,lvl2)] = aggfunc.get(lvl1, None)
                    values.append((lvl1, lvl2))
            res = res.append(pd.pivot_table(data = intersections,
                                            #columns = ['ta'],
                                            values = values,
                                            index = medias, 
                                            aggfunc = statistics_aggfunc), sort=True)
            #ta_res.columns= pd.MultiIndex.from_product([ta_res.columns.tolist(),[ta_name_lst[i]]])
            
            print('get_pivot', time.time() - tpiv)
            print('day' , time.time() - time_start_day)
            
        #######
        
        if 'pm_dow' in res.index.names:
            res.rename(index = {1 : 'Воскресенье',
                                2 : 'Понедельник',
                                3 : 'Вторник',
                                4 : 'Среда', 
                                5 : 'Четверг',
                                6 : 'Пятница',
                                7 : 'Суббота'}, 
                       level = 'pm_dow', inplace=True)
        if 'distr' in res.index.names:
            res.rename(index = {'N' : 'Сетевой',
                                'O' : 'Орбитальный'},
                       level = 'distr', inplace=True)
        if 'day_type' in res.index.names:
            res.rename(index = {'W' : 'Рабочий',
                                'E' : 'Выходной',
                                'H' : 'Праздник',
                                'F' : 'День траура'}, 
                       level = 'day_type', inplace=True)
        def dateformat_to_write(date_int):
            return re.sub(r'(\d{4})(\d{2})(\d{2})', r'\3.\2.\1', str(int(date_int)))
            #return datetime.strptime(str(int(date_int)), format('%Y%m%d')).date()
        if 'pm_day' in res.index.names:  
            res.rename(index = dateformat_to_write,
                       level = 'pm_day', inplace=True)
        #res.index.names = [None]*len(res.index.names)
        #res = res.sort_index(axis=1)
        if total:
            ta_lst = [('Total', '')] + ta_lst
        res = res[[(stat, ta) for stat in statistics for (ta, x) in ta_lst]]
        res.reset_index(inplace=True)
        if 'cid' in res.columns:
            res['cid'] = res['cid'].astype(int)
        if 'Quantity' in statistics:
            for col in [('Quantity', ta) for (ta, x) in ta_lst]:
                res[col] = res[col].astype(int)
        
        #res.rename()
        #res = res.take([0]).append(res.copy(), sort=False)
        cols_level0 = [None]*len(medias) + res.columns.get_level_values(0).tolist()[len(medias):]
        #cols_level1 = pd.DataFrame([res.columns.get_level_values(1).tolist()], columns= cols_level0)
        cols_level1 = [None]*len(medias) + res.columns.get_level_values(1).tolist()[len(medias):]
        res.columns = pd.MultiIndex.from_tuples(zip(cols_level0,cols_level1))
        #res.columns = cols_level0
        #res = pd.DataFrame([cols_level1],columns=cols_level0).append(res.copy(), sort=False)
        #cols_level1.to_csv(path+'results.csv', index=False, sep=';', encoding='cp1251', decimal =',', float_format='%.4f')#, merge_cells=False
        #return str(res.iloc[0])
        #res.iloc[0] = 
        #res.to_csv(path+'results.csv', index=False, header=False, sep=';', encoding='cp1251', decimal =',', float_format='%.4f', mode='a+')#, merge_cells=False
        print('\n' + 'всего ' + str((time.time() - time_start)) +' seconds')  
        
        cnxn.close()
        engine.dispose()
        
        res.to_csv('./app/tmp/' + 'results' + str(datetime.today()).replace(':','.') + '.csv', 
                   index=False, header=True, sep=';', encoding='cp1251', decimal =',', float_format='%.4f')
        return res.to_json(orient='split', double_precision=4, index=False, force_ascii=False)#, date_format='iso'
