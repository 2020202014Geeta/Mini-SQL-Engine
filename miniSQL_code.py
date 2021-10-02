# -*- coding: utf-8 -*-
import csv
import sys
import re
''' read the metadata file and store it in a dictionary: dict1 = { table1: [A,B,C],table2: [D,E]} '''
dict1={}
dict2={}
def readMDAta():
    fileDesM = open("metadata.txt", "r")
    mData = fileDesM.read()
    mDataRows = mData.split("\n")
    lenMFile = len(mDataRows)

    for i in range(0,lenMFile):
        if mDataRows[i]=='<begin_table>':
            list1 = []
            i= i+1
            tableName = mDataRows[i].lower()
            dict2[tableName] ={}
            i = i+1
            while(mDataRows[i]!='<end_table>'):
                list1.append(mDataRows[i].lower())
                i= i+1
            dict1[tableName]=list1
# dict2: {'table2': {'header': ['b', 'd'],'data': [ ['158', '11191'], ['773', '14421'], ['85', '5117'] ] } } 
def readCSV(): 
    for i in dict2:
        dict3 ={}
        list2=[]    
        dict3['header'] = dict1[i]
        csvFile = open(i + ".csv", 'r') 
        csvreader = csv.reader(csvFile)
        for row in csvreader:
            updatedRow = [int(i) for i in row]
            list2.append(updatedRow)
        dict3['data'] = list2    
        dict2[i]=dict3
#------------------------------final fields-------------------------------------------
def finalFieldsFun(parsedDataLL,crossProCol):
    finalFields = []
    distinctFlag = 0
    if type(parsedDataLL) is dict:
        parsedDataLL = [parsedDataLL]        
    if type(parsedDataLL) is str and parsedDataLL =="*":
        finalFields = crossProCol                # added
    else:           
        for el in parsedDataLL:           
            if type(el['value']) is str:
                finalFields.append(el['value'])
            else:
                if 'distinct' in el['value'].keys():                    
                    distinctFlag = 1
                    if type( el['value']['distinct']) is dict:
                        el['value']['distinct']= [el['value']['distinct']]
                        
                    for k in el['value']['distinct']:
                        if (type(k['value']) is str) and (k['value']=="*"):
                            finalFields = crossProCol                        # added
                        elif (type(k['value']) is str):
                            finalFields.append(k['value'])                 # added                     
                        elif type(k['value']) is dict:                           
                            agrFunFound = list(k['value'].keys())[0]
                            aggFunval = k['value'][agrFunFound]
                            finalFields.append(agrFunFound +'('+aggFunval + ')')        # added                                                  
                else:
                    agrFunFound = list(el['value'].keys())[0]
                    aggFunval = el['value'][agrFunFound]
                    finalFields.append(agrFunFound +'('+aggFunval + ')')        # added 
    return finalFields
#------------------------------select clause------------------------------------------
def agrExists(selectAttr):
    aggrFlag = 0
    for var in selectAttr:
        if type(var['value']) is dict:
            aggrFlag = 1
    return aggrFlag
    
def checkAgregate(attribute,group):
    if 'max' in attribute.keys():
        temp = [g[indexTable[attribute['max']]] for g in group]
        return max(temp)
    elif 'min' in attribute.keys():
        temp = [g[indexTable[attribute['min']]] for g in group]
        return min(temp)
    elif 'avg' in attribute.keys():
        temp = [g[indexTable[attribute['avg']]] for g in group]
        return sum(temp)/(len(temp))
    elif 'sum' in attribute.keys():
        temp = [g[indexTable[attribute['sum']]] for g in group]
        return sum(temp)
    elif 'count' in attribute.keys():
        return (len(group))
 
def selectClause(parsedData,interMTable):    
    finalResult = []
    aggrFlag = 0
    distinctFlag = 0
    if type(parsedData['select']) is dict:
        parsedData['select'] = [parsedData['select']]        
    if type(parsedData['select']) is str and parsedData['select']=="*":
        for j in interMTable:
            finalResult.append(j)
        return finalResult
    else:        
       if (type(parsedData['select'][0]['value']) is dict) and ('distinct' in parsedData['select'][0]['value'].keys()):
           distinctFlag = 1
           if type(parsedData['select'][0]['value']['distinct']) is dict:
               parsedData['select'][0]['value']['distinct'] = [parsedData['select'][0]['value']['distinct']]
           aggrFlag = agrExists(parsedData['select'][0]['value']['distinct'])
       else:
           aggrFlag = agrExists(parsedData['select'])           

    if distinctFlag:
        parsedData =  parsedData['select'][0]['value']['distinct']
    else:
        parsedData =  parsedData['select']
    if aggrFlag:
        innerList = [] 
        for var in parsedData:
            if type(var['value']) is str:
              try:
                  innerList.append( interMTable[0][ indexTable[var['value'] ] ]) 
              except:
                  print("INVALID SELECT ARGUMENTS")
                  sys.exit(0)                             
            elif type(var['value']) is dict:
               try:
                   val = checkAgregate(var['value'],interMTable)
               except:
                   print("INVALID SELECT ARGUMENTS")
                   sys.exit(0)  
               innerList.append( val)
        finalResult = innerList
    else: 
        for row in interMTable:
            innerList = [] 
            for var in parsedData:
                try:
                    innerList.append(row[indexTable[var['value'] ]])
                except:
                    print("INVALID SELECT ARGUMENTS")
                    sys.exit(0)
            finalResult.append(innerList) 
    if distinctFlag == 1 and aggrFlag==0:
       unique_list = []
       for x in finalResult:
           if x not in unique_list: 
               unique_list.append(x)
               finalResult = unique_list 
    return finalResult   
#--------------------------group BY select clause---------------------------------------------------------------- 
def gSelectClause(parsedData,interMTable):    
    finalResult = []
    distinctFlag = 0
    if type(parsedData['select']) is dict:
        parsedData['select'] = [parsedData['select']]
        
    if type(parsedData['select']) is str and parsedData['select']=="*":
        for table in interMTable:
            finalResult.append(interMTable[table][0])
    else:        
        for table in interMTable:
            innerList = []            
            for el in parsedData['select']:           
                if type(el['value']) is str:
                    try:
                        innerList.append(interMTable[table][0][indexTable[el['value']]])
                    except:
                        print("INVALID SELECT ARGUMENTS")
                        sys.exit(0)
                else:
                    if 'distinct' in el['value'].keys():
                        distinctFlag = 1
                        if type( el['value']['distinct']) is dict:
                            el['value']['distinct']= [el['value']['distinct']]
                        for k in el['value']['distinct']:
                            if (type(k['value']) is str) and (k['value']=="*"):
                                try:
                                   innerList.extend(interMTable[table][0])  
                                except:
                                    print("INVALID SELECT ARGUMENTS")
                                    sys.exit(0)
                            elif (type(k['value']) is str):
                                try:
                                    innerList.append(interMTable[table][0][indexTable[k['value']]])
                                except:
                                    print("INVALID SELECT ARGUMENTS")
                                    sys.exit(0)                                
                            elif type(k['value']) is dict:
                                try:
                                    val = checkAgregate(k['value'],interMTable[table])
                                except:
                                    print("INVALID SELECT ARGUMENTS")
                                    sys.exit(0)  
                                innerList.append(val)                        
                    else:
                        try:
                            val = checkAgregate(el['value'],interMTable[table])
                        except:
                            print("INVALID SELECT ARGUMENTS")
                            sys.exit(0)  
                        innerList.append(val)                    
            finalResult.append(innerList)
    if distinctFlag == 1:
        finalResult = [list(x) for x in set(tuple(x) for x in finalResult)]
    return finalResult
#-------------------------------Output function---------------------------------------
def outputFormat(finalResult,finalFields):
    tempFields = []
    for el in finalFields:
        for key in dict1:            
            if el in dict1[key]:                
                el = key+"."+el
                break
        tempFields.append(el)
    finalFields = tempFields     
    if not finalResult:
        print(*finalFields,sep = ', ') 
        print(*finalResult,sep = ', ')
        return
    if type(finalResult[0]) is not list:
       #print(finalResult[0]) 
       finalResult = [finalResult]
    print(*finalFields,sep = ', ') 
    for el in finalResult:
        print(*el,sep = ', ') 
#---------------------------------order by--------------------------------------------
def orderByClause(parsedData, finalResult): 
    if 'sort' in parsedData['orderby'].keys():
        sortMethod = parsedData['orderby']['sort']
    else:
        sortMethod = 'asc'         
    if 'groupby' in parsedData.keys():     
        if sortMethod == 'asc':
           final = {}
           for i in sorted(finalResult.keys()):
               final[i] = finalResult[i]
           finalResult = final

        elif sortMethod == 'desc':
           final = {}
           for i in sorted(finalResult.keys(),reverse=True):
               final[i] = finalResult[i]
           finalResult = final      
    else:
        try:
           finalResult = sorted(finalResult, key=lambda x: x[indexTable[parsedData['orderby']['value']]])
        except:
            print("INVALID ORDER BY ELEMENT")
            sys.exit(0)
        if sortMethod == 'desc':
            finalResult = finalResult[::-1]
    return finalResult            
#-------------------------------from clause-------------------------------------------
def fromClause(queryTable):
    if type(queryTable) is str:
        queryTable = [queryTable]
    interMTable = []
    indexTable = {}
    k = 0
    for qT in queryTable:
        for i in dict2[qT]['header']:
            crossProCol.append(i)
            indexTable[i]=k
            k = k+1
    for qT in queryTable:  
        if not interMTable:
            interMTable = (dict2[qT]['data'])
        else:
            tempTable = []
            for i in interMTable:
                for j in dict2[qT]['data']:
                    tempTable.append(i + j)
            interMTable = tempTable
    return interMTable,indexTable,crossProCol
#-----------------------------where clause----------------------------------------------
def checkOperator(row,op):
    if 'gt' in op.keys():
        try:
          if type(op['gt'][1]) is str:
              return ( int( row[ indexTable[ op['gt'][0] ] ] )) > ( int( row[ indexTable[ op['gt'][1] ] ] ))
          else:
             return ( int( row[ indexTable[ op['gt'][0] ] ] ) > op['gt'][1] )
        except:
            print("INVALID WHERE ARGUMENTS")
            sys.exit(0)
    elif 'lt' in op.keys():
        try:
            if type(op['lt'][1]) is str:
                return ( int( row[ indexTable[ op['lt'][0] ] ] )) < ( int( row[ indexTable[ op['lt'][1] ] ] ))
            else:
                return ( int( row[ indexTable[ op['lt'][0] ] ] ) < op['lt'][1] )
        except:
            print("INVALID WHERE ARGUMENTS")
            sys.exit(0)
    elif 'gte' in op.keys():
        try:
            if type(op['gte'][1]) is str:
                return ( int( row[ indexTable[ op['gte'][0] ] ] )) >= ( int( row[ indexTable[ op['gte'][1] ] ] ))
            else:
                return ( int( row[ indexTable[ op['gte'][0] ] ]) >= op['gte'][1] ) 
        except:
            print("INVALID WHERE ARGUMENTS")
            sys.exit(0)
    elif 'lte' in op.keys():
        try:
            if type(op['lte'][1]) is str:
                return ( int( row[ indexTable[ op['lte'][0] ] ] )) <= ( int( row[ indexTable[ op['lte'][1] ] ] ))
            else:
                return (  int( row[ indexTable[ op['lte'][0] ] ]) <= op['lte'][1] )
        except:
            print("INVALID WHERE ARGUMENTS")
            sys.exit(0)
    elif 'eq' in op.keys():
        try:
            if type(op['eq'][1]) is str:
                return ( int( row[ indexTable[ op['eq'][0] ] ] )) == ( int( row[ indexTable[ op['eq'][1] ] ] ))
            else:
                return ( int( row[ indexTable[ op['eq'][0] ] ]) == op['eq'][1] )
        except:
            print("INVALID WHERE ARGUMENTS")
            sys.exit(0)

def whereClause(parsedData):
    finalTable=[]
    if 'and' in parsedData['where'].keys():
        for row in interMTable:
            if (checkOperator(row,parsedData['where']['and'][0])) and (checkOperator(row,parsedData['where']['and'][1])):
               finalTable.append(row)
    elif 'or' in parsedData['where'].keys():
        for row in interMTable:
            if (checkOperator(row,parsedData['where']['or'][0])) or (checkOperator(row,parsedData['where']['or'][1])):
               finalTable.append(row)
    else:
        for row in interMTable:
             if checkOperator(row,parsedData['where']):
                 finalTable.append(row)
    return finalTable
#--------------------------------------groupby Clause---------------------------------
def groupByClause(parsedData,finalTable,indexTable):  
    temp = {}
    try:
        index = indexTable[parsedData['groupby']['value']]   
    except:
        print("INVALID GROUP BY ARGUMENTS")
        sys.exit(0)
    for i in range(len(finalTable)):
        if finalTable[i][index] in temp.keys():
            temp[finalTable[i][index]].append(finalTable[i])
        else:
            temp[finalTable[i][index]] = [finalTable[i]]
    return temp    
#------------------------------driver code---------------------------------------------
readMDAta()
readCSV()
indexTable =[]
finalFields = []
crossProCol = []
from moz_sql_parser import parse
query = str(sys.argv[1])
query = query.lower()
query = ' '.join(query.split())
if query[-1] != ";":
   print("INVALID QUERY DOES NOT ENDS WITH SEMICOLON.")
   sys.exit(0)
if bool(re.match('^select.*from.*', query)) is False:
    print("USAGE: SELECT * FROM TABLENAME;")
    sys.exit(0)
try:
   parsedData = parse(query)
except:
   print("USAGE: SELECT * FROM TABLENAME;")
   sys.exit(0)
fromCheck = parsedData['from']
if type(fromCheck) is not list:
    fromCheck = [fromCheck]    
for table in fromCheck:
   if table not in list(dict1.keys()):       
       print("INVALID TABLE NOT FOUND")
       sys.exit(0)
interMTable,indexTable,crossProCol = fromClause(parsedData['from'])
cross = interMTable
if 'where' in parsedData.keys():
    interMTable = whereClause(parsedData)
if 'groupby' in parsedData.keys():
    interMTable = groupByClause(parsedData,interMTable,indexTable)
    if 'orderby' in parsedData.keys():
        interMTable = orderByClause(parsedData, interMTable)
    finalResult = gSelectClause(parsedData,interMTable)
else:
    if 'orderby' in parsedData.keys():
        interMTable = orderByClause(parsedData, interMTable)
    finalResult = selectClause(parsedData,interMTable)
finalFields = finalFieldsFun(parsedData['select'],crossProCol)
outputFormat(finalResult,finalFields)    