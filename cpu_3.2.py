from dotenv import load_dotenv
load_dotenv()
import time
import pickle
import urllib
from sqlalchemy import create_engine
import boto3
import os
import pandas as pd
import pyodbc
import gc
import numpy as np
import requests
import datetime
import json
from datetime import date, datetime, timedelta
from subprocess import Popen
from io import StringIO

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    return response       

def markWrikeTaskComplete (taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/"
    querystring = {
        'status':'Completed'
        }     
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    response = requests.request("PUT", url, headers=headers, params=querystring)
    return response        

def makeWrikeTaskSubtask (taskid, parenttaskid):
    
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/" + "?addSuperTasks=['" + parenttaskid + "']"

    payload={}
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    response = requests.request("PUT", url, headers=headers, data=payload)
    return response            

def map_violate_check (row):
    if ((row['MAP'] !=0) and (row['MAP'] > row['Price']) or row['MAPViolate'] != '') and row['ProductType'] != 'D' and row['PriceStatusCode'] != 'BEATMAPV' and row['PriceStatusCode'] != 'MATCHMAPV':
        return 'MAP Violation'
    else:
        return ''

def approved_reductions (row):
    if (row['PriceStatusCode'] != 'BEATMAPV') and (row['PriceStatusCode'] != 'MATCHMAPV') and (row['SalePrice'] < row['MAP']) and (row['ProposedPrice'] >= row['MAP']):
        return 'Price to MAP Compliance'
    else:        
        if (
            (row['SalePrice'] != row['ProposedPrice']) and 
            (row['CompBelowMAP'] == '') and 
            (row['CompBelowGovLowest'] == '') and 
            #(row['ManualPriceAdjustments'] == '') and 
            (
                (row['ProposedProfitAfterShip'] >= 6 and row['ProposedMargin'] >= 14) or 
                (row['ProposedProfitAfterShip'] >= 12 and row['ProposedMargin'] >= 11) or 
                (row['ProposedProfitAfterShip'] >= 18 and row['ProposedMargin'] >= 10) or 
                (row['ProposedProfitAfterShip'] >= 25 and row['ProposedMargin'] >= 9)                
                # (row['ProposedProfitAfterShip'] >= 18 and row['ProposedMargin'] >= 8) or 
                # (row['ProposedProfitAfterShip'] >= 25 and row['ProposedMargin'] >= 5)
            )
        ):
            return 'Price Approved'
        else:            
            return ''
        
def proposed_price_check (row):
    #Don't attempt to change any prices on PROMO, LIQUIDATE, or LOGISTICS
    if row['PriceStatusCode'] == 'PROMO' or row['PriceStatusCode'] == 'LIQUIDATE' or row['PriceStatusCode'] == 'LOGISTICS':
        return row['SalePrice'] 
    #BEATMAPV means beat the Market and Violate MAP if necessary
    elif row['PriceStatusCode'] == 'BEATMAPV':  
        if row['Price'] < row['SalePrice']:
            return row['ProposedPrice']
        else:
            return row['SalePrice']
    #BEATMKT means beat the Market but do NOT Violate MAP
    elif row['PriceStatusCode'] == 'BEATMKT': 
        if row['ProposedPrice'] < row['MAP'] and row['ProductType'] != 'D':
            return row['MAP']
        else:
            if row['Price'] < row['SalePrice']:
                return row['ProposedPrice']
            else:
                return row['SalePrice']            
    #MATCHMAPV means match the Market and Violate MAP if necessary
    elif row['PriceStatusCode'] == 'MATCHMAPV':
        if row['Price'] >= row['MAP'] :
            return row['MAP']
        else:
            return row['Price']
    #MATCHMKT means match the Market but do NOT Violate MAP        
    elif row['PriceStatusCode'] == 'MATCHMKT':
        if row['ProposedPrice'] < row['MAP'] and row['ProductType'] != 'D':
            return row['MAP']
        else:
            return row['Price'] 
    #Everything else      
    else:
        if (row['ProposedPrice'] < row['MAP']) and (row['ProductType'] != 'D'):
            return row['MAP']
        else:
            if row['Price'] < row['SalePrice']:
                return row['ProposedPrice']
            else:
                return row['SalePrice']

def gov_lowest_check (row):
    if (row['Gov_Lowest'] != 0) and (row['Gov_Lowest'] > row['ProposedPrice']):
        return "Can't Reprice Below GSA Lowest"                
    else:
        return ''  

def grab_CPU_scrapes(prefix, bucket, client, cleanup):
    
    """
    params:
    - prefix: pattern to match in s3
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    evalDate = date.today().strftime('%m/%d/%y') 
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    dfObj = pd.DataFrame(columns=['Competitor','Brand','ModelReference1','ModelReference2','CleanStrip1','CleanStrip2','PsuedoCode1','PsuedoCode2','Description','Price','MSRP','ProductUrl','MAPViolate','Notes','PageUrl'])
    dfObjHeaders = pd.DataFrame(columns=['Filename','Competitor','Brand','ScrapeDate','CPUDate'])
    #3{'Filename': k, 'Competitor': CompName, 'Brand': BrandName, 'ScrapeDate': scrapedate, 'CPUDate': evalDate}
    print('Grabbing list of new scrape data')
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        if contents is None:
            return None, None
        else:
            for i in contents:
                k = i.get('Key')
                if k[-1] != '/':
                    keys.append(k)
                else:
                    dirs.append(k)
            next_token = results.get('NextContinuationToken')
    print('Reading CPU scrape data')
    index = 0
    print(keys)
    time.sleep(10)
    for k in keys:
        obj = client.get_object(Bucket=bucket, Key=k)
        print(k)
        scrapedata = k.split('.')#removing extension
        scrapedata = scrapedata[0].split('-')
        BrandName = scrapedata[2]
        CompName = scrapedata[1]
        scrapedate = scrapedata[5] + '/' + scrapedata[6] + '/' + scrapedata[4]
        d = {'Filename': k, 'Competitor': CompName, 'Brand': BrandName, 'ScrapeDate': scrapedate, 'CPUDate': evalDate}
        dfObjHeaders = dfObjHeaders.append(pd.DataFrame(data=d, index=[index]))
        index = index + 1

        body = obj['Body']
        csv_string = body.read().decode('utf-8')
        dfCSV = pd.read_csv(StringIO(csv_string), sep='\t')
        dfCSV.to_csv('\\\\FOT00WEB\\Alt Team\\CPU\\Scrapes\\' + k, header=True, sep='\t', index=True) 

        # dfCSV = pd.read_csv(obj['Body'], sep='\t', encoding='utf8')
        dfCSV['ScrapeDate'] = scrapedate
        dfCSV['Filename'] = k
        dfObj = dfObj.append(dfCSV, sort=False)

        print(dfObj)

        #CLEANUP - backs up old scrapes
        if cleanup:
            print('Backing up CPU scrape - ' + k)
            client.copy_object(Bucket='cpu-mozenda', CopySource='cpu-mozenda/' + k, Key='OldScrapes/' +  k)
            client.delete_object(Bucket='cpu-mozenda', Key=k)    

    dfObj['ScrapeDate'] = pd.to_datetime(dfObj['ScrapeDate'], errors='coerce')
    dfObj['ScrapeDate'].fillna((datetime.today() - timedelta(days=36)),inplace=True)
    dfObj = dfObj.reset_index(drop=True)

    return dfObj, dfObjHeaders

def grab_Old_CPU_scrapes(newScrapesDf):
    """
    params:
    - dataframe: appending old scrapes to this dataframe
    """

    directory = '\\\\FOT00WEB\\Alt Team\\CPU\\Scrapes\\'
    BrandName = ''
    CompName = ''  
    scrapedate = 69

    for filename in reversed(os.listdir(directory)):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):

            dfCSV = pd.read_csv(f, sep='\t', dtype={'ModelReference1':str,'ModelReference2':str})
            scrapedata = filename.split('.')#removing extension
            scrapedata = scrapedata[0].split('-')
            dfCSV['ScrapeDate'] = scrapedata[5] + '/' + scrapedata[6] + '/' + scrapedata[4]           
            dfCSV['Filename'] = filename
            dfCSV['ScrapeDate'] = pd.to_datetime(dfCSV['ScrapeDate'], errors='coerce')
            dfCSV['ScrapeDate'].fillna((datetime.today() - timedelta(days=36)), inplace=True) 

            if dfCSV.loc[dfCSV['ScrapeDate'] > (datetime.today() - timedelta(days=34))].shape[0] > 0:

                if (BrandName == scrapedata[2]) and (CompName == scrapedata[1]) and (scrapedate >= int(scrapedata[5] + scrapedata[6] + scrapedata[4])):                    
                    print("Not appending scrape " + filename + "... more recent scrape already imported")
                    print("Moving to archive" + filename)
                    os.rename(f, os.path.join('\\\\FOT00WEB\\Alt Team\\CPU\\Scrapes\\archive\\', filename))
                else:
                    print("Appending scrape " + filename)
                    newScrapesDf = newScrapesDf.append(dfCSV.loc[dfCSV['ScrapeDate'] > (datetime.today() - timedelta(days=34))] , sort=False)     

            BrandName = scrapedata[2]
            CompName = scrapedata[1]     
            scrapedate = int(scrapedata[5] + scrapedata[6] + scrapedata[4])# datetime.strptime(scrapedata[5] + '/' + scrapedata[6] + '/' + scrapedata[4], "%M/%d/%Y").date() 
            
    print(newScrapesDf)

    return newScrapesDf

def low_profit_check (row):
    if row['CurrentProfitAfterShip'] < 6 or (row['CurrentProfitAfterShip'] < 25 and row['CurrentProfitMargin'] < 14) or row['CurrentProfitMargin'] < 5:
        if row['ManualPriceAdjustments'] == '':
            return 'Current Price results in Low Profit'
        else:
            return row['ManualPriceAdjustments'] + '|Current Price results in Low Profit'
    else:
        return row['ManualPriceAdjustments']

def price_increase_check (row):
    if row['SalePrice'] < row['ProposedPrice']:
        if row['ManualPriceAdjustments'] == '':
            return 'Suggest Price Increase'
        else:
            return row['ManualPriceAdjustments'] + '|Suggest Price Increase'    
    elif row['Price'] == row['SageMSRP']:
        return ''      
    else:
        return row['ManualPriceAdjustments']          
    
if __name__ == '__main__':

    
    # #Uncomment Below to establish a new pickle for last runtime being yesterday
    # current_run_time = datetime.date(datetime.now() - timedelta(hours=24))
    # with open('\\\\FOT00WEB\\Alt Team\\CPU\\last_CPU_runtime.p', 'wb') as f:
    #     pickle.dump(current_run_time, f)
    # print(current_run_time)
    # exit()


    #Making Files and Tasks
    #KUACOUUA - Andrew
    #KUAEL7RV - Anthony
    #KUAAY4PZ - Kris
    #KUAAZIJV - Travis
    #KUALCDZR - Josiah
    #assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ]'
    #assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]'
    assignees = '[KUAEL7RV,KUAAY4PZ,KUALCDZR]'
    #assignees = '[KUACOUUA]'
    folderid = 'IEAAJKV3I43JE3W5'   #this is the newly added CPU folder ... had to use postman to figure these out ;)
    #logf = open("CPUerror.log", "w")
    #try:

    #Stored in .env for aws s3
    aws_access_key_id = os.environ.get("AWSAccessKeyId")
    aws_secret_access_key = os.environ.get("AWSSecretKey")

    #Initialize S3 Client
    print('Initializing Amazon S3 Client')
    client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key= aws_secret_access_key)

    #Download NEW Scrape CPU contents of bucket 
    print('Grabbing newly published Mozenda Scrape data')
    ScrapeDF, ScrapeDFHeaders = grab_CPU_scrapes(prefix = 'CPU', bucket = 'cpu-mozenda', client = client, cleanup = True) 
    #ScrapeDF = pd.DataFrame(data=None, columns=['Brand','Competitor','ModelReference1','ModelReference2','Description','Price','MSRP','ProductUrl','LocationUrl','Notes','PageUrl','MAPViolate'])
    
    if ScrapeDF is None:
        print("No new scrapes exist")
        print('making blank dataframe')
        ScrapeDF = pd.DataFrame(columns=['Competitor','Brand','ModelReference1','ModelReference2','CleanStrip1','CleanStrip2','PsuedoCode1','PsuedoCode2','Description','Price','MSRP','ProductUrl','MAPViolate','Notes','PageUrl'])
    ScrapeDF = grab_Old_CPU_scrapes(ScrapeDF)
    print(ScrapeDF)

    #If no scrape data send wrike task reporting nothing was processed
    if ScrapeDF is None:
        print('No scrape data was found')
        description = "CPU was run, but no scrapes were found in the amazon S3 bucket"
        #logf.write(description)
    else:
        print('Mozenda Scrape Data retrieved')

        sage_conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";") 
        #Establish sage connection
        print('Connecting to Sage')
        cnxn = pyodbc.connect(sage_conn_str, autocommit=True)    
        
        #SQL Sage data into dataframe
        sql = """
            SELECT 
                CI_Item.PrimaryVendorNo AS 'VendorNo', 
                CI_Item.ProductLine AS 'ProdLine', 
                CI_Item.ItemCode, 
                CI_Item.ProductType, 
                IM_ItemVendor.VendorAliasItemNo AS 'VendorAlias', 
                CI_Item.UDF_CATALOG_NO AS 'Catalog', 
                CI_Item.UDF_UPC AS 'UPC', 
                CI_Item.UDF_WEB_DISPLAY_MODEL_NUMBER AS 'DisplayModel', 
                CI_Item.ItemCodeDesc, 
                CI_Item.SuggestedRetailPrice AS 'SageMSRP', 
                CI_Item.UDF_MAP_PRICE AS 'MAP', 
                CI_Item.StandardUnitPrice AS 'SalePrice', 
                CI_Item.UDF_LOWEST_PRICE AS 'Gov_Lowest', 
                CI_Item.StandardUnitCost AS 'Cost', 
                CI_Item.ShipWeight, 
                CI_Item.UDF_VENDOR_PRICE_DATE AS 'PriceUpdateDate', 
                CI_Item.LastSoldDate, 
                CI_Item.TotalInventoryValue AS 'TotalInventory', 
                CI_Item.TotalQuantityOnHand AS 'TotalQtyOH', 
                CI_Item.UDF_PRICE_STATUS_CODE AS 'PriceStatusCode', 
                CI_Item.UDF_PRICE_STATUS_DATE AS 'PriceStatusDate',                
                CI_Item.UDF_PACK_QUANTITY AS 'PackQty',
                CI_Item.UDF_CALL AS 'PleaseCall',
                CI_Item.UDF_SPECIALORDER AS 'SpecialOrder',
                CI_Item.InactiveItem
            FROM 
                CI_Item CI_Item, 
                IM_ItemVendor IM_ItemVendor
            WHERE 
                CI_Item.ItemCode = IM_ItemVendor.ItemCode AND 
                CI_Item.PrimaryVendorNo = IM_ItemVendor.VendorNo
        """
        #Execute SQL
        print('Retreiving Sage data')
        SageDF = pd.read_sql(sql,cnxn)
        SageDF = SageDF.loc[(SageDF['PriceStatusCode'] != 'MANUAL')] 
        SageDF = SageDF.loc[(SageDF['PriceStatusCode'] != 'BOOKVALUE')] 
        SageDF = SageDF.loc[(SageDF['PriceStatusCode'] != 'PROMO')] 
        SageDF = SageDF.loc[(SageDF['PriceStatusCode'] != 'MANUAL')] 
        SageDF = SageDF.loc[(SageDF['PriceStatusCode'] != 'REBALANCE')]    
        SageDF = SageDF.loc[(SageDF['PriceStatusCode'] != 'LIQUIDATE')]         
        
        if ScrapeDF.shape[0] == 0:
            print('Sage failed to retrieve data')
            description = "CPU was run, Scrapes were found, but unable to pull in Sage data"
            response = makeWrikeTask(title = "CPU - Failed Sage Data Pull - " + date.today().strftime('%y/%m/%d') , description = description, assignees = assignees, folderid = folderid)
        else:
            print('Sage data succesfully pulled!')
            SageDF['ShipWeight'] = pd.to_numeric(SageDF['ShipWeight'])

            #Cleanup scrapedf .... Remove all no price items and dupes
            print('Cleaning up Scrape Data...')
            print('dropping scrapes mising Brand, Competitor, or Model Ref1')
            ScrapeDF = ScrapeDF.dropna(subset=['Brand'])
            ScrapeDF = ScrapeDF.dropna(subset=['Competitor'])
            ScrapeDF = ScrapeDF.dropna(subset=['ModelReference1'])      
            print('Fill in null values with blanks')      
            ScrapeDF = ScrapeDF.fillna('') 
            
            #Price Data first
            print('Removing "$" and "," from price fields')
            ScrapeDF['Price'] = ScrapeDF['Price'].astype(str)
            ScrapeDF['Price'] = ScrapeDF['Price'].str.replace(',', '')
            ScrapeDF['Price'] = ScrapeDF['Price'].str.replace('$', '') 
            ScrapeDF['Price'] = pd.to_numeric(ScrapeDF['Price'], errors='coerce') 
            ScrapeDF['MSRP'] = ScrapeDF['MSRP'].astype(str)
            ScrapeDF['MSRP'] = ScrapeDF['MSRP'].str.replace(',', '')
            ScrapeDF['MSRP'] = ScrapeDF['MSRP'].str.replace('$', '')
            ScrapeDF['MSRP'] = pd.to_numeric(ScrapeDF['MSRP'], errors='coerce')                   
            
            #this is equiv to a excel trim
            print('Trimming blank space')
            ScrapeDF = ScrapeDF.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            
            #Set all those with no price to whatever the scrape got with MSRP 
            print('Filling in empty compeititor prices with MSRP')
            ScrapeDF.loc[ScrapeDF['Price'] == 0, 'Price'] = ScrapeDF['MSRP']  
            
            #Get rid of everything without a Price
            print('Removing all items with out a competitor price')
            ScrapeDF = ScrapeDF.query('Price>0')            

            #Get rid of everything without a model ref
            print('Removing all items with out a model reference')
            ScrapeDF = ScrapeDF.query('ModelReference1 != ""')
            
            #Spliting Brand
            print('Exploding Brand for matching')
            ScrapeDF['Brand'] = ScrapeDF['Brand'].str.split(pat = ',')
            ScrapeDF = ScrapeDF.explode('Brand')
            
            #Removing dupes
            print('Removing duplicated and earlier Scraped items')
            ScrapeDF.sort_values(by=['Price'], inplace=True)   
            ScrapeDF.sort_values(by=['ScrapeDate'], inplace=True, ascending=False)   
            ScrapeDF.drop_duplicates(subset=['ModelReference1','Competitor','Brand'], inplace=True)
            
            #Model Reference to string type
            print('Setting Model References to data type string')
            ScrapeDF['ModelReference1'] = ScrapeDF['ModelReference1'].astype(str)
            ScrapeDF['ModelReference2'] = ScrapeDF['ModelReference2'].astype(str)
            #Trim the Models
            print('Trimming Model References')
            ScrapeDF['ModelReference1'] = ScrapeDF['ModelReference1'].str.strip()
            ScrapeDF['ModelReference2'] = ScrapeDF['ModelReference2'].str.strip()
            #Psuedo-ItemCode and Clean Strip for lookups
            print('Creating a ASCII uppercase verison of the Model References for lookups')
            ScrapeDF['CleanStrip1'] = ScrapeDF['ModelReference1'].str.encode('ascii', 'ignore').str.decode('ascii')
            ScrapeDF['CleanStrip2'] = ScrapeDF['ModelReference2'].str.encode('ascii', 'ignore').str.decode('ascii')
            ScrapeDF['CleanStrip1'] = ScrapeDF['CleanStrip1'].str.upper()
            ScrapeDF['CleanStrip2'] = ScrapeDF['CleanStrip2'].str.upper()
            print('Creating a Psuedo Item code verison of the Model References for lookups')            
            ScrapeDF['PsuedoCode1'] = ScrapeDF['CleanStrip1'].str.replace('[^A-Z0-9+-.]', '')
            ScrapeDF['PsuedoCode2'] = ScrapeDF['CleanStrip2'].str.replace('[^A-Z0-9+-.]', '')
            ScrapeDF['PsuedoCode1'] = ScrapeDF['PsuedoCode1'].str.replace(" ","")
            ScrapeDF['PsuedoCode2'] = ScrapeDF['PsuedoCode2'].str.replace(" ","")
            print('Reindexing...')
            ScrapeDF = ScrapeDF.reset_index()
            print('Saving scrape data to our CPU excel files')
            ScrapeDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\' + date.today().strftime('%y-%m-%d')  + 'ScrapeDF.xlsx')            
            
            #Connect to our access db to grab comp item code matches
            print('Connecting to our manual match database')
            CPU_conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                r'DBQ=\\FOT00WEB\Alt Team\CPU\dbfiles\CPU.accdb;'
                )
            CPU_cnxn = pyodbc.connect(CPU_conn_str)
            query = "SELECT ModelReference1, Competitor, Brand, CpuAddDate, ItemCode AS 'PsuedoCode1' FROM NoMatches WHERE ItemCode <> ''"
            print('Grabbing manually matched items')
            CPUdf = pd.read_sql(query, CPU_cnxn)
            CPUdf = CPUdf.sort_values(by='CpuAddDate',na_position='first')
            CPUdf = CPUdf.drop_duplicates(subset=['ModelReference1','Competitor','Brand'], keep='last')

            #Bring in NoMatch CPU ItemCode (from CPUdf .... above) overrides into PsuedoCode...which is lookedup directly into itemcode ....
            if CPUdf.shape[0] > 0:
                
                print(CPUdf.shape[0])
                CPUdf = CPUdf.dropna(subset=['Brand'])
                CPUdf = CPUdf.dropna(subset=['Competitor'])
                CPUdf = CPUdf.dropna(subset=['ModelReference1'])
                print(CPUdf.shape[0])
                print('Updating Scrape data with matched items')
                ScrapeDF.set_index(['ModelReference1','Competitor','Brand']).update(CPUdf.set_index(['ModelReference1','Competitor','Brand']))
                

            query = "SELECT ModelReference1, Competitor, Brand, ItemCode AS 'PsuedoCode1' FROM NoMatches WHERE ItemCode = '' or ItemCode is null" #This is causing problesm
            query = "SELECT ModelReference1, Competitor, Brand, ItemCode AS 'PsuedoCode1' FROM NoMatches"
            print('Grabbing non-matched items')
            CPUdf = pd.read_sql(query, CPU_cnxn)
            CPU_cnxn.close()
            
            #Intitalize pricing logic dataframe
            pricingColumns = list(ScrapeDF) + list(SageDF)
            pricingDF = pd.DataFrame(data=None, columns=pricingColumns)
            #Join Scrape data and Sage data
            print('Matching Scrape ModelReference1')
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['ModelReference1'].notnull()], how='inner', left_on=['VendorNo','ProdLine'], right_on=['ModelReference1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['ModelReference1'].notnull()], how='inner', left_on=['DisplayModel','ProdLine'], right_on=['ModelReference1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['ModelReference1'].notnull()], how='inner', left_on=['Catalog','ProdLine'], right_on=['ModelReference1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['ModelReference1'].notnull()], how='inner', left_on=['UPC','ProdLine'], right_on=['ModelReference1','Brand']), sort=False)
            print('Matching Scrape Clean Stripped verison of ModelReference1')
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['CleanStrip1'].notnull()], how='inner', left_on=['VendorNo','ProdLine'], right_on=['CleanStrip1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['CleanStrip1'].notnull()], how='inner', left_on=['DisplayModel','ProdLine'], right_on=['CleanStrip1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['CleanStrip1'].notnull()], how='inner', left_on=['Catalog','ProdLine'], right_on=['CleanStrip1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['CleanStrip1'].notnull()], how='inner', left_on=['UPC','ProdLine'], right_on=['CleanStrip1','Brand']), sort=False)
            print('Matching Scrape Psuedo ItemCode verison of ModelReference1')
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode1'].notnull()], how='inner', left_on=['VendorNo','ProdLine'], right_on=['PsuedoCode1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode1'].notnull()], how='inner', left_on=['DisplayModel','ProdLine'], right_on=['PsuedoCode1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode1'].notnull()], how='inner', left_on=['Catalog','ProdLine'], right_on=['PsuedoCode1','Brand']), sort=False)
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode1'].notnull()], how='inner', left_on=['UPC','ProdLine'], right_on=['PsuedoCode1','Brand']), sort=False)    
            pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode1'].notnull()], how='inner', left_on=['ItemCode','ProdLine'], right_on=['PsuedoCode1','Brand']), sort=False)    
            if ScrapeDF.dropna(subset=['ModelReference2']).shape[0] > 0:
                print('Matching Scrape ModelReference2')
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['ModelReference2'].notnull()], how='inner', left_on=['VendorNo','ProdLine'], right_on=['ModelReference2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['ModelReference2'].notnull()], how='inner', left_on=['DisplayModel','ProdLine'], right_on=['ModelReference2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['ModelReference2'].notnull()], how='inner', left_on=['Catalog','ProdLine'], right_on=['ModelReference2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['ModelReference2'].notnull()], how='inner', left_on=['UPC','ProdLine'], right_on=['ModelReference2','Brand']), sort=False)
                print('Matching Scrape Clean Stripped verison of ModelReference2')
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['CleanStrip2'].notnull()], how='inner', left_on=['VendorNo','ProdLine'], right_on=['CleanStrip2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['CleanStrip2'].notnull()], how='inner', left_on=['DisplayModel','ProdLine'], right_on=['CleanStrip2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['CleanStrip2'].notnull()], how='inner', left_on=['Catalog','ProdLine'], right_on=['CleanStrip2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['CleanStrip2'].notnull()], how='inner', left_on=['UPC','ProdLine'], right_on=['CleanStrip2','Brand']), sort=False)
                print('Matching Scrape Psuedo ItemCode verison of ModelReference2')
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode2'].notnull()], how='inner', left_on=['VendorNo','ProdLine'], right_on=['PsuedoCode2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode2'].notnull()], how='inner', left_on=['DisplayModel','ProdLine'], right_on=['PsuedoCode2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode2'].notnull()], how='inner', left_on=['Catalog','ProdLine'], right_on=['PsuedoCode2','Brand']), sort=False)
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode2'].notnull()], how='inner', left_on=['UPC','ProdLine'], right_on=['PsuedoCode2','Brand']), sort=False)                
                pricingDF = pricingDF.append(pd.merge(SageDF, ScrapeDF[ScrapeDF['PsuedoCode2'].notnull()], how='inner', left_on=['ItemCode','ProdLine'], right_on=['PsuedoCode2','Brand']), sort=False)                
            #Only unique prices on items from competitors

            #Clear out Sage Dataframe from mem
            del SageDF
            SageDF = ''
            gc.collect()        

            #These are the columns we want to save back to aid in matching the scrape nomatches
            nomatchColumns = ['Competitor','Brand','ModelReference1','ModelReference2','Description','ProductUrl','PageUrl']
            #Bump scrape against all the matches...only keep those that do not match
            print('Determing scrape items that could not be matched in')
            NoMatchDF = ScrapeDF[nomatchColumns].merge(pricingDF[['ModelReference1','Competitor']].drop_duplicates(), left_on=['ModelReference1','Competitor'], right_on=['ModelReference1','Competitor'], how='left', suffixes=('', '_right'), indicator=True)
            NoMatchDF = NoMatchDF.query('_merge=="left_only"')
            NoMatchDF = NoMatchDF.filter(nomatchColumns)

            print("Sorting and recombining new non-matched items' Brand")
            NoMatchDF = NoMatchDF.sort_values(by=['Brand'])
            NoMatchDF = NoMatchDF.groupby(['Competitor','ModelReference1','ModelReference2','Description','ProductUrl','PageUrl'])['Brand'].apply(lambda x: ','.join(x.astype(str))).reset_index()
            NoMatchDF = NoMatchDF.reset_index()

            print('Auditing non-matched scrape items against existing non-matched')
            NoMatchDF = NoMatchDF.merge(CPUdf, left_on=['ModelReference1','Competitor','Brand'], right_on=['ModelReference1','Competitor','Brand'], how='left', suffixes=('', '_right'), indicator=True)
            NoMatchDF = NoMatchDF.loc[(NoMatchDF['_merge'] == 'left_only')] 
            NoMatchDF = NoMatchDF.filter(nomatchColumns)
            NoMatchDF = NoMatchDF.dropna(subset=['ModelReference1'])
            NoMatchDF = NoMatchDF.loc[(NoMatchDF['ModelReference1'] != '')] 
            
            if NoMatchDF.shape[0] > 0:
                #Recombine split brands...gotta sort 'em first
                print('New non-matched items need to be evaluted...')
                #add date
                NoMatchDF['CpuAddDate'] = date.today().strftime('%m/%d/%y') 

                print('WAIT.....Everyone needs a daddy task!')
                response = makeWrikeTask(title = "CPU " + date.today().strftime('%m/%d/%y') + " Daddy Task", description = 'I clean up the wrike house', assignees = assignees, folderid = folderid)
                print('DaddyTaskMade!')
                response_dict = json.loads(response.text)
                daddytaskid = response_dict['data'][0]['id']                   

                print('Connecting to NO Matches Database')
                cnn_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(CPU_conn_str)}"
                acc_engine = create_engine(cnn_url)

                print('writing to acccess')
                NoMatchDF.to_sql('NoMatches', acc_engine, if_exists='append')

                print('Attaching to the daddy Wrike Task')
                #Create No matches Wrike task
                NoMatchDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\NoMatchDF.xlsx')
                description = r"""
                Confirm that we cannot match these. Upload manually matched item codes here "\\FOT00WEB\Alt Team\CPU\dbfiles\CPU.accdb"
                """
                response = makeWrikeTask(title = "CPU " + date.today().strftime('%m/%d/%y')  + " No Matches (" + str(NoMatchDF.shape[0]) +")", description = description, assignees = assignees, folderid = folderid)
                response_dict = json.loads(response.text)
                print('No match wrike task made!')
                taskid = response_dict['data'][0]['id']
                makeWrikeTaskSubtask(taskid,daddytaskid)   
                filetoattachpath = '\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\NoMatchDF.xlsx'
                print('Attaching No Match file')
                attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)
                print('No Match file attached!')
            else:
                print('No non-matched items need to be evaluated! Hooray!')

                print('WAIT.....Everyone needs a daddy task!')
                response = makeWrikeTask(title = "CPU " + date.today().strftime('%m/%d/%y') + " Daddy Task", description = 'I clean up the wrike house', assignees = assignees, folderid = folderid)
                print('DaddyTaskMade!')
                response_dict = json.loads(response.text)
                daddytaskid = response_dict['data'][0]['id']                   

            #Weeding out inactive items
            pricingDF = pricingDF.loc[(pricingDF['InactiveItem'] != 'Y')] 

            #Weeding out other stuff we shouldn't mess with
            pricingDF = pricingDF.loc[(pricingDF['PackQty'] <= 1)] 
            pricingDF = pricingDF.loc[(pricingDF['PleaseCall'] != 'Y')] 
            pricingDF = pricingDF.loc[(pricingDF['SpecialOrder'] != 'Y')]  

            #Sorting most recent ItemCode Scrape from Competitor to the top
            print("Sorting most recent ItemCode Scrape from Competitor to the top")
            pricingDF.sort_values(inplace=True, by=['ItemCode', 'ScrapeDate', 'Competitor'], ascending=False)   
            pricingDF.drop_duplicates(subset=['ItemCode','ScrapeDate','Competitor'], inplace=True)                 

            print('dropping duplicate matches from pricing matches')
            pricingDF.drop_duplicates(subset=['ItemCode','Competitor','Price'], inplace=True)
            pricingDF.reset_index(inplace=True, drop=True)       

            #Adding some dates to reel in the scope of what gets evaluated...to what is new to the CPU
            #This is needed due to the Fact we decided to keep the scrapes in the bucket for a while
            #\\\\FOT00WEB\\Alt Team\\CPU\\
            last_run_time = pickle.load(open('\\\\FOT00WEB\\Alt Team\\CPU\\last_CPU_runtime.p','rb'))
            print(last_run_time)    
            #Tweeking scope to remove those they were already evaluated
            pricingDF['ItemLatestScrapeDate'] = pricingDF.groupby(['ItemCode'])['ScrapeDate'].transform('max')  
            pricingDF['LastCPURunDate'] = last_run_time - timedelta(days=34)
            print("Checking logic")
            print(pricingDF)
            pricingDF = pricingDF.query("ItemLatestScrapeDate >= LastCPURunDate")
            print(pricingDF)                 

            #BI queries
            print('Adding Evalution Date')
            pricingDF['EvaluationDate'] = date.today().strftime('%m/%d/%y') 
            print('Calculating Proposed Price')
            
            #Establish Proposed Price
            pricingDF['ProposedPrice'] = pricingDF['Price'].apply(np.floor) - .05
            pricingDF['ConservativePrice'] = pricingDF['Cost'] + ((pricingDF['SageMSRP'] - pricingDF['Cost']) * .8)
            pricingDF.loc[(pricingDF['PriceStatusCode'] == 'CONSERVE') & (pricingDF['ProposedPrice'] < pricingDF['ConservativePrice']), 'ProposedPrice'] = pricingDF['ConservativePrice']
            pricingDF['ProposedPrice'] = pricingDF.apply(proposed_price_check, axis=1)
            
            #Establish Shipping Effect
            print('Predicting Shipping Effect')
            pricingDF['ShippingEffect'] = pricingDF['ShipWeight'] * -2 / 3
            pricingDF.loc[pricingDF['ShipWeight'] < 100, 'ShippingEffect'] = pricingDF['ShipWeight'] * -3 / 4
            pricingDF.loc[pricingDF['ShipWeight'] < 45, 'ShippingEffect'] = pricingDF['ShipWeight'] * -4 / 5
            pricingDF.loc[pricingDF['ShipWeight'] < 25, 'ShippingEffect'] = pricingDF['ShipWeight'] * -1
            pricingDF.loc[pricingDF['ShipWeight'] < 20, 'ShippingEffect'] = pricingDF['ShipWeight'] * -7 / 6
            pricingDF.loc[pricingDF['ShipWeight'] < 15, 'ShippingEffect'] = pricingDF['ShipWeight'] * -7 / 5
            pricingDF.loc[pricingDF['ShipWeight'] < 10, 'ShippingEffect'] = pricingDF['ShipWeight'] * -2
            pricingDF.loc[pricingDF['ShipWeight'] < 7, 'ShippingEffect'] = pricingDF['ShipWeight'] * -2.5
            pricingDF.loc[pricingDF['ShipWeight'] < 5, 'ShippingEffect'] = pricingDF['ShipWeight'] * -2.8
            pricingDF.loc[pricingDF['ShipWeight'] < 4, 'ShippingEffect'] = pricingDF['ShipWeight'] * -4
            pricingDF.loc[pricingDF['ShipWeight'] < 3, 'ShippingEffect'] = pricingDF['ShipWeight'] * -5
            pricingDF.loc[pricingDF['ShipWeight'] < 2, 'ShippingEffect'] = pricingDF['ShipWeight'] * -7
            pricingDF.loc[pricingDF['ShipWeight'] < 1.5, 'ShippingEffect'] = pricingDF['ShipWeight'] * -9
            pricingDF.loc[pricingDF['ShipWeight'] <= 1, 'ShippingEffect'] = -9
            pricingDF.loc[pricingDF['ProposedPrice'] <= 75, 'ShippingEffect'] = 3
            print('Calculating Proposed Profit with Shipping Effect')
            pricingDF['ProposedProfitAfterShip'] = pricingDF['ProposedPrice'] - pricingDF['Cost'] + pricingDF['ShippingEffect']
            print('Calculating Margins')
            pricingDF['ProposedMargin'] = pricingDF['ProposedProfitAfterShip'] / pricingDF['ProposedPrice'] * 100
            pricingDF['SageGrossMargin'] = (pricingDF['ProposedPrice'] - pricingDF['Cost']) / pricingDF['ProposedPrice'] * 100
            print('Reviewing Current Profitability')
            pricingDF['CurrentProfitAfterShip'] = (pricingDF['SalePrice'] - pricingDF['Cost'] + pricingDF['ShippingEffect'])
            pricingDF['CurrentProfitMargin'] = pricingDF['CurrentProfitAfterShip'] / pricingDF['SalePrice'] * 100
            print('Rounding all price data')
            pricingDF = pricingDF.round({'CurrentProfitMargin': 1, 'SageGrossMargin': 1, 'ProposedMargin': 1})

            #Violators
            print('Comparing proposed price with GSA lowest')
            pricingDF['CompBelowGovLowest'] = ''
            pricingDF['CompBelowGovLowest'] = pricingDF.apply (gov_lowest_check, axis=1)
            print('Determining which competiors are violating ')
            pricingDF['CompBelowMAP'] = ''
            pricingDF['CompBelowMAP'] = pricingDF.apply (map_violate_check, axis=1)
            
            #Manual Review
            print('Determining which items need manual review')
            pricingDF['ManualPriceAdjustments'] = ''
            pricingDF.loc[(pricingDF['ProposedPrice'] < 1), 'ManualPriceAdjustments'] = 'Extremely Low Value Item'
            pricingDF.loc[((pricingDF['ProposedPrice']) / (pricingDF['SalePrice'])) < 0.8, 'ManualPriceAdjustments'] = "Proposed is 80 percent of current price"
            pricingDF.loc[((pricingDF['ProposedPrice']) / (pricingDF['SalePrice'])) > 1.4, 'ManualPriceAdjustments'] = "Proposed is 40 percent larger than current price"
            pricingDF.loc[(((pricingDF['ProposedPrice']) - (pricingDF['SalePrice'])) / (pricingDF['SalePrice'])) < -0.25, 'ManualPriceAdjustments'] = 'Over 25% Reduction'
            pricingDF.loc[(((pricingDF['ProposedPrice']) - (pricingDF['SalePrice'])) / (pricingDF['SalePrice'])) > 0.25, 'ManualPriceAdjustments'] = 'Over 25% Increase'
            pricingDF.loc[(((pricingDF['ProposedPrice']) - (pricingDF['Cost'])) / (pricingDF['ProposedPrice'])) < 0.05, 'ManualPriceAdjustments'] = 'Less than 5% margin'

            #Never CPU reprice these (this is probably over the top... pretty sure I have these set to return whatever sale price is already in Sage) ... actually maybe useful incase of a errant ItemCode Match
            pricingDF.loc[pricingDF['PriceStatusCode'] == 'LIQUIDATE', 'ManualPriceAdjustments'] = 'LIQUIDATE'
            pricingDF.loc[pricingDF['PriceStatusCode'] == 'LOGISTICS', 'ManualPriceAdjustments'] = 'LOGISTICS'
            pricingDF.loc[pricingDF['PriceStatusCode'] == 'PROMO', 'ManualPriceAdjustments'] = 'PROMO'

            #addingthisback -_-
            pricingDF['ManualPriceAdjustments'] = pricingDF.apply (price_increase_check, axis=1)            

            #Approved Reductions
            print('Determining which items are approved price reductions')
            pricingDF['ApprovedPriceReductions'] = pricingDF.apply(approved_reductions, axis=1)

            print(r'Saving BI logic to "Y:\CPU\excelfiles\pricingDF.xlsx"')
            pricingDF.reset_index(inplace=True, drop=True)
            pricingDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\pricingDF.xlsx')

            MapViolatorsDF = pricingDF.loc[(pricingDF['CompBelowMAP'] != '')] 
            MapViolatorsDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\MapViolatorsDF.xlsx')

            ApprovedPriceReductionsDF = pricingDF.loc[(pricingDF['ApprovedPriceReductions'] != '') & (pricingDF['CompBelowGovLowest'] == '') & (pricingDF['ManualPriceAdjustments'] == '')]   
            ApprovedPriceReductionsDF = ApprovedPriceReductionsDF.sort_values(by=['ProposedPrice'], ascending=True)
            ApprovedPriceReductionsDF = ApprovedPriceReductionsDF.drop_duplicates(subset=['ItemCode'])
            ApprovedPriceReductionsDF = ApprovedPriceReductionsDF.query('ProposedPrice != SalePrice')
            ApprovedPriceReductionsDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\ApprovedPriceReductionsDF.xlsx')



            ManualPriceAdjustmentsDF = pricingDF.loc[(pricingDF['ManualPriceAdjustments'] != '') & (pricingDF['ManualPriceAdjustments'] != 'LIQUIDATE') & (pricingDF['ManualPriceAdjustments'] != 'PROMO') & (pricingDF['ManualPriceAdjustments'] != 'LOGISTICS') & (pricingDF['ApprovedPriceReductions'] != '')] 

            #Approved Reductions
            print('Determining which items are approved price reductions')
            #ManualPriceAdjustmentsDF['ApprovedPriceReductions'] = ManualPriceAdjustmentsDF.apply (approved_reductions, axis=1)

            #, columns = ['Competitor','Brand','ModelReference1','ModelReference2','Price','MSRP','ScrapeDate','VendorNo','ItemCode','PriceStatusCode','ProductType','SageMSRP','MAP','SalePrice','Cost','Gov_Lowest','TotalQtyOH','ProposedPrice','ShippingEffect','ProposedProfitAfterShip','ProposedMargin','SageGrossMargin','ManualPriceAdjustments','PriceReductionsThatNeedApproval','PriceIncreasesThatNeedApproval','ApprovedPriceReductions','ProposedPricesThatNeedReview']
            ManualPriceAdjustmentsDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\ManualPriceAdjustmentsDF.xlsx')


            #Violators task
            print('Determining which competitors are MAP violating')
            
            if (MapViolatorsDF.shape[0] > 0) and (date.today().weekday() == 1): 
                print('MAP Violators Found! Making Wrike task...')
                MapViolatorsDF.reset_index(inplace=True, drop=True)        
                MapViolatorsDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\' + date.today().strftime('%y-%m-%d') + 'MapViolatorsDF.xlsx')
                description = r"""Report these MAP violators in the attached file. 
                    If no file is attached then, there was too many to report via the Wrike API.
                    In that case you can find the file here Y:\CPU\excelfiles\TASKDATEMapViolatorsDF.xlsx
                    NOTE: When checking for MAP violations on Fluke, make sure they are in a product family that has a MAP policy. You can find these on the price list. AC - 2022-09-08
                    """.replace('TASKDATE',date.today().strftime('%y-%m-%d'))
                response = makeWrikeTask(title = "CPU " + date.today().strftime('%m/%d/%y') + " MAP violators" + " (" + str(MapViolatorsDF.shape[0]) + ")", description = description, assignees = assignees, folderid = folderid)
                print('MAP Violating Wrike Task Made!')
                response_dict = json.loads(response.text)
                taskid = response_dict['data'][0]['id']
                makeWrikeTaskSubtask(taskid,daddytaskid)   
                filetoattachpath = '\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\' + date.today().strftime('%y-%m-%d') + 'MapViolatorsDF.xlsx'
                print('Attaching file')
                attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
                print('File attached!') #probably should have a handle for this...can't attach too big
            
            #Manual Adustments task
            print('Determining which scraped items require manual review')
            
            if ManualPriceAdjustmentsDF.shape[0] > 0:
                print('Manual Review Items Found! Making Wrike task...')
                ManualPriceAdjustmentsDF.reset_index(inplace=True, drop=True)        
                ManualPriceAdjustmentsDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\' + date.today().strftime('%y-%m-%d') + 'ManualPriceAdjustments.xlsx')    
                description = "Please evaulate these Manual Adjustment prices "
                response = makeWrikeTask(title = "CPU " + date.today().strftime('%m/%d/%y') + " Manual Adjustment Pricing" + " (" + str(ManualPriceAdjustmentsDF.shape[0]) + ")", description = description, assignees = assignees, folderid = folderid)
                print('Manual Review Items Wrike Task Made!')
                response_dict = json.loads(response.text)
                taskid = response_dict['data'][0]['id']
                makeWrikeTaskSubtask(taskid,daddytaskid)   
                filetoattachpath = '\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\' + date.today().strftime('%y-%m-%d') + 'ManualPriceAdjustments.xlsx'
                print('Attaching file')
                attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)
                print('File attached!') #probably should have a handle for the...can't attach too big

            #Approved Reductions _ in meantime we will have these evaluated
            print('Determining which scraped are approved for price reductions')
             
            if ApprovedPriceReductionsDF.shape[0] > 0:
                print('Approved Price Reduction Items Found! Making Wrike task...')
                ApprovedPriceReductionsDF = ApprovedPriceReductionsDF.reset_index(drop=True)   

                print('Creating Sage upload file') 
                ApprovedPriceReductionsSageUploadDF = ApprovedPriceReductionsDF[['ItemCode','ProposedPrice','SageGrossMargin','EvaluationDate']]
                csvfilepath = r'\\FOT00WEB\Alt Team\Qarl\Automatic VI Jobs\Maintenance\CSVs\Repricing.csv'
                ApprovedPriceReductionsSageUploadDF.to_csv(csvfilepath, index=False, header=False)
                print('Creating Approved Price Reduction Items file for review') 
                ApprovedPriceReductionsDF.to_excel('\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\' + date.today().strftime('%y-%m-%d') + 'ApprovedPriceReductions.xlsx')
                description = """
                The CPU found items which it can automatically reprice. Attached are two files, one is the sage upload and the other is the CPU workbook. \n
                ("\\FOT00WEB\Alt Team\Qarl\Automatic VI Jobs\Maintenance\CSVs\Repricing.csv")\n
                ("\\FOT00WEB\Alt Team\CPU\excelfiles\pricingDF.xlsx")\n
                The CPU should automatically push these prices to sage. After this task is created, the Sage VI job will start. The CPU should mark this task complete whenever it finishes.\n
                If the task isn't marked complete, we likely need to try re-pushing the VI job as it may be frozen, 'AA_REPRICER' aka 'VIW5L'). Before you do make sure the file here was actually updated\n
                ("\\FOT00WEB\Alt Team\Qarl\Automatic VI Jobs\Maintenance\CSVs\Repricing.csv"), otherwise you will be pushing data from a prior evalution.\n
                Ask Andrew if you have any questions/concerns!
                """
                print('Creating Wrike Task')
                response = makeWrikeTask(title = "CPU " + date.today().strftime('%m/%d/%y') + " Already Pushed Priced Reductions" + " (" + str(ApprovedPriceReductionsDF.shape[0]) + ")", description = description, assignees = assignees, folderid = folderid)
                print('Wrike Task Created!')
                response_dict = json.loads(response.text)
                taskid = response_dict['data'][0]['id']
                makeWrikeTaskSubtask(taskid,daddytaskid)   
                filetoattachpath = '\\\\FOT00WEB\\Alt Team\\CPU\\excelfiles\\' + date.today().strftime('%y-%m-%d') + 'ApprovedPriceReductions.xlsx'
                attachWrikeTask(attachmentpath = csvfilepath, taskid = taskid)   
                print('Sage uploaded file attached to wrike task')
                attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)       
                print('Approved Price Reduction Items file for review file attached to wrike task')

                #Auto VI .... uncomment  below to turn on....untested
                print('Sleeping for 1 minute')
                time.sleep(60)
                print('Attempting VI to Sage for approved price reductions')
                
                #removed 'r' tag in front of cwd string....dunnon if that was the issue AT 2022-05-26
                p = Popen('Auto_REPRICER_VIWI5K.bat', cwd= 'Y:\\Qarl\Automatic VI Jobs\\Maintenance', shell = True)
                stdout, stderr = p.communicate()   
                p.wait()

                print('Sleeping for 5 more minutes')
                time.sleep(300)                                

                print('Sage VI Complete!')
                markWrikeTaskComplete (taskid)
                print('Wrike Task Marked Complete')

            else:
                print('Creating Wrike Task')
                description = """
                The CPU couldn't find any items in need of pricing adjustments
                """                
                response = makeWrikeTask(title = "CPU " + date.today().strftime('%m/%d/%y') + " There are no Approved Priced Reductions Today", description = description, assignees = assignees, folderid = folderid)
                print('Wrike Task Created!')       
                response_dict = json.loads(response.text)
                taskid = response_dict['data'][0]['id']    
                makeWrikeTaskSubtask(taskid,daddytaskid)    
                markWrikeTaskComplete (taskid)
                print('Wrike Task Marked Complete')

    #Save Todays Runtime for the next run
    current_run_time = datetime.date(datetime.now())
    print(current_run_time)    
    with open('\\\\FOT00WEB\\Alt Team\\CPU\\last_CPU_runtime.p', 'wb') as f:
        pickle.dump(current_run_time, f)
    f.close()          