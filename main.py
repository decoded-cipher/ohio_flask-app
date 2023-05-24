from flask import Flask, request, json, make_response
import sqlite3
import pandas as pd

app = Flask(__name__)


# ----------------------------------------------


# loading csv data into sqlite3 database
db_connection = sqlite3.connect('oil_data.db')
csv_workbook = pd.read_csv('oil_data.csv')

csv_workbook.to_sql(name='oil_data', con=db_connection, if_exists='replace', index=True)
db_connection.commit()
db_connection.close()


# ----------------------------------------------


# base route for the api
@app.route('/')
def getBaseRoot():
    return make_response({'message': 'Welcome to the Oil Data API'}, 200)


# ----------------------------------------------


# data route for the api to get the annual production of a well by api_wellNumber(well) as a query parameter
@app.route('/data', methods=['GET'])
def getAnnualProduction():
    well_id = request.args.get('well')
    
    db_connection = sqlite3.connect('oil_data.db')
    df = pd.read_sql_query("SELECT * FROM oil_data WHERE api_wellNumber = ?", db_connection, params=[well_id])
    db_connection.close()

    annualSales = json.loads(df.to_json(orient='records'))

    if len(annualSales) == 0:
        # if no well is found with the given api_wellNumber
        return make_response({'error': 'No such well found'}, 404)
    else:
        oil = 0
        gas = 0
        brine = 0

        for i in range(len(annualSales)):
            annualSales[i]['oil'] = int(annualSales[i]['oil'].replace(',', ''))
            annualSales[i]['gas'] = int(annualSales[i]['gas'].replace(',', ''))
            annualSales[i]['brine'] = int(annualSales[i]['brine'].replace(',', ''))

        for i in range(len(annualSales)):
            oil += annualSales[i]['oil']
            gas += annualSales[i]['gas']
            brine += annualSales[i]['brine']
        
        # returning the annual production of the well
        return make_response({'oil': oil, 'gas': gas, 'brine': brine}, 200)


# ----------------------------------------------


# route to handle invalid routes
@app.route('/<path:path>')
def handlingInvalidRoutes(path):
    return make_response({'error': 'Invalid route'}, 404)


# ----------------------------------------------


# app running on custom port 8080
if __name__ == '__main__':
    app.run(debug=True, port=8080)