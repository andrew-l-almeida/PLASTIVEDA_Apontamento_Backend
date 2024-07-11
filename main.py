import databases

from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=['1 per day'],
    storage_uri="memory://"
)

CORS(app,resources={r"/*": {"origins": ["*"], "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"]}}, supports_credentials=True)


@app.route('/getOrder/<int:cd_Ped>', methods=['GET'])
@limiter.exempt
def getOrder(cd_Ped):
    orders = databases.areco.get_order(cd_Ped)
    return jsonify({'orders': orders})

@app.route('/getOrders', methods = ['GET'])
@limiter.limit('5 per second')
def getOrders():
        orders = databases.areco.get_orders()
        return jsonify({'orders': orders})
    
@app.route('/getOrderDetails/<int:id_ped>', methods=['GET'])
@limiter.exempt
def getOrderDetails(id_ped):
    order_details = databases.areco.get_order_details(id_ped)
    return jsonify({'order': order_details})

@app.route('/getOfDetails/<int:id_of>', methods=['GET'])
@limiter.exempt
def getOfDetails(id_of):
    of_details = databases.areco.get_of_details(id_of)
    return jsonify({'of_details': of_details})


@app.route('/getOperators', methods=['GET'])
@limiter.exempt
def getOperators():
    setores = databases.areco.get_operators()
    return jsonify({'setores': setores})

@app.route('/getMachines', methods=['GET'])
@limiter.exempt
def getMachines():
    machines = databases.areco.get_machines()
    return jsonify({'machines': machines})

@app.route('/insertNewRecord', methods=['POST'])
@limiter.exempt
def insertNewRecord():
    data = request.json
    
    new_record = databases.areco.insert_new_record(data)
    print(new_record)
    return jsonify({'id_apontProd': new_record})

@app.route('/finalizeRecord', methods=['POST'])
@limiter.exempt
def finalizeRecord():
    data = request.json
    
    finalizedRecord = databases.areco.finalize_record(data)
    
    return jsonify({'id_apontProd': finalizedRecord})

@app.route('/getRawMaterial/<int:id_of>', methods=['GET'])
@limiter.exempt
def getRawMaterial(id_of):
    raw_material = databases.areco.get_raw_material(id_of)
    return jsonify({'rawMaterial': raw_material})

@app.route('/getAuxiliarOrders/<int:id_of>', methods=['GET'])
@limiter.exempt
def getAuxiliarOrders(id_of):
    auxiliarOrders = databases.areco.get_auxiliar_orders(id_of)
    
    return jsonify({'auxiliarOrders': auxiliarOrders})
    
    
if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)