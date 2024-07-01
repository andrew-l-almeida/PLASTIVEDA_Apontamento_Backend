import databases

from flask import Flask, jsonify
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
    
@app.route('/getOrderDetails/<int:id_ped>')
@limiter.exempt
def getOrderDetails(id_ped):
    order_details = databases.areco.get_order_details(id_ped)
    return jsonify({'order': order_details})

@app.route('/getOfDetails/<int:id_of>')
@limiter.exempt
def getOfDetails(id_of):
    of_details = databases.areco.get_of_details(id_of)
    return jsonify({'of_details': of_details})
    
if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)