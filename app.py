from flask import request, jsonify
from flask_restful import Api

api_bp = Api(name='finance_assistant')

@api_bp.route('/rewards_categories', methods=['POST'])
def add_rewards_category():
    """Add a new rewards category."""
    data = request.get_json()
    if not data or 'name' not in data:
        return jsonify({'error': 'Missing category name'}), 400

    category = {'name': data['name']}
    # Correctly call the public method
    data_manager.add_rewards_category(category)
    return jsonify({'message': 'Reward category added successfully'}), 201

@api_bp.route('/rewards_payees', methods=['POST'])
def add_rewards_payee():
    """Add a new rewards payee."""
    data = request.get_json()
    if not data or 'name' not in data:
        return jsonify({'error': 'Missing payee name'}), 400

    payee = {'name': data['name']}
    # Correctly call the public method
    data_manager.add_rewards_payee(payee)
    return jsonify({'message': 'Reward payee added successfully'}), 201