from flask import Flask, request
from flask_restful import Api, Resource
import json
from models.span.get_span import get_span_by_filters
app = Flask(__name__)

api = Api(app)

class Span(Resource):
    def get(self):
        filters = []
        args = dict(request.args)
        for key in args:
            arg = json.loads(args[key])
            filters.append((key, arg[0], arg[1], arg[2]))
        return get_span_by_filters(*filters)

api.add_resource(Span, '/spans')

if __name__ == "__main__":
    # Only for debugging while developing
    app.run(host='0.0.0.0', debug=True, port=5000)