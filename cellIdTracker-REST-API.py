
from os import environ
from flask import Flask, request, jsonify, Blueprint
from marshmallow_mongoengine import ModelSchema, fields
from marshmallow.exceptions import ValidationError
import mongoengine as me

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
routing_blueprint = Blueprint('routing', __name__, template_folder='templates')

if 'MONGODB_DATABASE' not in environ:
    print("MONGODB_DATABASE not set, exiting...")
    exit(1)

if 'MONGODB_USER' in environ and 'MONGODB_PASSWORD' in environ:
    me.connect(environ.get('MONGODB_DATABASE'),
               host=environ.get('MONGODB_HOST', 'localhost'),
               port=int(environ.get('MONGODB_PORT', '27017')),
               username=environ.get('MONGODB_USER'),
               password=environ.get('MONGODB_PASSWORD'),
               connect=False)
else:
    me.connect(environ.get('MONGODB_DATABASE'),
               host=environ.get('MONGODB_HOST', 'localhost'),
               port=int(environ.get('MONGODB_PORT', '27017')),
               connect=False)


class Source(me.Document):
    meta = {'collection': 'sources'}
    imei = me.StringField(required=True)
    imsi = me.StringField(required=True)


class CellIdentity(me.DynamicEmbeddedDocument):
    pass


class SignalStrength(me.DynamicEmbeddedDocument):
    pass


class CellInfo(me.EmbeddedDocument):
    active = me.BooleanField(required=True)
    type = me.StringField(required=True, choices=['LTE', 'UMTS', 'CDMA', 'GSM'])
    # TODO: find a way to get this into an EmbeddedDocument again
    # cell_identity = me.EmbeddedDocumentField(CellIdentity, required=True)
    # signal_strength = me.EmbeddedDocumentField(SignalStrength,required=True)
    cell_identity = me.DictField(required=True)
    cell_signal_strength = me.DictField(required=True)


class LocationInformation(me.EmbeddedDocument):
    latitude = me.FloatField(required=True)
    longitude = me.FloatField(required=True)
    accuracy = me.FloatField(required=True)


class Measurement(me.Document):
    meta = {'collection': 'measurements'}
    source_id = me.ObjectIdField(required=True)
    timestamp = me.DateTimeField(required=True)
    location_information = me.EmbeddedDocumentField(LocationInformation)
    battery = me.FloatField(required=True)
    cell_info = me.EmbeddedDocumentListField(CellInfo)


class SourceSchema(ModelSchema):
    class Meta:
        model = Source
        strict = True


class AuthSchema(SourceSchema):
    psk = fields.String(required=True)

    def handle_error(self, error, data):
        if 'psk' in error.field_names:
            error.status_code = 401
        raise error


@app.errorhandler(ValidationError)
def handle_validation_error(error):
    try:
        status_code = error.status_code
    except AttributeError:
        status_code = 400

    return (jsonify(status=status_code,
                    message=error.messages),
            status_code)
    pass


class MeasurementSchema(ModelSchema):
    class Meta:
        model = Measurement
        strict = True


source_schema = SourceSchema()
auth_schema = AuthSchema()
measurement_schema = MeasurementSchema()


@routing_blueprint.route('/auth', methods=['POST'])
def auth():
    auth_schema.validate(request.json)
    source = source_schema.load(request.json).data

    if request.json['psk'] != environ.get('AUTH_PSK', 'defaultpsk'):
        return (jsonify(status=403,
                        message="No or wrong PSK provided."),
                403)

    try:
        existing_source_record = Source.objects.get(imei=source.imei,
                                                    imsi=source.imsi)

        return (jsonify(status=200,
                        source_id=str(existing_source_record.id)),
                200)
    except me.DoesNotExist:
        source.save()

        return (jsonify(status=201,
                        source_id=str(source.id)),
                201)


@routing_blueprint.route('/measurements', methods=['POST'])
def post_measurement():
    measurement = measurement_schema.load(request.json).data
    measurement.save()

    return (jsonify(status=201),
            201)


app.register_blueprint(routing_blueprint, url_prefix=environ.get('API_ROOT', ''))

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
