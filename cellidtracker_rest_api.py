from os import environ
from flask import Flask, request, jsonify, Blueprint
from marshmallow_mongoengine import ModelSchema, fields
from marshmallow import validate, validates_schema, ValidationError
import mongoengine as me


app = Flask(__name__)
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
    readable_name = me.StringField(required=True)


class CellIdentity(me.DynamicEmbeddedDocument):
    pass


class SignalStrength(me.DynamicEmbeddedDocument):
    pass


class CellInfo(me.EmbeddedDocument):
    active = me.BooleanField(required=True)
    type = me.StringField(required=True, choices=['LTE', 'UMTS', 'CDMA', 'GSM'])
    cell_identity = me.DictField(required=True)
    cell_signal_strength = me.DictField(required=True)


class LocationInformation(me.EmbeddedDocument):
    latitude = me.FloatField(required=True)
    longitude = me.FloatField(required=True)
    accuracy = me.FloatField(required=True)
    altitude = me.FloatField(required=False)
    age = me.IntField(required=True)


class Measurement(me.Document):
    meta = {'collection': 'measurements'}
    version = me.StringField(required=True)
    source_id = me.ObjectIdField(required=True)
    timestamp = me.DateTimeField(required=True)
    location_information = me.EmbeddedDocumentField(LocationInformation)
    battery = me.FloatField(required=True)
    cell_info = me.EmbeddedDocumentListField(CellInfo)


class ValidationModelSchema(ModelSchema):
    @validates_schema(pass_original=True)
    def check_unknown_fields(self, data, original_data):
        unexpected = set(original_data) - set(self.fields)
        if unexpected:
            raise ValidationError('Received data for unexpected field.', unexpected)


class SourceSchema(ValidationModelSchema):
    class Meta:
        model = Source


class AuthSchema(SourceSchema):
    psk = fields.String(required=True)

    def handle_error(self, error, data):
        if 'psk' in error.field_names:
            error.status_code = 401
        raise error


class MeasurementSchema(ValidationModelSchema):
    class Meta:
        model = Measurement


source_schema = SourceSchema()
auth_schema = AuthSchema()
measurement_schema = MeasurementSchema()


@app.errorhandler(me.ValidationError)
@app.errorhandler(ValidationError)
def handle_validation_error(error):
    try:
        status_code = error.status_code
    except AttributeError:
        status_code = 400

    error_message = error.message if isinstance(error, me.ValidationError) else error.messages

    return (jsonify(status=status_code,
                    message=error_message),
            status_code)


@app.errorhandler(Exception)
def handle_generic_error(error):
    return (jsonify(status=500,
                    message=str(error)),
            500)


@routing_blueprint.route('/auth', methods=['POST'])
def auth():
    auth_schema.validate(request.json)
    source = source_schema.load({key: request.json[key] for key in request.json if key != 'psk'})

    if request.json['psk'] != environ.get('AUTH_PSK', 'defaultpsk'):
        return (jsonify(status=403,
                        message="No or wrong PSK provided."),
                403)

    try:
        existing_source_record = Source.objects.get(imei=source.imei,
                                                    imsi=source.imsi)

        # update readable name in case it changed
        existing_source_record.readable_name = source.readable_name
        existing_source_record.save()

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
    measurement = measurement_schema.load(request.json)
    measurement.save()

    return (jsonify(status=201),
            201)


class GetMeasurementSchema(ModelSchema):
    measurement_fields = fields.String()


get_measurement_schema = GetMeasurementSchema()


@routing_blueprint.route('/measurement/<measurement_id>', methods=['GET'])
def get_measurement(measurement_id):
    parameters = get_measurement_schema.load(request.args)

    try:
        measurement = Measurement.objects(id=measurement_id)
        if 'measurement_fields' in parameters:
            measurement = measurement.only(*parameters['measurement_fields'].split(','))

        return (jsonify(status=200,
                        result=measurement_schema.dump(measurement.get())),
                200)

    except me.DoesNotExist:
        return (jsonify(status=404,
                        message="Measurement with object id {0} was not found.".format(measurement_id)),
                404)


class GetMeasurementsSchema(ModelSchema):
    latitude_upper_bound = fields.Float(required=True)
    latitude_lower_bound = fields.Float(required=True)
    longitude_lower_bound = fields.Float(required=True)
    longitude_upper_bound = fields.Float(required=True)
    min_location_age = fields.Integer(validate=validate.Range(min=0))
    max_location_age = fields.Integer(validate=validate.Range(min=0))
    min_location_accuracy = fields.Float(validate=validate.Range(min=0))
    max_location_accuracy = fields.Float(validate=validate.Range(min=0))
    measurement_fields = fields.String()

    @validates_schema
    def validate_coordinates(self, data):
        if data['latitude_upper_bound'] < data['latitude_lower_bound']:
            raise ValidationError("latitude_upper_bound must be greater than latitude_lower_bound.")
        if data['longitude_lower_bound'] > data['longitude_upper_bound']:
            raise ValidationError("longitude_lower_bound must be smaller than longitude_upper_bound.")

        if ('min_location_age' in data and 'max_location_age' in data and
                data['min_location_age'] > data['max_location_age']):
            raise ValidationError("min_location_age must be smaller than max_location_age.")

        if ('min_location_accuracy' in data and 'max_location_accuracy' in data and
                data['min_location_accuracy'] > data['max_location_accuracy']):
            raise ValidationError("min_location_accuracy must be smaller than max_location_accuracy.")


get_measurements_schema = GetMeasurementsSchema()


@routing_blueprint.route('/measurements', methods=['GET'])
def get_measurements():
    parameters = get_measurements_schema.load(request.args)
    mongoquery_parameters = {
        'location_information__latitude__lte': parameters['latitude_upper_bound'],
        'location_information__latitude__gte': parameters['latitude_lower_bound'],
        'location_information__longitude__gte': parameters['longitude_lower_bound'],
        'location_information__longitude__lte': parameters['longitude_upper_bound']
    }
    if 'min_location_age' in parameters:
        mongoquery_parameters['location_information__age__gte'] = parameters['min_location_age']
    if 'max_location_age' in parameters:
        mongoquery_parameters['location_information__age__lte'] = parameters['max_location_age']
    if 'min_location_accuracy' in parameters:
        mongoquery_parameters['location_information__accuracy__gte'] = parameters['min_location_accuracy']
    if 'max_location_accuracy' in parameters:
        mongoquery_parameters['location_information__accuracy__lte'] = parameters['max_location_accuracy']

    measurements = Measurement.objects(**mongoquery_parameters)
    if 'measurement_fields' in parameters:
        measurements = measurements.only(*parameters['measurement_fields'].split(','))

    return (jsonify(status=200,
                    len=len(measurements),
                    results=measurement_schema.dump(measurements.all(), many=True)),
            200)


app.register_blueprint(routing_blueprint, url_prefix=environ.get('API_ROOT', ''))

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)

# TODO: make marshmallow schema validation work in nested elements (only works in top-level currently)
# TODO: cell_identity and cell_signal_strength as DynamicEmbeddedDocuments (currently unknown/dynamic attributes are not persisted to database)
# TODO: make geographic cutouts handle map edges (switch from negative to positive latitude) correctly
