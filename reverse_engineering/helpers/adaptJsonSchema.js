const mapJsonSchema = require('./mapJsonSchema');

const handleDate = field => {
	return Object.assign({}, field, {
		type: 'number',
		mode: 'int',
		logicalType: 'date'
	});
};

const handleTime = field => {
	return Object.assign({}, field, {
		type: 'number',
		mode: 'int',
		logicalType: 'time-millis'
	});
};

const handleDateTime = field => {
	return Object.assign({}, field, {
		type: 'number',
		mode: 'long',
		logicalType: 'timestamp-millis'
	});
};


const adaptJsonSchema = jsonSchema => {
	return mapJsonSchema(jsonSchema, jsonSchemaItem => {
		if (jsonSchemaItem.type !== 'string') {
			return jsonSchemaItem;
		}

		switch(jsonSchemaItem.format) {
			case 'date': 
				return handleDate(jsonSchemaItem);
			case 'time': 
				return handleTime(jsonSchemaItem);
			case 'date-time': 
				return handleDateTime(jsonSchemaItem);
			default:
				return jsonSchemaItem;
		};
	});
};

module.exports = adaptJsonSchema;
