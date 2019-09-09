const mapJsonSchema = require('./mapJsonSchema');
const _ = require('lodash');

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

const handleNumber = field => {
	return Object.assign({}, field, {
		type: 'bytes',
		logicalType: 'decimal'
	});
};

const handleInt = field => {
	return Object.assign({}, field, {
		type: 'number',
		mode: 'int'
	});
};

const handleStringFormat = field => {
	switch(field.format) {
		case 'date':
			return handleDate(field);
		case 'time':
			return handleTime(field);
		case 'date-time':
			return handleDateTime(field);
		default:
			return field;
	};
};

const adaptMultiple = field => {
	const { fieldData, types } = field.type.reduce(({ fieldData, types }, type) => {
		const typeField = Object.assign({}, fieldData, { type });
		const updatedData = adaptType(typeField);
		const updatedTypes = types.map(initialType => {
			if (initialType === type) {
				return updatedData.type;
			}
			return initialType;
		});

		return {
			fieldData: updatedData,
			types: _.uniq(updatedTypes)
		};
	}, { fieldData: field, types: field.type });

	if (types.length === 1) {
		return fieldData;
	}

	return Object.assign({}, fieldData, {type: types});
};

const adaptType = field => {
	const type = field.type;

	if (_.isArray(type)) {
		return adaptMultiple(field);
	}

	if (type === 'string') {
		return handleStringFormat(field);
	}

	if (type === 'number') {
		return handleNumber(field);
	}

	if (type === 'integer' || type === 'int') {
		return handleInt(field);
	}

	return field;
};

const adaptJsonSchema = jsonSchema => {
	return mapJsonSchema(jsonSchema, jsonSchemaItem => {
		return adaptType(jsonSchemaItem);
	});
};

module.exports = adaptJsonSchema;
