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
	const { fieldData, types } = field.type.reduce(({ fieldData, types }, type, index) => {
		const typeField = Object.assign({}, fieldData, { type });
		const updatedData = adaptType(typeField);
		types[index] = type;

		return {
			fieldData: updatedData,
			types
		};
	}, { fieldData: field, types: field.type });

	const uniqTypes =  _.uniq(types);
	if (uniqTypes.length === 1) {
		return fieldData;
	}

	return Object.assign({}, fieldData, {type: uniqTypes});
};

const handleEmptyDefault = field => {
	const typesWithoutDefault = ['bytes', 'fixed', 'record', 'array', 'map', 'null'];
	const hasDefault = !_.isUndefined(field.default) && field.default !== '';
	const isMultiple = _.isArray(field.types);
	if (isMultiple && field.types.every(type => typesWithoutDefault.includes(type))) {
		return field;
	}

	if (hasDefault || typesWithoutDefault.includes(field.type)) {
		return field;
	}

	return Object.assign({}, field, {
		error: {
			"default": true
		}
	});
};

const handleEmptyDefaultInProperties = field => {
	let required = _.get(field, 'required', []);

	if (!_.isPlainObject(field.properties)) {
		return field;
	}

	const updatedProperties = Object.keys(field.properties).reduce((properties, key) => {
		const property = field.properties[key];

		if (required.includes(key)) {
			return Object.assign({}, properties, {
				[key]: property
			});
		}

		const updatedProperty = handleEmptyDefault(property);
		if (property !== updatedProperty) {
			required = required.filter(name => name !== key);
		}

		return Object.assign({}, properties, {
			[key]: updatedProperty
		});
	
	}, {});

	return Object.assign({}, field, {
		properties: updatedProperties,
		required
	});
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

const populateDefaultNullValuesForMultiple = field => {
	if (!_.isArray(field.type))	{
		return field;
	}
	if (_.first(field.type) !== 'null') {
		return field;
	}

	return Object.assign({}, field, { default: null });
};

const adaptJsonSchema = jsonSchema => {
	return mapJsonSchema(jsonSchema, jsonSchemaItem => {
		return _.flow([
			adaptType,
			populateDefaultNullValuesForMultiple,
			handleEmptyDefaultInProperties
		])(jsonSchemaItem);
	});
};

module.exports = adaptJsonSchema;
