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
	if (field.mode || field.logicalType) {
		return field;
	}

	return Object.assign({}, field, {
		type: 'bytes',
		subtype: 'decimal'
	});
};

const handleInt = field => {
	return Object.assign({}, field, {
		type: 'number',
		mode: 'int'
	});
};

const handleStringFormat = field => {
	const { format, ...fieldData } = field;

	switch(format) {
		case 'date':
			return handleDate(fieldData);
		case 'time':
			return handleTime(fieldData);
		case 'date-time':
			return handleDateTime(fieldData);
		default:
			return field;
	};
};

const adaptMultiple = field => {
	const { fieldData, types } = field.type.reduce(({ fieldData, types }, type, index) => {
		const typeField = Object.assign({}, fieldData, { type });
		const updatedData = adaptType(typeField);
		types[index] = updatedData.type;

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
	const typesWithoutDefault = ['bytes', 'fixed', 'object', 'record', 'array', 'map', 'null'];
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

const adaptTitle = jsonSchema => {
	if (!jsonSchema.title) {
		return jsonSchema;
	}

	return Object.assign({}, jsonSchema, {
		title: convertToValidAvroName(jsonSchema.title)
	});
};

const adaptRequiredNames = jsonSchema => {
	if (!_.isArray(jsonSchema.required)) {
		return jsonSchema;
	}

	return Object.assign({}, jsonSchema, {
		required: jsonSchema.required.map(convertToValidAvroName)
	});
};

const adaptPropertiesNames = jsonSchema => {
	if (!_.isPlainObject(jsonSchema)) {
		return jsonSchema;
	}

	const propertiesKeys = [ 'properties', 'definitions', 'patternProperties' ];
	
	const adaptedSchema = adaptRequiredNames(jsonSchema);

	return propertiesKeys.reduce((schema, propertyKey) => {
		const properties = schema[propertyKey];
		if (_.isEmpty(properties)) {
			return schema;
		}

		const adaptedProperties = Object.keys(properties).reduce((adaptedProperties, key) => {
			if (key === '$ref') {
				return Object.assign({}, adaptedProperties, {
					[key]: convertReferenceName(properties[key])
				})
			}

			const updatedKey = convertToValidAvroName(key);
			const adaptedProperty = adaptPropertiesNames(properties[key]);

			return Object.assign({}, adaptedProperties, {
				[updatedKey]: adaptedProperty
			});
		}, {});

		return Object.assign({}, schema, {
			[propertyKey]: adaptedProperties
		});
	}, adaptedSchema);
};

const adaptNames = _.flow([
	adaptTitle,
	adaptPropertiesNames
]);

const convertReferenceName = ref => {
	if (!_.isString(ref)) {
		return ref;
	}

	const refNames = ref.split('/');
	const referenceName = _.last(refNames);
	const adaptedReferenceName = convertToValidAvroName(referenceName);

	return refNames.slice(0, -1).concat(adaptedReferenceName).join('/');
};

const convertToValidAvroName = name => {
	if (!_.isString(name)) {
		return name;
	}

	return name.replace(/[^A-Za-z0-9_]/g, '_');
};

const adaptJsonSchema = jsonSchema => {
	const adaptedJsonSchema = adaptNames(jsonSchema);

	return mapJsonSchema(adaptedJsonSchema, _.flow([
		adaptType,
		populateDefaultNullValuesForMultiple,
		handleEmptyDefaultInProperties
	]));
};

const adaptJsonSchemaName = convertToValidAvroName;

module.exports = { adaptJsonSchema, adaptJsonSchemaName };
