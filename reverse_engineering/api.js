'use strict'

const fs = require('fs');
const path = require('path');
const _ = require('lodash');
const avro = require('avsc');
const snappy = require('snappyjs');
const DEFAULT_FIELD_NAME = 'New_field';
let stateExtension = null;

const ADDITIONAL_PROPS = ['logicalType', 'scale', 'precision', 'name', 'arrayItemName', 'doc', 'order', 'aliases', 'symbols', 'namespace', 'size', 'default', 'pattern', 'choice'];
const DATA_TYPES = [
	'string',
	'bytes',
	'boolean',
	'null',
	'record',
	'array',
	'enum',
	'fixed',
	'int',
	'long',
	'float',
	'double',
	'map'
]

module.exports = {
	reFromFile(data, logger, callback) {
		handleFileData(data.filePath)
			.then(fileData => {
				return parseData(fileData);
			})
			.then(schema => {
				try {
					const jsonSchema = convertToJsonSchema(schema);
					const namespace = jsonSchema.namespace;
					jsonSchema.title = jsonSchema.name;
					delete jsonSchema.namespace;
					delete jsonSchema.name;
					const strJsonSchema = JSON.stringify(jsonSchema, null, 4);
					return callback(null, { jsonSchema: strJsonSchema, extension: stateExtension, containerName: namespace });
				} catch (err) {
					logger.log('error', { message: err.message, stack: err.stack }, 'Parsing Avro Schema Error');
					return callback(handleErrorObject(err))
				}
			})
			.catch(err => {
				logger.log('error', { message: err.message, stack: err.stack }, 'Avro Reverse-Engineering Error');
				callback(err)
			});
	}
};

const getFileExt = (filePath) => {
	return path.extname(filePath);
};

const handleFileData = (filePath) => {
	return new Promise((resolve, reject) => {
		const extension = getFileExt(filePath);
		stateExtension = extension;
		const respond = (err, content) => {
			if(err) {
				reject(handleErrorObject(err));
			} else {
				resolve(content);
			}
		};

		if (extension === '.avro') {
			readAvroData(filePath, respond);
		} else if (extension === '.avsc') {
			fs.readFile(filePath, 'utf-8', respond);
		} else {
			const error = new Error(`The file ${filePath} is not recognized as Avro Schema or Data.`)
			respond(error);
		}
	});
};

const readAvroData = (filePath, cb) => {
	const codecs = {
		snappy: function (buf, cb) {
			const uncompressed = snappy.uncompress(buf.slice(0, buf.length - 4));
			return cb(uncompressed);
		},
		null: function (buf, cb) { cb(null, buf); }
	};


	avro.createFileDecoder(filePath, { codecs })
		.on('metadata', (type, codecs, header) => {
			try {
				const schema = JSON.stringify(type);
				return cb(null, schema);
			} catch (error) {
				return cb(handleErrorObject(error));
			}
		})
		.on('error', cb);
};


const parseData = (fileData) => {
	return new Promise((resolve, reject) => {
		try {
			resolve(JSON.parse(fileData));
		} catch(err) {
			reject(handleErrorObject(err));
		}
	});
};

const convertToJsonSchema = (data) => {
	let jsonSchema = {};
	const definitions = {};
	handleRecursiveSchema(data, jsonSchema, {}, definitions);
	jsonSchema.type = 'object';
	jsonSchema.$schema = 'http://json-schema.org/draft-04/schema#';
	jsonSchema.definitions = definitions;
	return jsonSchema;
};

const handleRecursiveSchema = (data, schema, parentSchema = {}, definitions = {}) => {
	for (let prop in data) {
		switch(prop) {
			case 'type':
				handleType(data, schema, parentSchema, definitions);
				break;
			case 'fields':
				handleFields(data, prop, schema, definitions);
				break;
			case 'items':
				handleItems(data, prop, schema, definitions);
				break;
			default:
				handleOtherProps(data, prop, schema);
		}
	}
	if (isRequired(data, schema)) {
		addRequired(parentSchema, data.name);
	}
	return;
};


const handleType = (data, schema, parentSchema, definitions) => {
	if (Array.isArray(data.type)) {
		schema = handleMultipleTypes(data, schema, parentSchema, definitions);
	} else if (typeof data.type === 'object') {
		if (data.type.name) {		
			data.type = addDefinitions([data.type], definitions).pop();

			handleRecursiveSchema(data, schema, {}, definitions);
		} else if (data.type.items) {
			data.type.items = convertItemsToDefinitions(data.type.items, definitions);

			handleRecursiveSchema(data.type, schema, {}, definitions);
		} else {
			handleRecursiveSchema(data.type, schema, {}, definitions);
		}
	} else {
		schema = getType(schema, data, data.type);
	}
};

const convertItemsToDefinitions = (items, definitions) => {
	const itemToDefinition = (item, definitions) => {
		if (!item.name) {
			return item;
		}

		const type = addDefinitions([ item ], definitions).pop();
		const newItem = {
			name: item.name,
			type
		};

		return newItem;
	};

	if (Array.isArray(items)) {
		return items.map(item => itemToDefinition(item, definitions));
	} else {
		return itemToDefinition(items, definitions);
	}
};

const handleMultipleTypes = (data, schema, parentSchema, definitions) => {
	const hasComplexType = data.type.some(isComplexType);

	if (data.type.length === 1) {
		data.type = data.type[0];
		return handleType(data, schema, parentSchema, definitions);
	}

	if (hasComplexType) {
		data.type = addDefinitions(data.type, definitions);
		parentSchema = getChoice(data, parentSchema);
	} else {
		const typeObjects = data.type.map(type => getType({}, data, type));
		schema = Object.assign(schema, ...typeObjects);
		schema.type = typeObjects.map(item => item.type);
	}
};

const addDefinitions = (types, definitions) => {
	return types.map(type => {
		if (Object(type) !== type) {
			return type;
		} else if (!type.name) {
			return type;
		}

		let schema = {};
		handleRecursiveSchema(type, schema, {}, definitions);
		definitions[type.name] = schema;

		return type.name;
	});
};

const isComplexType = (type) => {
	if (DATA_TYPES.includes(type)) {
		return false;
	}
	if (typeof type === 'string') {
		return true;
	}

	const isNumber = [
		'int',
		'long',
		'float',
		'double',
	].includes(type.type);

	if (isNumber) {
		return false;
	} else {
		return true;
	}
};

const removeChangedField = (parentSchema, name) => {
	if (parentSchema.properties) {
		delete parentSchema.properties[name];
	} else if (parentSchema.items) {
		// delete multiple array item
	}
	return parentSchema;
};

const getType = (schema, field, type) => {
	if (Object(type) === type) {
		return Object.assign({}, schema, type, getType(schema, field, type.type));
	}

	switch(type) {
		case 'string':
		case 'bytes':
		case 'boolean':
		case 'record':
		case 'array':
		case 'enum':
		case 'fixed':
		case 'null':
		case 'choice':
			return Object.assign(schema, { type });
		case 'int':
		case 'long':
		case 'float':
		case 'double':
			return Object.assign(schema, {
				type: 'number',
				mode: type
			});
		case 'map':
			return Object.assign(schema, {
				type,
				subtype: `map<${field.values}>`
			});
		default:
			return Object.assign(schema, { $ref: '#/definitions/' + type });
	}
};

const getChoice = (data, parentSchema) => {
	const oneOfItem = getOneOf(data);
	parentSchema.properties = Object.assign({} ,parentSchema.properties, oneOfItem);

	return parentSchema;
};

const getOneOf = (data) => {
	const name = data.name || DEFAULT_FIELD_NAME;
	const oneOfProperties = data.type.map(item => {

		const subField = getSubField(item);
		const subFieldSchema = {};
		handleRecursiveSchema(subField, subFieldSchema);
		
		return getCommonSubSchema(subFieldSchema, name, item.name);
	});

	return {
		[name]: {
				name,
				oneOf_meta: data,
				type: 'choice',
				choice: 'oneOf',
				items: oneOfProperties
		}
	};
};


const getSubSchema = (data) => {
	return Object.assign({
		type: 'object'
	}, data);
}

const getCommonSubSchema = (properties, fieldName, itemName) => {
	const name = itemName ? itemName : fieldName;

	return getSubSchema({
		type: 'subschema',
		properties: {
			[name]: properties
		}
	});
}

const getSubField = (item) => {
	const field = (typeof item === 'object') ? item : { type: item };
	return field;
};

const handleFields = (data, prop, schema, definitions) => {
	schema.properties = {};
	schema.required = [];
	data[prop].forEach(element => {
		const name = element.name || DEFAULT_FIELD_NAME;
		schema.properties[name] = {};
		handleRecursiveSchema(element, schema.properties[name], schema, definitions);
	});
};

const handleItems = (data, prop, schema, definitions) => {
	const items = data[prop];

	if (typeof items === 'object') {
		schema.items = {};
		handleRecursiveSchema(items, schema.items, schema, definitions);
	} else {
		schema.items = {
			type: items
		};
	}
};

const handleOtherProps = (data, prop, schema) => {
	if (!ADDITIONAL_PROPS.includes(prop)) {
		return;
  	}
  	if (prop === 'default' && typeof data[prop] === 'boolean') {
		schema[prop] = data[prop].toString();	
	} else {
		schema[prop] = data[prop];
	}
};

const handleErrorObject = (error) => {
	let plainObject = {};
	Object.getOwnPropertyNames(error).forEach(function (key) {
		plainObject[key] = error[key];
	});
	return plainObject;
};

const isRequired = (data, schema) => {
	if (!data) {
		return false;
	} else if (data.hasOwnProperty('default')) {
		return false;
	} else {
		return true;
	}
};

const addRequired = (parentSchema, name) => {
	if (!Array.isArray(parentSchema.required)) {
		parentSchema.required = [name];
		return;
	}

	parentSchema.required.push(name);
};
