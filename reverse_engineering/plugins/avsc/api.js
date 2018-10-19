'use strict'

// const { someMethod } = require('./helpers/someHelper');
const fs = require('fs');
const _ = require('lodash');
const DEFAULT_FIELD_NAME = 'New Field';

module.exports = {
	reFromFile(data, logger, callback) {
		readFileData(data.filePath)
		.then(fileData => {
			return parseData(fileData);
		})
		.then(schema => {
			const jsonSchema = convertToJsonSchema(schema);
			return callback(null, jsonSchema);
		})
		.catch(callback);
	}
};

const readFileData = (filePath) => {
	return new Promise((resolve, reject) => {
		// resolve(sampleSchema);
		fs.readFile(filePath, 'utf-8', (err, content) => {
			if(err){
				reject(err);
			} else {
				resolve(content);
			}
		});
	});
};

const parseData = (fileData) => {
	return new Promise((resolve, reject) => {
		try {
			resolve(JSON.parse(fileData));
		} catch(err) {
			reject(err);
		}
	});
};

const reFromFile = (data, logger, callback) => {
	readFileData(data.filePath)
	.then(fileData => {
		return parseData(fileData);
	})
	.then(schema => {
		const jsonSchema = convertToJsonSchema(schema);
		return callback(null, jsonSchema);
	})
	.catch(callback);
};

const convertToJsonSchema = (data) => {
	let jsonSchema = {};
	handleRecursiveSchema(data, jsonSchema);
	jsonSchema.type = 'object';
	jsonSchema.$schema = 'http://json-schema.org/draft-04/schema#';
	return jsonSchema;
};

const handleRecursiveSchema = (data, schema, parentSchema = {}) => {
	for (let prop in data) {
		switch(prop) {
			case 'type':
				handleType(data, prop, schema, parentSchema);
				break;
			case 'fields':
				handleFields(data, prop, schema);
				break;
			case 'items':
				handleItems(data, prop, schema);
				break;
			default:
				handleOtherProps(data, prop, schema);
		}
	}
	return;
};


const handleType = (data, prop, schema, parentSchema) => {
	if (Array.isArray(data[prop])) {
		schema = handleMultipleTypes(data, prop, schema, parentSchema);
	} else if (typeof data[prop] === 'object') {
		handleRecursiveSchema(data[prop], schema);
	} else {
		schema = getType(schema, data, data[prop]);
	}
};


const handleMultipleTypes = (data, prop, schema, parentSchema) => {
	const hasComplexType = data[prop].find(item => typeof item !== 'string');

	if (hasComplexType) {
		parentSchema = getChoice(data, prop, parentSchema);
		parentSchema = removeChangedField(parentSchema, data.name);
	} else {
		schema[prop] = data[prop];
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
	switch(type) {
		case 'string':
		case 'bytes':
		case 'number':
		case 'boolean':
		case 'null':
		case 'record':
		case 'array':
		case 'enum':
		case 'fixed':
			return Object.assign(schema, { type });
		case 'map':
			return Object.assign(schema, {
				type,
				subtype: `map<${field.values}>`,
				keyType: 'string'
			});
		default:
			return Object.assign(schema, { type: 'string' });
	}
};

const getChoice = (data, prop, parentSchema) => {
	parentSchema.oneOf = [];
	data[prop].forEach(item => {
		const name = data.name || DEFAULT_FIELD_NAME;
		const subField = getSubField(item);
		const subFieldSchema = {};
		handleRecursiveSchema(subField, subFieldSchema);
		
		const subSchema = {
			type: 'object',
			properties: {
				[name]: subFieldSchema
			}
		};
		parentSchema.oneOf.push(subSchema);
	});
	return parentSchema;
};

const getSubField = (item) => {
	const field = (typeof item === 'object') ? item : { type: item };
	return field;
};

const handleFields = (data, prop, schema) => {
	schema.properties = {};
	data[prop].forEach(element => {
		const name = element.name || DEFAULT_FIELD_NAME;
		schema.properties[name] = {};
		handleRecursiveSchema(element, schema.properties[name], schema);
	});
};

const handleItems = (data, prop, schema) => {
	const items = data[prop];
	
	if (typeof items === 'object') {
		schema.items = {};
		handleRecursiveSchema(items, schema.items, schema);
	} else {
		schema.items = {
			type: items
		};
	}
};

const handleOtherProps = (data, prop, schema) => {
	schema[prop] = data[prop];
	return;
};