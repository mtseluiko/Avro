'use strict'

const fs = require('fs');
const path = require('path');
const _ = require('lodash');
const avro = require('avsc');
const snappy = require('snappyjs');
const DEFAULT_FIELD_NAME = 'New Field';
let stateExtension = null;

const ADDITIONAL_PROPS = ['name', '_name', 'doc', 'order', 'aliases', 'symbols', 'namespace', 'size', 'default'];

module.exports = {
	reFromFile(data, logger, callback) {
		handleFileData(data.filePath)
		.then(fileData => {
			return parseData(fileData);
		})
		.then(schema => {
			const jsonSchema = convertToJsonSchema(schema);
			try {
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
			if(err){
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
	handleRecursiveSchema(data, jsonSchema);
	jsonSchema.type = 'object';
	jsonSchema.$schema = 'http://json-schema.org/draft-04/schema#';
	return jsonSchema;
};

const handleRecursiveSchema = (data, schema, parentSchema = {}) => {
	for (let prop in data) {
		switch(prop) {
			case 'type':
				handleType(data, schema, parentSchema);
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


const handleType = (data, schema, parentSchema) => {
	if (Array.isArray(data.type)) {
		schema = handleMultipleTypes(data, schema, parentSchema);
	} else if (typeof data.type === 'object') {
		handleRecursiveSchema(data.type, schema);
	} else {
		schema = getType(schema, data, data.type);
	}
};


const handleMultipleTypes = (data, schema, parentSchema) => {
	const hasComplexType = data.type.find(item => typeof item !== 'string');

	if (hasComplexType) {
		parentSchema = getChoice(data, parentSchema);
		parentSchema = removeChangedField(parentSchema, data.name);
	} else {
		const typeObjects = data.type.map(type => getType({}, data, type));
		schema = Object.assign(schema, ...typeObjects);
		schema.type = typeObjects.map(item => item.type);
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
		case 'boolean':
		case 'null':
		case 'record':
		case 'array':
		case 'enum':
		case 'fixed':
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
			return Object.assign(schema, { type: 'string' });
	}
};

const getChoice = (data, parentSchema) => {
	parentSchema.oneOf = [];
	data.type.forEach(item => {
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
		items._name = items.name;
		delete items.name;
		handleRecursiveSchema(items, schema.items, schema);
	} else {
		schema.items = {
			type: items
		};
	}
};

const handleOtherProps = (data, prop, schema) => {
	if (ADDITIONAL_PROPS.includes(prop)) {
		schema[prop] = data[prop];
	}
	return;
};

const handleErrorObject = (error) => {
	let plainObject = {};
	Object.getOwnPropertyNames(error).forEach(function(key) {
		plainObject[key] = error[key];
	});
	return plainObject;
};