'use strict'

const fs = require('fs');
const _ = require('lodash');
const avro = require('avsc');
const snappy = require('snappyjs');
const DEFAULT_FIELD_NAME = 'New Field';

module.exports = {
	reFromFile(data, logger, callback) {
		handleFileData(data.filePath)
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

const getFileType = (filePath) => {
	const type = filePath.slice(-4);
	return type;
};

const handleFileData = (filePath) => {
	return new Promise((resolve, reject) => {
		const fileType = getFileType(filePath);
		const respond = (err, content) => {
			if(err){
				reject(err);
			} else {
				resolve(content);
			}
		};

		if (fileType === 'avro') {
			readAvroData(filePath, respond);
		} else if (fileType === 'avsc') {
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
			const uncompressed = snappy.uncompress(buf);
			return cb(uncompressed);
		},
		null: function (buf, cb) { cb(null, buf); },
		//other codecs
	};


	avro.createFileDecoder(filePath, { codecs })
		.on('metadata', (type, codecs, header) => { 
			try {
				const schema = JSON.stringify(type);
				return cb(null, schema);
			} catch (error) {
				return cb(error);
			}
		})
		.on('error', cb);
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