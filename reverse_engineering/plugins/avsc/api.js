'use strict'

// const { someMethod } = require('./helpers/someHelper');
const fs = require('fs');
const _ = require('lodash');
const DEFAULT_FIELD_NAME = 'New Field';

module.exports = {
	reFromFile(data, logger, callback) {
		readFileData(data.filePath)
		.then(fileData => {

		})
		.catch(callback);
	}
};

const reFromFile = (data, logger, callback) => {
	readFileData(data.filePath)
	.then(fileData => {
		const jsonSchema = convertToJsonSchema(fileData);
		console.log(JSON.stringify(jsonSchema, null, 4));
		return callback(null, fileData);
	})
	.catch(callback);
};

const convertToJsonSchema = (data) => {
	let jsonSchema = {};
	handleRecursiveSchema(data, jsonSchema);
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
		schema[prop] = data[prop];
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

const sampleSchema = {
	"type": "record",
	"name": "Document",
	"fields": [{
		"name": "DocId",
		"type": "long"
	}, {
		"name": "Links",
		"type": ["null", {
			"name": "Links",
			"type": "record",
			"fields": [{
				"name": "Backward",
				"type": {
					"type": "array",
					"items": "long"
				}
			}, {
				"name": "Forward",
				"type": {
					"type": "array",
					"items": "long"
				}
			}]
		}]
	}, {
		"name": "Name",
		"type": {
			"type": "array",
			"items": {
				"name": "Name",
				"type": "record",
				"fields": [{
					"name": "Language",
					"type": {
						"type": "array",
						"items": {
							"name": "Language",
							"type": "record",
							"fields": [{
								"name": "Code",
								"type": "string"
							}, {
								"name": "Country",
								"type": ["null", "string"]
							}]
						}
					}
				}, {
					"name": "Url",
					"type": ["null", "string"]
				}]
			}
		}
	}]
};

const readFileData = (filePath) => {
	return new Promise((resolve, reject) => {
		resolve(sampleSchema);
		// fs.readFile(filePath, 'utf-8', (err, content) => {
		// 	if(err){
		// 		reject(err);
		// 	} else {
		// 		resolve(content);
		// 	}
		// });
	});
};

reFromFile({}, {}, (err, res) => {
	console.log(err, res);
});